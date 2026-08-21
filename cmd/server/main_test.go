package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appconfig "github.com/Bennybl/session-handler/internal/config"
	"github.com/Bennybl/session-handler/internal/eventstream"
	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
)

func TestRunSelectsConfiguredAdapters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		configuration    appconfig.Config
		wantMemory       int
		wantPostgres     int
		wantStdin        int
		wantNATS         int
		wantDatabaseURL  string
		wantNATSURL      string
		wantNATSStream   string
		wantNATSSubject  string
		wantNATSConsumer string
	}{
		{
			name:          "memory and stdin",
			configuration: runtimeConfig(appconfig.StorageMemory, appconfig.EventStreamStdin),
			wantMemory:    1, wantStdin: 1,
		},
		{
			name: "PostgreSQL and NATS",
			configuration: func() appconfig.Config {
				value := runtimeConfig(appconfig.StoragePostgres, appconfig.EventStreamNATS)
				value.DatabaseURL = "postgres://database/session"
				value.NATSURL = "nats://broker:4222"
				value.NATSStream = "CUSTOM_EVENTS"
				value.NATSSubject = "custom.events"
				value.NATSConsumer = "custom-consumer"
				value.NATSDeadLetterSubject = "custom.events.dlq"
				return value
			}(),
			wantPostgres: 1, wantNATS: 1, wantDatabaseURL: "postgres://database/session",
			wantNATSURL: "nats://broker:4222", wantNATSStream: "CUSTOM_EVENTS",
			wantNATSSubject: "custom.events", wantNATSConsumer: "custom-consumer",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			repo := &fakeRepository{}
			source := newBlockingSource()
			server := newFakeServer()
			var memoryCalls, postgresCalls, stdinCalls, natsCalls int
			var databaseURL, natsURL string
			var natsConfig eventstream.NATSConfig
			dependencies := runtimeDependencies{
				stdin: io.NopCloser(&emptyReader{}), stderr: io.Discard,
				newMemory: func() repository.SessionRepository { memoryCalls++; return repo },
				openPostgres: func(_ context.Context, url string) (repository.SessionRepository, error) {
					postgresCalls++
					databaseURL = url
					return repo, nil
				},
				newStdinSource: func(io.Reader) (eventstream.Source, error) {
					stdinCalls++
					return source, nil
				},
				openNATSSource: func(_ context.Context, url string, config eventstream.NATSConfig) (natsSource, error) {
					natsCalls++
					natsURL = url
					natsConfig = config
					return &fakeNATSSource{fakeSource: source}, nil
				},
				newServer: func(_ string, handler http.Handler) managedServer {
					server.handler = handler
					return server
				},
			}
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() { result <- run(ctx, test.configuration, dependencies) }()
			waitClosed(t, server.started, "HTTP server start")
			cancel()
			if err := waitResult(t, result); err != nil {
				t.Fatalf("run() error = %v", err)
			}
			if memoryCalls != test.wantMemory || postgresCalls != test.wantPostgres ||
				stdinCalls != test.wantStdin || natsCalls != test.wantNATS {
				t.Fatalf("factory calls memory=%d postgres=%d stdin=%d NATS=%d", memoryCalls, postgresCalls, stdinCalls, natsCalls)
			}
			if databaseURL != test.wantDatabaseURL || natsURL != test.wantNATSURL {
				t.Fatalf("factory URLs database=%q NATS=%q", databaseURL, natsURL)
			}
			if test.wantNATS == 1 && (natsConfig.StreamName != test.wantNATSStream ||
				natsConfig.Subject != test.wantNATSSubject || natsConfig.ConsumerName != test.wantNATSConsumer ||
				natsConfig.DeadLetterSubject != "custom.events.dlq") {
				t.Fatalf("NATS config = %+v", natsConfig)
			}
		})
	}
}

func TestRunReadinessAndGracefulShutdown(t *testing.T) {
	t.Parallel()
	repo := &fakeRepository{}
	source := newBlockingSource()
	server := newFakeServer()
	dependencies := memoryStdinDependencies(repo, source, server)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- run(ctx, runtimeConfig(appconfig.StorageMemory, appconfig.EventStreamStdin), dependencies)
	}()
	waitClosed(t, server.started, "HTTP server start")
	waitClosed(t, source.entered, "event source read")

	if status := requestStatus(server.handler, http.MethodGet, "/healthz"); status != http.StatusOK {
		t.Fatalf("health status = %d", status)
	}
	if status := requestStatus(server.handler, http.MethodGet, "/readyz"); status != http.StatusOK {
		t.Fatalf("ready status = %d", status)
	}
	if status := requestStatus(server.handler, http.MethodPost, "/v1/sessions/events"); status != http.StatusNotFound {
		t.Fatalf("event route status = %d", status)
	}

	cancel()
	if err := waitResult(t, result); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	waitClosed(t, source.cancelled, "consumer cancellation")
	if !source.closed.Load() || !repo.closed.Load() || !server.shutdown.Load() {
		t.Fatalf("cleanup source=%v repository=%v server=%v", source.closed.Load(), repo.closed.Load(), server.shutdown.Load())
	}
	if status := requestStatus(server.handler, http.MethodGet, "/readyz"); status != http.StatusServiceUnavailable {
		t.Fatalf("ready status after shutdown = %d", status)
	}
}

func TestRunStdinEOFLeavesQueryServerRunning(t *testing.T) {
	t.Parallel()
	repo := &fakeRepository{}
	source := &fakeSource{eof: true, cancelled: make(chan struct{})}
	server := newFakeServer()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- run(ctx, runtimeConfig(appconfig.StorageMemory, appconfig.EventStreamStdin), memoryStdinDependencies(repo, source, server))
	}()
	waitClosed(t, server.started, "HTTP server start")
	select {
	case err := <-result:
		t.Fatalf("run() stopped at stdin EOF: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if status := requestStatus(server.handler, http.MethodGet, "/readyz"); status != http.StatusOK {
		t.Fatalf("ready status after stdin EOF = %d", status)
	}
	cancel()
	if err := waitResult(t, result); err != nil {
		t.Fatalf("run() error = %v", err)
	}
}

type fakeRepository struct {
	closed atomic.Bool
}

func (*fakeRepository) Mutate(context.Context, session.SessionKey, repository.MutationFunc) error {
	return nil
}

func (*fakeRepository) Query(context.Context, session.QuerySpec) (session.QueryResult, error) {
	return session.QueryResult{}, nil
}

func (repository *fakeRepository) Close() error {
	repository.closed.Store(true)
	return nil
}

type fakeSource struct {
	eof       bool
	entered   chan struct{}
	enter     sync.Once
	cancelled chan struct{}
	cancel    sync.Once
	closed    atomic.Bool
}

func newBlockingSource() *fakeSource {
	return &fakeSource{entered: make(chan struct{}), cancelled: make(chan struct{})}
}

func (source *fakeSource) Next(ctx context.Context) (eventstream.Message, error) {
	if source.entered != nil {
		source.enter.Do(func() { close(source.entered) })
	}
	if source.eof {
		return nil, io.EOF
	}
	<-ctx.Done()
	source.cancel.Do(func() { close(source.cancelled) })
	return nil, ctx.Err()
}

func (source *fakeSource) Close() error {
	source.closed.Store(true)
	return nil
}

type fakeNATSSource struct {
	*fakeSource
}

func (*fakeNATSSource) PublishDeadLetter(context.Context, eventstream.DeadLetter) error { return nil }

type fakeServer struct {
	handler  http.Handler
	started  chan struct{}
	stop     chan struct{}
	start    sync.Once
	stopOnce sync.Once
	shutdown atomic.Bool
}

func newFakeServer() *fakeServer {
	return &fakeServer{started: make(chan struct{}), stop: make(chan struct{})}
}

func (server *fakeServer) Serve() error {
	server.start.Do(func() { close(server.started) })
	<-server.stop
	return http.ErrServerClosed
}

func (server *fakeServer) Shutdown(context.Context) error {
	server.shutdown.Store(true)
	server.stopOnce.Do(func() { close(server.stop) })
	return nil
}

type emptyReader struct{}

func (*emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

func memoryStdinDependencies(repo repository.SessionRepository, source eventstream.Source, server *fakeServer) runtimeDependencies {
	return runtimeDependencies{
		stdin: io.NopCloser(&emptyReader{}), stderr: io.Discard,
		newMemory:      func() repository.SessionRepository { return repo },
		openPostgres:   func(context.Context, string) (repository.SessionRepository, error) { return repo, nil },
		newStdinSource: func(io.Reader) (eventstream.Source, error) { return source, nil },
		openNATSSource: func(context.Context, string, eventstream.NATSConfig) (natsSource, error) {
			return &fakeNATSSource{fakeSource: source.(*fakeSource)}, nil
		},
		newServer: func(_ string, handler http.Handler) managedServer {
			server.handler = handler
			return server
		},
	}
}

func runtimeConfig(storage appconfig.StorageDriver, stream appconfig.EventStreamDriver) appconfig.Config {
	return appconfig.Config{
		StorageDriver: storage, EventStreamDriver: stream, HTTPAddr: "127.0.0.1:0",
		NATSStream: "SESSION_EVENTS", NATSSubject: "sessions.events", NATSConsumer: "session-handler",
		NATSDeadLetterSubject: "sessions.events.dlq", StartupTimeout: time.Second, ShutdownTimeout: time.Second,
	}
}

func requestStatus(handler http.Handler, method, target string) int {
	request := httptest.NewRequest(method, target, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response.Code
}

func waitClosed(t *testing.T, channel <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", operation)
	}
}

func waitResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for run result")
		return nil
	}
}
