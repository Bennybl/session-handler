package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appconfig "github.com/Bennybl/session-handler/internal/config"
	"github.com/Bennybl/session-handler/internal/eventstream"
	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

// The configured drivers decide which storage and event source are built, and
// the stream settings reach the NATS source unchanged.
func TestRunSelectsConfiguredAdapters(t *testing.T) {
	t.Parallel()

	postgresNATS := runtimeConfig(appconfig.StoragePostgres, appconfig.EventStreamNATS)
	postgresNATS.DatabaseURL = "postgres://database/session"
	postgresNATS.NATSURL = "nats://broker:4222"
	postgresNATS.NATSStream = "CUSTOM_EVENTS"
	postgresNATS.NATSSubject = "custom.events"
	postgresNATS.NATSConsumer = "custom-consumer"
	postgresNATS.NATSDeadLetterSubject = "custom.events.dlq"

	tests := []struct {
		name          string
		configuration appconfig.Config
		wantBuilt     []string
	}{
		{
			name:          "memory and stdin",
			configuration: runtimeConfig(appconfig.StorageMemory, appconfig.EventStreamStdin),
			wantBuilt:     []string{"memory", "stdin"},
		},
		{
			name:          "PostgreSQL and NATS",
			configuration: postgresNATS,
			wantBuilt:     []string{"postgres", "nats"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			spy := &runtimeSpy{}
			server := newFakeServer()
			runUntilCancelled(t, test.configuration, spy.dependencies(&fakeRepository{}, newBlockingSource(), server), server)

			if !reflect.DeepEqual(spy.built, test.wantBuilt) {
				t.Fatalf("adapters built = %v, want %v", spy.built, test.wantBuilt)
			}
			if spy.databaseURL != test.configuration.DatabaseURL {
				t.Errorf("database URL = %q, want %q", spy.databaseURL, test.configuration.DatabaseURL)
			}
			if spy.natsURL != test.configuration.NATSURL {
				t.Errorf("NATS URL = %q, want %q", spy.natsURL, test.configuration.NATSURL)
			}
			if test.configuration.EventStreamDriver != appconfig.EventStreamNATS {
				return
			}
			want := eventstream.NATSConfig{
				StreamName:        test.configuration.NATSStream,
				Subject:           test.configuration.NATSSubject,
				ConsumerName:      test.configuration.NATSConsumer,
				DeadLetterSubject: test.configuration.NATSDeadLetterSubject,
			}
			got := eventstream.NATSConfig{
				StreamName:        spy.natsConfig.StreamName,
				Subject:           spy.natsConfig.Subject,
				ConsumerName:      spy.natsConfig.ConsumerName,
				DeadLetterSubject: spy.natsConfig.DeadLetterSubject,
			}
			if got != want {
				t.Errorf("NATS stream settings = %+v, want %+v", got, want)
			}
		})
	}
}

// While running, the process reports ready and serves queries only. Cancelling
// shuts the consumer, repository, and server down, and readiness turns off.
func TestRunReadinessAndGracefulShutdown(t *testing.T) {
	t.Parallel()

	repo := &fakeRepository{}
	source := newBlockingSource()
	server := newFakeServer()
	spy := &runtimeSpy{}
	configuration := runtimeConfig(appconfig.StorageMemory, appconfig.EventStreamStdin)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- run(ctx, configuration, spy.dependencies(repo, source, server)) }()
	sessiontest.Await(t, server.started, "the HTTP server to start")
	sessiontest.Await(t, source.entered, "the event source to be read")

	for _, probe := range []struct {
		method, target string
		wantStatus     int
	}{
		{method: http.MethodGet, target: "/healthz", wantStatus: http.StatusOK},
		{method: http.MethodGet, target: "/readyz", wantStatus: http.StatusOK},
		{method: http.MethodPost, target: "/v1/sessions/events", wantStatus: http.StatusNotFound},
	} {
		if got := requestStatus(server.handler, probe.method, probe.target); got != probe.wantStatus {
			t.Errorf("%s %s status = %d, want %d", probe.method, probe.target, got, probe.wantStatus)
		}
	}

	cancel()
	if err := waitResult(t, result); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	sessiontest.Await(t, source.cancelled, "the consumer to observe cancellation")
	if !source.closed.Load() || !repo.closed.Load() || !server.shutdown.Load() {
		t.Errorf("cleanup: source closed = %v, repository closed = %v, server shut down = %v",
			source.closed.Load(), repo.closed.Load(), server.shutdown.Load())
	}
	if got := requestStatus(server.handler, http.MethodGet, "/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("readiness after shutdown = %d, want 503", got)
	}
}

// Piped input ends, but the query API must keep serving what was ingested.
func TestRunStdinEOFLeavesQueryServerRunning(t *testing.T) {
	t.Parallel()

	source := &fakeSource{eof: true, cancelled: make(chan struct{})}
	server := newFakeServer()
	spy := &runtimeSpy{}
	configuration := runtimeConfig(appconfig.StorageMemory, appconfig.EventStreamStdin)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- run(ctx, configuration, spy.dependencies(&fakeRepository{}, source, server)) }()
	sessiontest.Await(t, server.started, "the HTTP server to start")

	select {
	case err := <-result:
		t.Fatalf("run() returned at stdin EOF: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if got := requestStatus(server.handler, http.MethodGet, "/readyz"); got != http.StatusOK {
		t.Errorf("readiness after stdin EOF = %d, want 200", got)
	}

	cancel()
	if err := waitResult(t, result); err != nil {
		t.Fatalf("run() error = %v", err)
	}
}

// runUntilCancelled starts run, waits for the server, then shuts it down.
func runUntilCancelled(t *testing.T, configuration appconfig.Config, dependencies runtimeDependencies, server *fakeServer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- run(ctx, configuration, dependencies) }()
	sessiontest.Await(t, server.started, "the HTTP server to start")
	cancel()
	if err := waitResult(t, result); err != nil {
		t.Fatalf("run() error = %v", err)
	}
}

// runtimeSpy records which adapters run built and the settings they were given.
type runtimeSpy struct {
	built       []string
	databaseURL string
	natsURL     string
	natsConfig  eventstream.NATSConfig
}

func (spy *runtimeSpy) dependencies(repo repository.SessionRepository, source *fakeSource, server *fakeServer) runtimeDependencies {
	return runtimeDependencies{
		stdin:  io.NopCloser(&emptyReader{}),
		stderr: io.Discard,
		newMemory: func() repository.SessionRepository {
			spy.built = append(spy.built, "memory")
			return repo
		},
		openPostgres: func(_ context.Context, url string) (repository.SessionRepository, error) {
			spy.built = append(spy.built, "postgres")
			spy.databaseURL = url
			return repo, nil
		},
		newStdinSource: func(io.Reader) (eventstream.Source, error) {
			spy.built = append(spy.built, "stdin")
			return source, nil
		},
		openNATSSource: func(_ context.Context, url string, config eventstream.NATSConfig) (natsSource, error) {
			spy.built = append(spy.built, "nats")
			spy.natsURL = url
			spy.natsConfig = config
			return &fakeNATSSource{fakeSource: source}, nil
		},
		newServer: func(_ string, handler http.Handler) managedServer {
			server.handler = handler
			return server
		},
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

func (r *fakeRepository) Close() error {
	r.closed.Store(true)
	return nil
}

// fakeSource either ends immediately at EOF or blocks until its context ends,
// recording that it was entered, cancelled, and closed.
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

func (s *fakeSource) Next(ctx context.Context) (eventstream.Message, error) {
	if s.entered != nil {
		s.enter.Do(func() { close(s.entered) })
	}
	if s.eof {
		return nil, io.EOF
	}
	<-ctx.Done()
	s.cancel.Do(func() { close(s.cancelled) })
	return nil, ctx.Err()
}

func (s *fakeSource) Close() error {
	s.closed.Store(true)
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

func (s *fakeServer) Serve() error {
	s.start.Do(func() { close(s.started) })
	<-s.stop
	return http.ErrServerClosed
}

func (s *fakeServer) Shutdown(context.Context) error {
	s.shutdown.Store(true)
	s.stopOnce.Do(func() { close(s.stop) })
	return nil
}

type emptyReader struct{}

func (*emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

func runtimeConfig(storage appconfig.StorageDriver, stream appconfig.EventStreamDriver) appconfig.Config {
	return appconfig.Config{
		StorageDriver: storage, EventStreamDriver: stream, HTTPAddr: "127.0.0.1:0",
		NATSStream: "SESSION_EVENTS", NATSSubject: "sessions.events", NATSConsumer: "session-handler",
		NATSDeadLetterSubject: "sessions.events.dlq",
		StartupTimeout:        time.Second, ShutdownTimeout: time.Second,
	}
}

func requestStatus(handler http.Handler, method, target string) int {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(method, target, nil))
	return response.Code
}

func waitResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for run() to return")
		return nil
	}
}
