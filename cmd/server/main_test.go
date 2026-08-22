package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appconfig "github.com/Bennybl/session-handler/internal/config"
	"github.com/Bennybl/session-handler/internal/eventstream"
)

func TestOperationalHealthAndReadiness(t *testing.T) {
	ready := &atomic.Bool{}
	handler := operationalHandler(http.NotFoundHandler(), ready)
	request := func(path string) int {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		return recorder.Code
	}
	if got := request("/healthz"); got != http.StatusOK {
		t.Fatalf("health = %d", got)
	}
	if got := request("/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("initial readiness = %d", got)
	}
	ready.Store(true)
	if got := request("/readyz"); got != http.StatusOK {
		t.Fatalf("readiness = %d", got)
	}
}

func TestOperationalHandlerHasNoHTTPEventIngress(t *testing.T) {
	handler := operationalHandler(http.NotFoundHandler(), &atomic.Bool{})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/events", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("POST /v1/events status = %d, want 404", recorder.Code)
	}
}

func TestRuntimeLifecycleUsesAbstractEventSource(t *testing.T) {
	configuration, err := appconfig.FromLookup(func(key string) (string, bool) {
		if key == "HTTP_ADDR" {
			return "127.0.0.1:0", true
		}
		if key == "SHUTDOWN_TIMEOUT" {
			return "2s", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	source := newBlockingSource()
	go func() {
		result <- runWithSourceFactory(ctx, configuration, log.New(io.Discard, "", 0), func(context.Context, appconfig.Config) (eventstream.Source, error) {
			return source, nil
		})
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runtime did not shut down")
	}
	if source.closeCalls.Load() != 1 {
		t.Fatalf("source close calls = %d, want 1", source.closeCalls.Load())
	}
}

type blockingSource struct {
	closed     chan struct{}
	closeOnce  sync.Once
	closeCalls atomic.Int32
}

func newBlockingSource() *blockingSource { return &blockingSource{closed: make(chan struct{})} }

func (s *blockingSource) Consume(ctx context.Context, _ eventstream.Handler) error {
	select {
	case <-ctx.Done():
	case <-s.closed:
	}
	return nil
}

func (s *blockingSource) Close() error {
	s.closeOnce.Do(func() {
		s.closeCalls.Add(1)
		close(s.closed)
	})
	return nil
}
