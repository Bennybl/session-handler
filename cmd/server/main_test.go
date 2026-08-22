package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	appconfig "github.com/Bennybl/session-handler/internal/config"
)

func TestOperationalHealthAndReadiness(t *testing.T) {
	ready := &atomic.Bool{}
	handler := operationalHandler(http.NotFoundHandler(), http.NotFoundHandler(), ready)
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

func TestStandaloneRuntimeStartsWithoutExternalServices(t *testing.T) {
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
	go func() { result <- run(ctx, configuration, log.New(io.Discard, "", 0)) }()
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
}
