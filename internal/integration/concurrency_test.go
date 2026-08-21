package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/eventstream"
	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/repository/memory"
	"github.com/Bennybl/session-handler/internal/service"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

const concurrentWriters = 16

// Every adapter must stay correct while many keys are written and the query API
// is read at the same time.
func TestConcurrentStreamMutationsAndHTTPQueries(t *testing.T) {
	for _, adapter := range concurrentAdapters(t) {
		t.Run(adapter.name, func(t *testing.T) {
			harness := newApplicationHarness(t, adapter.open(t), concurrentIDGenerator())

			writeErrors := make(chan error, concurrentWriters)
			start := make(chan struct{})
			var writers sync.WaitGroup
			for index := range concurrentWriters {
				writers.Add(1)
				go func() {
					defer writers.Done()
					<-start
					writeErrors <- consumeOneStdin(harness.service, service.Event{
						EventID: sessiontest.EventID(2000 + index),
						Type:    service.EventLogin, TenantID: "tenant-concurrent",
						Username: fmt.Sprintf("user-%02d", index), IP: "192.0.2.200",
						Tags: []string{"concurrent"}, Timestamp: sessiontest.At("10:00").Add(time.Duration(index) * time.Second),
					})
				}()
			}

			readErrors := make(chan error, 1)
			stopReading := make(chan struct{})
			go func() {
				for {
					select {
					case <-stopReading:
						readErrors <- nil
						return
					default:
						if err := queryWithoutAssertions(harness.handler); err != nil {
							readErrors <- err
							return
						}
					}
				}
			}()

			close(start)
			writers.Wait()
			close(stopReading)

			for range concurrentWriters {
				if err := <-writeErrors; err != nil {
					t.Fatalf("concurrent stream consumer error = %v", err)
				}
			}
			if err := <-readErrors; err != nil {
				t.Fatalf("concurrent HTTP query error = %v", err)
			}

			got := queryHTTP(t, harness.handler, map[string]any{"filters": []any{
				filter("tenantId", "eq", "tenant-concurrent"),
			}})
			if len(got.Sessions) != concurrentWriters {
				t.Fatalf("stored sessions = %d, want one per writer (%d)", len(got.Sessions), concurrentWriters)
			}
		})
	}
}

type concurrentAdapter struct {
	name string
	open func(*testing.T) repository.SessionRepository
}

// concurrentAdapters returns the in-memory store, plus PostgreSQL when a test
// database is configured.
func concurrentAdapters(t *testing.T) []concurrentAdapter {
	t.Helper()
	adapters := []concurrentAdapter{{
		name: "memory",
		open: func(t *testing.T) repository.SessionRepository {
			repo := memory.New()
			t.Cleanup(func() { _ = repo.Close() })
			return repo
		},
	}}
	databaseURL := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL"))
	if databaseURL == "" {
		return adapters
	}
	return append(adapters, concurrentAdapter{
		name: "postgres",
		open: func(t *testing.T) repository.SessionRepository {
			repo, _, _ := newPostgresRepository(t, databaseURL)
			return repo
		},
	})
}

// consumeOneStdin drives one event through a complete source and consumer, so
// the writers contend the way the real ingestion path does.
func consumeOneStdin(application *service.SessionService, event service.Event) error {
	encoded, err := encodeEventValue(event)
	if err != nil {
		return err
	}
	source, err := eventstream.NewStdinSource(bytes.NewReader(append(encoded, '\n')))
	if err != nil {
		return err
	}
	consumer, err := eventstream.NewConsumer(source, application, &recordingDeadLetters{}, eventstream.Options{})
	if err != nil {
		return err
	}
	return consumer.Run(context.Background())
}

func queryWithoutAssertions(handler http.Handler) error {
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/query", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		return fmt.Errorf("query status %d: %s", response.Code, response.Body.String())
	}
	return nil
}
