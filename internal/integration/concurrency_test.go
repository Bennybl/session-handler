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
)

func TestConcurrentStreamMutationsAndHTTPQueries(t *testing.T) {
	factories := []struct {
		name string
		open func(*testing.T) repository.SessionRepository
	}{
		{name: "memory", open: func(t *testing.T) repository.SessionRepository {
			repo := memory.New()
			t.Cleanup(func() { _ = repo.Close() })
			return repo
		}},
	}
	if databaseURL := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL")); databaseURL != "" {
		factories = append(factories, struct {
			name string
			open func(*testing.T) repository.SessionRepository
		}{name: "postgres", open: func(t *testing.T) repository.SessionRepository {
			repo, _, _ := newPostgresRepository(t, databaseURL)
			return repo
		}})
	}

	for _, factory := range factories {
		t.Run(factory.name, func(t *testing.T) {
			repo := factory.open(t)
			harness := newApplicationHarness(t, repo, concurrentIDGenerator())
			const writers = 16
			results := make(chan error, writers)
			start := make(chan struct{})
			var group sync.WaitGroup
			for index := range writers {
				index := index
				group.Add(1)
				go func() {
					defer group.Done()
					<-start
					event := service.Event{
						EventID: fmt.Sprintf("b1000000-0000-4000-8000-%012d", index+1),
						Type:    service.EventLogin, TenantID: "tenant-concurrent", Username: fmt.Sprintf("user-%02d", index),
						IP: "192.0.2.200", Tags: []string{"concurrent"},
						Timestamp: time.Date(2026, 8, 21, 10, 0, index, 0, time.UTC),
					}
					results <- consumeOneStdin(harness.service, event)
				}()
			}
			queryErrors := make(chan error, 1)
			stopQueries := make(chan struct{})
			go func() {
				for {
					select {
					case <-stopQueries:
						queryErrors <- nil
						return
					default:
						if err := queryWithoutAssertions(harness.handler); err != nil {
							queryErrors <- err
							return
						}
					}
				}
			}()

			close(start)
			group.Wait()
			close(stopQueries)
			for range writers {
				if err := <-results; err != nil {
					t.Fatalf("concurrent stream consumer error = %v", err)
				}
			}
			if err := <-queryErrors; err != nil {
				t.Fatalf("concurrent HTTP query error = %v", err)
			}
			got := queryHTTP(t, harness.handler, map[string]any{"filters": []any{
				map[string]any{"field": "tenantId", "operator": "eq", "value": "tenant-concurrent"},
			}})
			if len(got.Sessions) != writers {
				t.Fatalf("concurrent sessions = %d, want %d", len(got.Sessions), writers)
			}
		})
	}
}

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
		return fmt.Errorf("status %d: %s", response.Code, response.Body.String())
	}
	return nil
}
