package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/eventstream"
	"github.com/Bennybl/session-handler/internal/httpapi"
	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/service"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

// The sessions the fixture events produce, in the order logins occur.
var (
	aliceSessionID      = sessiontest.SessionID(1301)
	deduplicatedID      = sessiontest.SessionID(1302)
	bobSessionID        = sessiontest.SessionID(1303)
	otherAliceSessionID = sessiontest.SessionID(1304)
)

// The event identities the fixture stream carries.
var (
	aliceLoginEventID    = sessiontest.EventID(1301)
	aliceUpdateEventID   = sessiontest.EventID(1302)
	aliceLogoutEventID   = sessiontest.EventID(1303)
	bobLoginEventID      = sessiontest.EventID(1304)
	otherLoginEventID    = sessiontest.EventID(1305)
	invalidUpdateEventID = sessiontest.EventID(1306)
)

// integrationNow is the clock every harness reports, after all fixture events.
var integrationNow = sessiontest.At("2026-08-22 12:00")

type applicationHarness struct {
	service *service.SessionService
	handler http.Handler
}

type apiQueryResponse struct {
	Sessions   []apiSession `json:"sessions"`
	NextCursor string       `json:"nextCursor"`
}

type apiSession struct {
	ID       string     `json:"id"`
	TenantID string     `json:"tenantId"`
	Username string     `json:"username"`
	IP       string     `json:"ip"`
	LoginAt  time.Time  `json:"loginAt"`
	LogoutAt *time.Time `json:"logoutAt"`
	States   []apiState `json:"states"`
}

type apiState struct {
	Tags      []string   `json:"tags"`
	ValidFrom time.Time  `json:"validFrom"`
	ValidTo   *time.Time `json:"validTo"`
}

// querySnapshot is everything the HTTP API reports about a seeded store. Two
// storage and stream combinations must produce identical snapshots.
type querySnapshot struct {
	AllIDs       []string
	ActiveIDs    []string
	AdminIDs     []string
	AliceStates  int
	PaginatedIDs []string
}

type recordingDeadLetters struct {
	mu      sync.Mutex
	letters []eventstream.DeadLetter
}

func (publisher *recordingDeadLetters) PublishDeadLetter(_ context.Context, letter eventstream.DeadLetter) error {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	publisher.letters = append(publisher.letters, letter)
	return nil
}

func (publisher *recordingDeadLetters) count() int {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	return len(publisher.letters)
}

func newApplicationHarness(t *testing.T, repo repository.SessionRepository, newSessionID func() (string, error)) applicationHarness {
	t.Helper()
	application, err := service.New(service.Dependencies{
		Repository:   repo,
		Now:          func() time.Time { return integrationNow },
		NewSessionID: newSessionID,
	})
	if err != nil {
		t.Fatalf("service.New() error = %v", err)
	}
	handler, err := httpapi.NewHandler(application)
	if err != nil {
		t.Fatalf("httpapi.NewHandler() error = %v", err)
	}
	return applicationHarness{service: application, handler: handler}
}

// fixtureIDGenerator hands out the fixture session IDs in login order, so both
// adapters label the same lifecycles the same way.
func fixtureIDGenerator(t *testing.T) func() (string, error) {
	t.Helper()
	ids := []string{aliceSessionID, deduplicatedID, bobSessionID, otherAliceSessionID}
	var issued atomic.Int64
	return func() (string, error) {
		position := issued.Add(1) - 1
		if position >= int64(len(ids)) {
			return "", fmt.Errorf("the fixture session ID generator is exhausted after %d logins", len(ids))
		}
		return ids[position], nil
	}
}

func concurrentIDGenerator() func() (string, error) {
	var issued atomic.Int64
	return func() (string, error) {
		return sessiontest.SessionID(int(issued.Add(1))), nil
	}
}

// fixtureEvents is the stream both adapters consume. It deliberately repeats
// Alice's login, and ends with an update that no longer has an active session,
// so deduplication and dead lettering are both exercised.
func fixtureEvents() []service.Event {
	loginAt := sessiontest.At("10:00")
	aliceLogin := service.Event{
		EventID: aliceLoginEventID, Type: service.EventLogin,
		TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10",
		Tags: []string{"user"}, Timestamp: loginAt,
	}
	return []service.Event{
		aliceLogin,
		aliceLogin,
		{
			EventID: aliceUpdateEventID, Type: service.EventUpdate,
			TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10",
			Tags: []string{"admin", "user"}, Timestamp: sessiontest.At("10:30"),
		},
		{
			EventID: aliceLogoutEventID, Type: service.EventLogout,
			TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10",
			Timestamp: sessiontest.At("12:00"),
		},
		{
			EventID: bobLoginEventID, Type: service.EventLogin,
			TenantID: "tenant-a", Username: "bob", IP: "192.0.2.10",
			Tags: []string{"user"}, Timestamp: sessiontest.At("10:15"),
		},
		{
			EventID: otherLoginEventID, Type: service.EventLogin,
			TenantID: "tenant-b", Username: "alice", IP: "192.0.2.10",
			Tags: []string{"tenant-b"}, Timestamp: sessiontest.At("10:20"),
		},
		{
			EventID: invalidUpdateEventID, Type: service.EventUpdate,
			TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10",
			Tags: []string{"invalid"}, Timestamp: sessiontest.At("13:00"),
		},
	}
}

func encodeEvent(t *testing.T, event service.Event) []byte {
	t.Helper()
	encoded, err := encodeEventValue(event)
	if err != nil {
		t.Fatalf("encode event %s: %v", event.EventID, err)
	}
	return encoded
}

// encodeEventValue writes the stream envelope, which is the wire format both
// the stdin and NATS sources decode.
func encodeEventValue(event service.Event) ([]byte, error) {
	return json.Marshal(struct {
		EventID   string            `json:"eventId"`
		Type      service.EventType `json:"type"`
		TenantID  string            `json:"tenantId"`
		Username  string            `json:"username"`
		IP        string            `json:"ip"`
		Tags      []string          `json:"tags"`
		Timestamp time.Time         `json:"timestamp"`
	}{
		EventID: event.EventID, Type: event.Type, TenantID: event.TenantID,
		Username: event.Username, IP: event.IP, Tags: event.Tags, Timestamp: event.Timestamp,
	})
}

// consumeStdin runs events through the real NDJSON source and consumer.
func consumeStdin(t *testing.T, application *service.SessionService, events []service.Event, publisher eventstream.DeadLetterPublisher) {
	t.Helper()
	lines := make([]string, len(events))
	for index, event := range events {
		lines[index] = string(encodeEvent(t, event))
	}
	source, err := eventstream.NewStdinSource(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err != nil {
		t.Fatalf("NewStdinSource() error = %v", err)
	}
	consumer, err := eventstream.NewConsumer(source, application, publisher, eventstream.Options{})
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("stdin consumer Run() error = %v", err)
	}
}

func queryHTTP(t *testing.T, handler http.Handler, request any) apiQueryResponse {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("encode the query request: %v", err)
	}
	httpRequest := httptest.NewRequest(http.MethodPost, "/v1/sessions/query", bytes.NewReader(body))
	httpRequest.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httpRequest)

	if response.Code != http.StatusOK {
		t.Fatalf("query %s: status = %d, body = %s", body, response.Code, response.Body.String())
	}
	var decoded apiQueryResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode the query response: %v", err)
	}
	return decoded
}

func filter(field, operator string, value any) map[string]any {
	return map[string]any{"field": field, "operator": operator, "value": value}
}

// exerciseHTTPQueries reads a seeded store through the query API only, so the
// result depends on stored behavior rather than on how events were ingested.
func exerciseHTTPQueries(t *testing.T, handler http.Handler) querySnapshot {
	t.Helper()
	acrossTheDay := filter("activity", "overlaps", map[string]any{
		"from": "2026-08-21T09:00:00Z", "to": "2026-08-21T14:00:00Z",
	})

	all := queryHTTP(t, handler, map[string]any{"filters": []any{acrossTheDay}})
	active := queryHTTP(t, handler, map[string]any{})
	admin := queryHTTP(t, handler, map[string]any{"filters": []any{
		filter("tenantId", "eq", "tenant-a"),
		filter("username", "eq", "alice"),
		filter("activity", "at", "2026-08-21T11:00:00Z"),
		filter("tags", "containsAll", []string{"admin"}),
	}})

	// Three different users share one IP, which must not merge them.
	sharedIP := queryHTTP(t, handler, map[string]any{"filters": []any{
		filter("ip", "eq", "192.0.2.10"), acrossTheDay,
	}})
	if len(sharedIP.Sessions) != 3 {
		t.Fatalf("sessions on the shared IP = %+v, want 3", sharedIP.Sessions)
	}

	var paginated []string
	for cursor := ""; ; {
		page := map[string]any{"limit": 1}
		if cursor != "" {
			page["cursor"] = cursor
		}
		result := queryHTTP(t, handler, map[string]any{"filters": []any{acrossTheDay}, "page": page})
		if len(result.Sessions) != 1 {
			t.Fatalf("page after cursor %q = %+v, want exactly one session", cursor, result.Sessions)
		}
		paginated = append(paginated, result.Sessions[0].ID)
		if cursor = result.NextCursor; cursor == "" {
			break
		}
	}

	aliceStates := 0
	for _, value := range all.Sessions {
		if value.ID == aliceSessionID {
			aliceStates = len(value.States)
		}
	}
	return querySnapshot{
		AllIDs:      ids(all.Sessions),
		ActiveIDs:   ids(active.Sessions),
		AdminIDs:    ids(admin.Sessions),
		AliceStates: aliceStates, PaginatedIDs: paginated,
	}
}

func ids(sessions []apiSession) []string {
	result := make([]string, len(sessions))
	for index, value := range sessions {
		result[index] = value.ID
	}
	return result
}

// waitUntil polls condition, which is how the tests observe work the NATS
// consumer performs on its own goroutine.
func waitUntil(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}
