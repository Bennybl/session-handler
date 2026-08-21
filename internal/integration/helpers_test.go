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
)

const (
	aliceSessionID       = "00000000-0000-4000-8000-000000001301"
	deduplicatedID       = "00000000-0000-4000-8000-000000001302"
	bobSessionID         = "00000000-0000-4000-8000-000000001303"
	otherAliceSessionID  = "00000000-0000-4000-8000-000000001304"
	aliceLoginEventID    = "a1000000-0000-4000-8000-000000000001"
	aliceUpdateEventID   = "a1000000-0000-4000-8000-000000000002"
	aliceLogoutEventID   = "a1000000-0000-4000-8000-000000000003"
	bobLoginEventID      = "a1000000-0000-4000-8000-000000000004"
	otherLoginEventID    = "a1000000-0000-4000-8000-000000000005"
	invalidUpdateEventID = "a1000000-0000-4000-8000-000000000006"
)

var integrationNow = time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)

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

func newApplicationHarness(t *testing.T, repo repository.SessionRepository, generator func() (string, error)) applicationHarness {
	t.Helper()
	application, err := service.New(service.Dependencies{
		Repository: repo, Now: func() time.Time { return integrationNow }, NewSessionID: generator,
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

func fixtureIDGenerator(t *testing.T) func() (string, error) {
	t.Helper()
	ids := []string{aliceSessionID, deduplicatedID, bobSessionID, otherAliceSessionID}
	var index atomic.Int64
	return func() (string, error) {
		position := index.Add(1) - 1
		if position >= int64(len(ids)) {
			return "", fmt.Errorf("fixture session ID generator exhausted")
		}
		return ids[position], nil
	}
}

func concurrentIDGenerator() func() (string, error) {
	var sequence atomic.Int64
	return func() (string, error) {
		return fmt.Sprintf("00000000-0000-4000-8000-%012d", sequence.Add(1)), nil
	}
}

func fixtureEvents() []service.Event {
	loginAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	login := service.Event{
		EventID: aliceLoginEventID, Type: service.EventLogin, TenantID: "tenant-a", Username: "alice",
		IP: "192.0.2.10", Tags: []string{"user"}, Timestamp: loginAt,
	}
	return []service.Event{
		login,
		login,
		{EventID: aliceUpdateEventID, Type: service.EventUpdate, TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10", Tags: []string{"admin", "user"}, Timestamp: loginAt.Add(30 * time.Minute)},
		{EventID: aliceLogoutEventID, Type: service.EventLogout, TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10", Timestamp: loginAt.Add(2 * time.Hour)},
		{EventID: bobLoginEventID, Type: service.EventLogin, TenantID: "tenant-a", Username: "bob", IP: "192.0.2.10", Tags: []string{"user"}, Timestamp: loginAt.Add(15 * time.Minute)},
		{EventID: otherLoginEventID, Type: service.EventLogin, TenantID: "tenant-b", Username: "alice", IP: "192.0.2.10", Tags: []string{"tenant-b"}, Timestamp: loginAt.Add(20 * time.Minute)},
		{EventID: invalidUpdateEventID, Type: service.EventUpdate, TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10", Tags: []string{"invalid"}, Timestamp: loginAt.Add(3 * time.Hour)},
	}
}

func encodeEvent(t *testing.T, event service.Event) []byte {
	t.Helper()
	encoded, err := encodeEventValue(event)
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}
	return encoded
}

func encodeEventValue(event service.Event) ([]byte, error) {
	envelope := struct {
		EventID   string            `json:"eventId"`
		Type      service.EventType `json:"type"`
		TenantID  string            `json:"tenantId"`
		Username  string            `json:"username"`
		IP        string            `json:"ip"`
		Tags      []string          `json:"tags"`
		Timestamp time.Time         `json:"timestamp"`
	}{
		EventID: event.EventID, Type: event.Type, TenantID: event.TenantID, Username: event.Username,
		IP: event.IP, Tags: event.Tags, Timestamp: event.Timestamp,
	}
	return json.Marshal(envelope)
}

func consumeStdin(t *testing.T, application *service.SessionService, events []service.Event, publisher eventstream.DeadLetterPublisher) {
	t.Helper()
	lines := make([]string, len(events))
	for index, event := range events {
		lines[index] = string(encodeEvent(t, event))
	}
	source, err := eventstream.NewStdinSource(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err != nil {
		t.Fatalf("eventstream.NewStdinSource() error = %v", err)
	}
	consumer, err := eventstream.NewConsumer(source, application, publisher, eventstream.Options{})
	if err != nil {
		t.Fatalf("eventstream.NewConsumer() error = %v", err)
	}
	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("stdin consumer Run() error = %v", err)
	}
}

func queryHTTP(t *testing.T, handler http.Handler, request any) apiQueryResponse {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("encode query request: %v", err)
	}
	httpRequest := httptest.NewRequest(http.MethodPost, "/v1/sessions/query", bytes.NewReader(body))
	httpRequest.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httpRequest)
	if response.Code != http.StatusOK {
		t.Fatalf("query status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded apiQueryResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	return decoded
}

func exerciseHTTPQueries(t *testing.T, handler http.Handler) querySnapshot {
	t.Helper()
	activity := map[string]any{
		"field": "activity", "operator": "overlaps",
		"value": map[string]any{"from": "2026-08-21T09:00:00Z", "to": "2026-08-21T14:00:00Z"},
	}
	all := queryHTTP(t, handler, map[string]any{"filters": []any{activity}})
	active := queryHTTP(t, handler, map[string]any{})
	admin := queryHTTP(t, handler, map[string]any{"filters": []any{
		map[string]any{"field": "tenantId", "operator": "eq", "value": "tenant-a"},
		map[string]any{"field": "username", "operator": "eq", "value": "alice"},
		map[string]any{"field": "activity", "operator": "at", "value": "2026-08-21T11:00:00Z"},
		map[string]any{"field": "tags", "operator": "containsAll", "value": []string{"admin"}},
	}})
	sharedIP := queryHTTP(t, handler, map[string]any{"filters": []any{
		map[string]any{"field": "ip", "operator": "eq", "value": "192.0.2.10"}, activity,
	}})
	if len(sharedIP.Sessions) != 3 {
		t.Fatalf("shared-IP query sessions = %+v, want 3", sharedIP.Sessions)
	}

	var pages []string
	cursor := ""
	for {
		request := map[string]any{"filters": []any{activity}, "page": map[string]any{"limit": 1}}
		if cursor != "" {
			request["page"].(map[string]any)["cursor"] = cursor
		}
		page := queryHTTP(t, handler, request)
		if len(page.Sessions) != 1 {
			t.Fatalf("pagination page = %+v", page)
		}
		pages = append(pages, page.Sessions[0].ID)
		cursor = page.NextCursor
		if cursor == "" {
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
		AllIDs: ids(all.Sessions), ActiveIDs: ids(active.Sessions), AdminIDs: ids(admin.Sessions),
		AliceStates: aliceStates, PaginatedIDs: pages,
	}
}

func ids(sessions []apiSession) []string {
	result := make([]string, len(sessions))
	for index, value := range sessions {
		result[index] = value.ID
	}
	return result
}

func waitFor(t *testing.T, description string, condition func() bool) {
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
