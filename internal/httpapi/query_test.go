package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/repository/memory"
	"github.com/Bennybl/session-handler/internal/service"
	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

const queryPath = "/v1/sessions/query"

// The handler decodes a generic filter list without interpreting it, and
// answers with complete session histories.
func TestQueryDecodesGenericSpecificationAndEncodesCompleteHistory(t *testing.T) {
	t.Parallel()

	loginAt, changedAt, logoutAt := sessiontest.At("10:00"), sessiontest.At("10:30"), sessiontest.At("11:00")
	sessionID := sessiontest.SessionID(1101)
	result := session.QueryResult{
		Sessions: []session.Session{{
			ID:      sessionID,
			Key:     sessiontest.Key("tenant-a", "alice", "192.0.2.10"),
			LoginAt: loginAt, LogoutAt: &logoutAt, LastEventID: "internal-event-id",
			States: []session.SessionState{
				{Tags: []string{"user"}, ValidFrom: loginAt, ValidTo: &changedAt},
				{Tags: []string{"admin", "user"}, ValidFrom: changedAt, ValidTo: &logoutAt},
			},
		}},
		NextCursor: "next-page",
	}
	var got service.QueryRequest
	handler := newHandler(t, queryServiceFunc(func(_ context.Context, request service.QueryRequest) (session.QueryResult, error) {
		got = request
		return result, nil
	}))

	response := postQuery(handler, `{
		"filters": [
			{"field":"tenantId","operator":"eq","value":"tenant-a"},
			{"field":"activity","operator":"overlaps","value":{"from":"2026-08-21T10:00:00Z","to":"2026-08-21T12:00:00Z"}},
			{"field":"tags","operator":"containsAll","value":["admin","user"]}
		],
		"page":{"limit":7,"cursor":"current-page"}
	}`)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}

	// Each filter reaches the service with its value shape intact.
	if want := (session.PageRequest{Limit: 7, Cursor: "current-page"}); got.Page != want {
		t.Errorf("page = %+v, want %+v", got.Page, want)
	}
	if len(got.Filters) != 3 {
		t.Fatalf("filters = %+v, want 3", got.Filters)
	}
	if want := sessiontest.Filter("tenantId", "eq", "tenant-a"); got.Filters[0] != want {
		t.Errorf("scalar filter = %+v, want %+v", got.Filters[0], want)
	}
	wantInterval := map[string]any{"from": "2026-08-21T10:00:00Z", "to": "2026-08-21T12:00:00Z"}
	if !reflect.DeepEqual(got.Filters[1].Value, wantInterval) {
		t.Errorf("interval filter value = %#v, want %#v", got.Filters[1].Value, wantInterval)
	}
	if want := []any{"admin", "user"}; !reflect.DeepEqual(got.Filters[2].Value, want) {
		t.Errorf("list filter value = %#v, want %#v", got.Filters[2].Value, want)
	}

	var decoded queryHTTPResponse
	decodeJSON(t, response, &decoded)
	if decoded.NextCursor != "next-page" {
		t.Errorf("nextCursor = %q, want %q", decoded.NextCursor, "next-page")
	}
	if len(decoded.Sessions) != 1 {
		t.Fatalf("sessions = %+v, want 1", decoded.Sessions)
	}
	encoded := decoded.Sessions[0]
	if encoded.ID != sessionID || encoded.TenantID != "tenant-a" || encoded.Username != "alice" || encoded.IP != "192.0.2.10" {
		t.Errorf("session identity = %+v, want %q for tenant-a/alice/192.0.2.10", encoded, sessionID)
	}
	if !encoded.LoginAt.Equal(loginAt) || encoded.LogoutAt == nil || !encoded.LogoutAt.Equal(logoutAt) {
		t.Errorf("lifecycle = %v to %v, want %v to %v", encoded.LoginAt, encoded.LogoutAt, loginAt, logoutAt)
	}
	if len(encoded.States) != 2 {
		t.Errorf("states = %+v, want the complete two-state history", encoded.States)
	}

	// The event ID is internal deduplication metadata and must not leak.
	if body := response.Body.String(); strings.Contains(body, "internal-event-id") || strings.Contains(body, "lastEventId") {
		t.Errorf("response exposed the internal event identity: %s", body)
	}
}

func TestQueryRejectsMalformedRequestsAndHidesUnexpectedFailures(t *testing.T) {
	t.Parallel()

	_, handler := newMemoryHandler(t, fixedClock(sessiontest.At("12:00")))
	tests := []struct {
		name string
		body string
	}{
		{name: "empty body", body: ""},
		{name: "wrong top-level shape", body: `[]`},
		{name: "trailing JSON", body: `{ } { }`},
		{name: "unknown request field", body: `{"unexpected":true}`},
		{name: "wrong filters shape", body: `{"filters":{}}`},
		{name: "unknown filter field", body: `{"filters":[{"field":"tenantId","operator":"eq","value":"tenant-a","extra":true}]}`},
		{name: "missing field", body: `{"filters":[{"operator":"eq","value":"tenant-a"}]}`},
		{name: "missing operator", body: `{"filters":[{"field":"tenantId","value":"tenant-a"}]}`},
		{name: "missing value", body: `{"filters":[{"field":"tenantId","operator":"eq"}]}`},
		{name: "null value", body: `{"filters":[{"field":"tenantId","operator":"eq","value":null}]}`},
		{name: "unsupported operator", body: `{"filters":[{"field":"tenantId","operator":"contains","value":"tenant-a"}]}`},
		{name: "unsupported logical field", body: `{"filters":[{"field":"email","operator":"eq","value":"a@example.com"}]}`},
		{name: "limit above the maximum", body: `{"page":{"limit":201}}`},
		{name: "invalid cursor", body: `{"page":{"cursor":"not-a-cursor"}}`},
		{name: "unknown page field", body: `{"page":{"offset":1}}`},
	}

	for _, test := range tests {
		response := postQuery(handler, test.body)
		if response.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400; body = %s", test.name, response.Code, response.Body.String())
			continue
		}
		var problem errorHTTPResponse
		decodeJSON(t, response, &problem)
		if problem.Error == "" {
			t.Errorf("%s: the error response carries no message", test.name)
		}
	}

	// Queries are the only business route; events arrive through the stream.
	queries := 0
	counting := newHandler(t, queryServiceFunc(func(context.Context, service.QueryRequest) (session.QueryResult, error) {
		queries++
		return session.QueryResult{}, nil
	}))
	if got := request(counting, http.MethodPost, "/v1/sessions/events", `{}`); got.Code != http.StatusNotFound {
		t.Errorf("POST /v1/sessions/events status = %d, want 404", got.Code)
	}
	if got := request(counting, http.MethodGet, queryPath, ""); got.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET %s status = %d, want 405", queryPath, got.Code)
	}
	if queries != 0 {
		t.Errorf("query service calls = %d, want 0", queries)
	}

	// An unexpected failure is a 500 that reveals nothing about its cause.
	failing := newHandler(t, queryServiceFunc(func(context.Context, service.QueryRequest) (session.QueryResult, error) {
		return session.QueryResult{}, errors.New("database password must stay private")
	}))
	response := postQuery(failing, `{}`)
	if response.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", response.Code)
	}
	if strings.Contains(response.Body.String(), "password") {
		t.Errorf("response leaked the internal failure: %s", response.Body.String())
	}
}

// Without an activity filter a query means "active now", and a cursor keeps
// that instant fixed so later pages stay consistent as the clock moves.
func TestQueryCursorPinsTheEvaluationTime(t *testing.T) {
	t.Parallel()

	now := sessiontest.At("12:00")
	currentTime := now
	application, handler := newMemoryHandler(t, func() time.Time { return currentTime },
		login("alice", "192.0.2.10", sessiontest.At("10:00"), "user"),
		login("bob", "192.0.2.11", sessiontest.At("11:00"), "admin"),
		login("carol", "192.0.2.12", sessiontest.At("08:00"), "past"),
		logout("carol", "192.0.2.12", sessiontest.At("09:00")),
	)

	// Carol logged out before now, so only Alice and Bob are active.
	first := decodeQuery(t, handler, `{"page":{"limit":1}}`)
	if len(first.Sessions) != 1 || first.NextCursor == "" {
		t.Fatalf("first page = %d sessions, cursor %q; want 1 session and a cursor", len(first.Sessions), first.NextCursor)
	}

	// Both log out, and the clock moves past their logout.
	for _, active := range []struct{ username, ip string }{{"alice", "192.0.2.10"}, {"bob", "192.0.2.11"}} {
		event := logout(active.username, active.ip, now.Add(time.Hour))
		if err := application.ApplyEvent(context.Background(), event); err != nil {
			t.Fatalf("log %s out: %v", active.username, err)
		}
	}
	currentTime = now.Add(2 * time.Hour)

	// The cursor still evaluates at 12:00, so the second page is reachable.
	second := decodeQuery(t, handler, fmt.Sprintf(`{"page":{"limit":1,"cursor":%q}}`, first.NextCursor))
	if len(second.Sessions) != 1 || second.NextCursor != "" {
		t.Fatalf("second page = %d sessions, cursor %q; want 1 session and no cursor", len(second.Sessions), second.NextCursor)
	}
	if second.Sessions[0].ID == first.Sessions[0].ID {
		t.Fatalf("both pages returned session %q", first.Sessions[0].ID)
	}

	// A fresh query uses the current clock, where nobody is active.
	current := decodeQuery(t, handler, `{}`)
	if len(current.Sessions) != 0 {
		t.Fatalf("sessions active at %v = %+v, want none", currentTime, current.Sessions)
	}
}

type queryServiceFunc func(context.Context, service.QueryRequest) (session.QueryResult, error)

func (function queryServiceFunc) Query(ctx context.Context, request service.QueryRequest) (session.QueryResult, error) {
	return function(ctx, request)
}

// The response shapes below are written out rather than reused from the handler
// so the tests pin the wire contract independently of the encoder.
type queryHTTPResponse struct {
	Sessions   []sessionHTTPResponse `json:"sessions"`
	NextCursor string                `json:"nextCursor"`
}

type sessionHTTPResponse struct {
	ID       string              `json:"id"`
	TenantID string              `json:"tenantId"`
	Username string              `json:"username"`
	IP       string              `json:"ip"`
	LoginAt  time.Time           `json:"loginAt"`
	LogoutAt *time.Time          `json:"logoutAt"`
	States   []stateHTTPResponse `json:"states"`
}

type stateHTTPResponse struct {
	Tags      []string   `json:"tags"`
	ValidFrom time.Time  `json:"validFrom"`
	ValidTo   *time.Time `json:"validTo"`
}

type errorHTTPResponse struct {
	Error string `json:"error"`
}

func newHandler(t *testing.T, query QueryService) http.Handler {
	t.Helper()
	handler, err := NewHandler(query)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	return handler
}

// newMemoryHandler builds the real service and handler over an in-memory store,
// applies the seed events, and returns both so a test can keep driving events.
func newMemoryHandler(t *testing.T, now func() time.Time, events ...service.Event) (*service.SessionService, http.Handler) {
	t.Helper()
	repo := memory.New()
	t.Cleanup(func() { _ = repo.Close() })

	assigned := 0
	application, err := service.New(service.Dependencies{
		Repository: repo,
		Now:        now,
		NewSessionID: func() (string, error) {
			assigned++
			return sessiontest.SessionID(1100 + assigned), nil
		},
	})
	if err != nil {
		t.Fatalf("service.New() error = %v", err)
	}
	for _, event := range events {
		if err := application.ApplyEvent(context.Background(), event); err != nil {
			t.Fatalf("seed %s for %s: %v", event.Type, event.Username, err)
		}
	}
	return application, newHandler(t, application)
}

func login(username, ip string, at time.Time, tags ...string) service.Event {
	return service.Event{
		EventID: sessiontest.NextEventID(), Type: service.EventLogin,
		TenantID: "tenant-a", Username: username, IP: ip, Tags: tags, Timestamp: at,
	}
}

func logout(username, ip string, at time.Time) service.Event {
	return service.Event{
		EventID: sessiontest.NextEventID(), Type: service.EventLogout,
		TenantID: "tenant-a", Username: username, IP: ip, Timestamp: at,
	}
}

func postQuery(handler http.Handler, body string) *httptest.ResponseRecorder {
	return request(handler, http.MethodPost, queryPath, body)
}

func decodeQuery(t *testing.T, handler http.Handler, body string) queryHTTPResponse {
	t.Helper()
	response := postQuery(handler, body)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded queryHTTPResponse
	decodeJSON(t, response, &decoded)
	return decoded
}

func request(handler http.Handler, method, target, body string) *httptest.ResponseRecorder {
	httpRequest := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	httpRequest.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httpRequest)
	return response
}

func decodeJSON(t *testing.T, response *httptest.ResponseRecorder, destination any) {
	t.Helper()
	if err := json.NewDecoder(response.Body).Decode(destination); err != nil {
		t.Fatalf("decode response %q: %v", response.Body.String(), err)
	}
}

func fixedClock(at time.Time) func() time.Time {
	return func() time.Time { return at }
}
