package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/repository/memory"
	"github.com/Bennybl/session-handler/internal/service"
	"github.com/Bennybl/session-handler/internal/session"
)

func TestQueryDecodesGenericSpecificationAndEncodesCompleteHistory(t *testing.T) {
	t.Parallel()
	loginAt := mustHTTPTime(t, "2026-08-21T10:00:00Z")
	changedAt := mustHTTPTime(t, "2026-08-21T10:30:00Z")
	logoutAt := mustHTTPTime(t, "2026-08-21T11:00:00Z")
	wantResult := session.QueryResult{
		Sessions: []session.Session{{
			ID:      "00000000-0000-4000-8000-000000001101",
			Key:     session.SessionKey{TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10"},
			LoginAt: loginAt, LogoutAt: &logoutAt, LastEventID: "internal-event-id",
			States: []session.SessionState{
				{Tags: []string{"user"}, ValidFrom: loginAt, ValidTo: &changedAt},
				{Tags: []string{"admin", "user"}, ValidFrom: changedAt, ValidTo: &logoutAt},
			},
		}},
		NextCursor: "next-page",
	}
	var gotRequest service.QueryRequest
	query := queryServiceFunc(func(_ context.Context, request service.QueryRequest) (session.QueryResult, error) {
		gotRequest = request
		return wantResult, nil
	})
	handler := mustHandler(t, query)
	body := `{
		"filters": [
			{"field":"tenantId","operator":"eq","value":"tenant-a"},
			{"field":"activity","operator":"overlaps","value":{"from":"2026-08-21T10:00:00Z","to":"2026-08-21T12:00:00Z"}},
			{"field":"tags","operator":"containsAll","value":["admin","user"]}
		],
		"page":{"limit":7,"cursor":"current-page"}
	}`

	response := performRequest(handler, http.MethodPost, "/v1/sessions/query", body)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if gotRequest.Page != (session.PageRequest{Limit: 7, Cursor: "current-page"}) || len(gotRequest.Filters) != 3 {
		t.Fatalf("query request = %+v", gotRequest)
	}
	if gotRequest.Filters[0].Field != "tenantId" || gotRequest.Filters[0].Operator != "eq" || gotRequest.Filters[0].Value != "tenant-a" {
		t.Fatalf("first filter = %+v", gotRequest.Filters[0])
	}
	interval, ok := gotRequest.Filters[1].Value.(map[string]any)
	if !ok || interval["from"] != "2026-08-21T10:00:00Z" || interval["to"] != "2026-08-21T12:00:00Z" {
		t.Fatalf("interval value = %#v", gotRequest.Filters[1].Value)
	}
	if !reflect.DeepEqual(gotRequest.Filters[2].Value, []any{"admin", "user"}) {
		t.Fatalf("tag value = %#v", gotRequest.Filters[2].Value)
	}

	var decoded queryHTTPResponse
	decodeHTTPJSON(t, response, &decoded)
	if decoded.NextCursor != "next-page" || len(decoded.Sessions) != 1 || len(decoded.Sessions[0].States) != 2 {
		t.Fatalf("query response = %+v", decoded)
	}
	got := decoded.Sessions[0]
	if got.ID != wantResult.Sessions[0].ID || got.TenantID != "tenant-a" || got.Username != "alice" || got.IP != "192.0.2.10" ||
		!got.LoginAt.Equal(loginAt) || got.LogoutAt == nil || !got.LogoutAt.Equal(logoutAt) {
		t.Fatalf("session response = %+v", got)
	}
	if strings.Contains(response.Body.String(), "internal-event-id") || strings.Contains(response.Body.String(), "lastEventId") {
		t.Fatalf("response exposed internal event identity: %s", response.Body.String())
	}
}

func TestQueryRejectsMalformedAndUnsupportedRequests(t *testing.T) {
	t.Parallel()
	handler := newMemoryHandler(t, mustHTTPTime(t, "2026-08-21T12:00:00Z"), nil)
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
		{name: "invalid limit", body: `{"page":{"limit":201}}`},
		{name: "invalid cursor", body: `{"page":{"cursor":"not-a-cursor"}}`},
		{name: "unknown page field", body: `{"page":{"offset":1}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := performRequest(handler, http.MethodPost, "/v1/sessions/query", test.body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body = %s", response.Code, response.Body.String())
			}
			var problem errorHTTPResponse
			decodeHTTPJSON(t, response, &problem)
			if problem.Error == "" {
				t.Fatal("error response has no message")
			}
		})
	}
}

func TestQueryEmptyFiltersDefaultToNowAndCursorPreservesEvaluationTime(t *testing.T) {
	t.Parallel()
	now := mustHTTPTime(t, "2026-08-21T12:00:00Z")
	currentTime := now
	application, handler := seededMemoryHandler(t, func() time.Time { return currentTime })

	first := performRequest(handler, http.MethodPost, "/v1/sessions/query", `{"page":{"limit":1}}`)
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", first.Code, first.Body.String())
	}
	var firstPage queryHTTPResponse
	decodeHTTPJSON(t, first, &firstPage)
	if len(firstPage.Sessions) != 1 || firstPage.NextCursor == "" {
		t.Fatalf("first page = %+v", firstPage)
	}

	for index, key := range []struct{ username, ip string }{{"alice", "192.0.2.10"}, {"bob", "192.0.2.11"}} {
		err := application.ApplyEvent(context.Background(), service.Event{
			EventID: "92000000-0000-4000-8000-00000000000" + string(rune('1'+index)),
			Type:    service.EventLogout, TenantID: "tenant-a", Username: key.username, IP: key.ip,
			Timestamp: now.Add(time.Hour),
		})
		if err != nil {
			t.Fatalf("logout %s: %v", key.username, err)
		}
	}
	currentTime = now.Add(2 * time.Hour)
	secondBody, err := json.Marshal(map[string]any{"page": map[string]any{"limit": 1, "cursor": firstPage.NextCursor}})
	if err != nil {
		t.Fatalf("encode second request: %v", err)
	}
	second := performRequest(handler, http.MethodPost, "/v1/sessions/query", string(secondBody))
	if second.Code != http.StatusOK {
		t.Fatalf("second status = %d, body = %s", second.Code, second.Body.String())
	}
	var secondPage queryHTTPResponse
	decodeHTTPJSON(t, second, &secondPage)
	if len(secondPage.Sessions) != 1 || secondPage.NextCursor != "" || secondPage.Sessions[0].ID == firstPage.Sessions[0].ID {
		t.Fatalf("second page = %+v after first = %+v", secondPage, firstPage)
	}

	withoutCursor := performRequest(handler, http.MethodPost, "/v1/sessions/query", `{}`)
	var current queryHTTPResponse
	decodeHTTPJSON(t, withoutCursor, &current)
	if withoutCursor.Code != http.StatusOK || len(current.Sessions) != 0 {
		t.Fatalf("current page = %+v, status = %d", current, withoutCursor.Code)
	}
}

func TestOnlyQueryBusinessRouteIsExposed(t *testing.T) {
	t.Parallel()
	calls := 0
	handler := mustHandler(t, queryServiceFunc(func(context.Context, service.QueryRequest) (session.QueryResult, error) {
		calls++
		return session.QueryResult{}, nil
	}))

	event := performRequest(handler, http.MethodPost, "/v1/sessions/events", `{}`)
	if event.Code != http.StatusNotFound || calls != 0 {
		t.Fatalf("event route status = %d, query calls = %d", event.Code, calls)
	}
	wrongMethod := performRequest(handler, http.MethodGet, "/v1/sessions/query", "")
	if wrongMethod.Code != http.StatusMethodNotAllowed || calls != 0 {
		t.Fatalf("GET query status = %d, query calls = %d", wrongMethod.Code, calls)
	}
}

func TestQueryMapsUnexpectedServiceFailureToInternalServerError(t *testing.T) {
	t.Parallel()
	handler := mustHandler(t, queryServiceFunc(func(context.Context, service.QueryRequest) (session.QueryResult, error) {
		return session.QueryResult{}, errors.New("database password must stay private")
	}))
	response := performRequest(handler, http.MethodPost, "/v1/sessions/query", `{}`)
	if response.Code != http.StatusInternalServerError || strings.Contains(response.Body.String(), "password") {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

type queryServiceFunc func(context.Context, service.QueryRequest) (session.QueryResult, error)

func (function queryServiceFunc) Query(ctx context.Context, request service.QueryRequest) (session.QueryResult, error) {
	return function(ctx, request)
}

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

func mustHandler(t *testing.T, query QueryService) http.Handler {
	t.Helper()
	handler, err := NewHandler(query)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	return handler
}

func newMemoryHandler(t *testing.T, now time.Time, ids []string) http.Handler {
	t.Helper()
	repository := memory.New()
	t.Cleanup(func() { _ = repository.Close() })
	index := 0
	application, err := service.New(service.Dependencies{
		Repository: repository,
		Now:        func() time.Time { return now },
		NewSessionID: func() (string, error) {
			if index >= len(ids) {
				return "00000000-0000-4000-8000-000000009999", nil
			}
			id := ids[index]
			index++
			return id, nil
		},
	})
	if err != nil {
		t.Fatalf("service.New() error = %v", err)
	}
	return mustHandler(t, application)
}

func seededMemoryHandler(t *testing.T, now func() time.Time) (*service.SessionService, http.Handler) {
	t.Helper()
	repository := memory.New()
	t.Cleanup(func() { _ = repository.Close() })
	ids := []string{
		"00000000-0000-4000-8000-000000001101",
		"00000000-0000-4000-8000-000000001102",
		"00000000-0000-4000-8000-000000001103",
	}
	index := 0
	application, err := service.New(service.Dependencies{
		Repository: repository,
		Now:        now,
		NewSessionID: func() (string, error) {
			id := ids[index]
			index++
			return id, nil
		},
	})
	if err != nil {
		t.Fatalf("service.New() error = %v", err)
	}
	events := []service.Event{
		{EventID: "91000000-0000-4000-8000-000000000001", Type: service.EventLogin, TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10", Tags: []string{"user"}, Timestamp: mustHTTPTime(t, "2026-08-21T10:00:00Z")},
		{EventID: "91000000-0000-4000-8000-000000000002", Type: service.EventLogin, TenantID: "tenant-a", Username: "bob", IP: "192.0.2.11", Tags: []string{"admin"}, Timestamp: mustHTTPTime(t, "2026-08-21T11:00:00Z")},
		{EventID: "91000000-0000-4000-8000-000000000003", Type: service.EventLogin, TenantID: "tenant-a", Username: "carol", IP: "192.0.2.12", Tags: []string{"past"}, Timestamp: mustHTTPTime(t, "2026-08-21T08:00:00Z")},
		{EventID: "91000000-0000-4000-8000-000000000004", Type: service.EventLogout, TenantID: "tenant-a", Username: "carol", IP: "192.0.2.12", Timestamp: mustHTTPTime(t, "2026-08-21T09:00:00Z")},
	}
	for _, event := range events {
		if err := application.ApplyEvent(context.Background(), event); err != nil {
			t.Fatalf("seed event %s: %v", event.EventID, err)
		}
	}
	return application, mustHandler(t, application)
}

func performRequest(handler http.Handler, method, target, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func decodeHTTPJSON(t *testing.T, response *httptest.ResponseRecorder, destination any) {
	t.Helper()
	if err := json.NewDecoder(response.Body).Decode(destination); err != nil {
		t.Fatalf("decode response %q: %v", response.Body.String(), err)
	}
}

func mustHTTPTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("time.Parse(%q) error = %v", value, err)
	}
	return parsed
}
