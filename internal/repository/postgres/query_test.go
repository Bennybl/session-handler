package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Bennybl/session-handler/internal/query"
	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

var secondSessionID = sessiontest.SessionID(102)

func TestQueryContract(t *testing.T) {
	runContractCases(t, "generic state scoped filters", "pagination and stable cursors", "query result isolation")
}

func TestQuerySupportsEveryRegistryEntry(t *testing.T) {
	repo := freshRepository(t)
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	loginAt := sessiontest.At("10:00")
	evaluatedAt := sessiontest.At("10:30")
	sessiontest.Login(t, repo, key, firstSessionID, loginAt, "user")

	// Every filter below describes the one seeded session, so each registry
	// entry is exercised against real SQL.
	tests := []struct {
		name   string
		filter session.Filter
	}{
		{name: "session ID equals", filter: sessiontest.Filter("sessionId", "eq", firstSessionID)},
		{name: "session ID in", filter: sessiontest.Filter("sessionId", "in", []string{secondSessionID, firstSessionID})},
		{name: "tenant equals", filter: sessiontest.Filter("tenantId", "eq", "tenant-a")},
		{name: "tenant in", filter: sessiontest.Filter("tenantId", "in", []string{"tenant-b", "tenant-a"})},
		{name: "username equals", filter: sessiontest.Filter("username", "eq", "alice")},
		{name: "username in", filter: sessiontest.Filter("username", "in", []string{"bob", "alice"})},
		{name: "IP equals", filter: sessiontest.Filter("ip", "eq", "192.0.2.10")},
		{name: "IP in", filter: sessiontest.Filter("ip", "in", []string{"192.0.2.11", "192.0.2.10"})},
		{name: "tags contain all", filter: sessiontest.Filter("tags", "containsAll", []string{"user"})},
		{name: "tags contain any", filter: sessiontest.Filter("tags", "containsAny", []string{"missing", "user"})},
		{name: "activity at", filter: sessiontest.Filter("activity", "at", evaluatedAt)},
		{name: "activity overlaps", filter: sessiontest.Filter("activity", "overlaps", interval("10:01", "10:30"))},
		{name: "login time equals", filter: sessiontest.Filter("loginTime", "eq", loginAt)},
		{name: "login time greater than", filter: sessiontest.Filter("loginTime", "gt", sessiontest.At("09:59"))},
		{name: "login time greater or equal", filter: sessiontest.Filter("loginTime", "gte", loginAt)},
		{name: "login time less than", filter: sessiontest.Filter("loginTime", "lt", sessiontest.At("10:01"))},
		{name: "login time less or equal", filter: sessiontest.Filter("loginTime", "lte", loginAt)},
		{name: "login time between", filter: sessiontest.Filter("loginTime", "between", interval("09:59", "10:01"))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := sessiontest.Query(t, repo, sessiontest.Spec(evaluatedAt, test.filter))
			if len(result.Sessions) != 1 || result.Sessions[0].ID != firstSessionID {
				t.Fatalf("matching sessions = %+v, want only %q", result.Sessions, firstSessionID)
			}
		})
	}
}

// Activity and tag filters compile into one correlated predicate, so they must
// be satisfied by the same state row.
func TestQueryUsesOneMatchingStateAndLoadsCompleteHistory(t *testing.T) {
	repo := freshRepository(t)
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	loginAt, updateAt := sessiontest.At("10:00"), sessiontest.At("11:00")
	evaluatedAt := sessiontest.At("11:01")
	sessiontest.Login(t, repo, key, firstSessionID, loginAt, "user")
	sessiontest.Update(t, repo, key, updateAt, "admin")

	// At 10:30 the session was tagged "user", not "admin".
	mismatched := sessiontest.Query(t, repo, sessiontest.Spec(evaluatedAt,
		sessiontest.Filter("activity", "at", sessiontest.At("10:30")),
		sessiontest.Filter("tags", "containsAll", []string{"admin"}),
	))
	if len(mismatched.Sessions) != 0 {
		t.Fatalf("matching sessions = %+v, want none; the filters matched different states", mismatched.Sessions)
	}

	// After 11:00 both filters describe the same state, and the response then
	// carries the session's complete history.
	matched := sessiontest.Query(t, repo, sessiontest.Spec(evaluatedAt,
		sessiontest.Filter("activity", "at", evaluatedAt),
		sessiontest.Filter("tags", "containsAll", []string{"admin"}),
	))
	if len(matched.Sessions) != 1 {
		t.Fatalf("matching sessions = %+v, want one", matched.Sessions)
	}
	if len(matched.Sessions[0].States) != 2 {
		t.Fatalf("states = %+v, want the complete two-state history", matched.Sessions[0].States)
	}
}

// Paging counts sessions, not the state rows they join to, so a session with
// several matching states still occupies one slot on a page.
func TestQueryPagesDistinctSessionsAcrossKeysetBoundaries(t *testing.T) {
	repo := freshRepository(t)
	loginAt, updateAt := sessiontest.At("10:00"), sessiontest.At("11:00")
	alice := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	sessiontest.Login(t, repo, alice, firstSessionID, loginAt, "user")
	sessiontest.Update(t, repo, alice, updateAt, "user")
	sessiontest.Login(t, repo, sessiontest.Key("tenant-a", "bob", "192.0.2.10"), secondSessionID, loginAt, "user")

	spec := session.QuerySpec{
		Filters:     []session.Filter{sessiontest.Filter("activity", "overlaps", interval("10:00", "12:00"))},
		Page:        session.PageRequest{Limit: 1},
		EvaluatedAt: updateAt,
	}
	first := sessiontest.Query(t, repo, spec)
	if len(first.Sessions) != 1 || first.Sessions[0].ID != firstSessionID || first.NextCursor == "" {
		t.Fatalf("first page = %+v, want only %q and a cursor", first, firstSessionID)
	}
	if len(first.Sessions[0].States) != 2 {
		t.Fatalf("first page states = %+v, want both of Alice's states", first.Sessions[0].States)
	}

	spec.Page.Cursor = first.NextCursor
	second := sessiontest.Query(t, repo, spec)
	if len(second.Sessions) != 1 || second.Sessions[0].ID != secondSessionID || second.NextCursor != "" {
		t.Fatalf("second page = %+v, want only %q and no cursor", second, secondSessionID)
	}
}

// Field names, operators, and values never reach the SQL text: unknown names
// are rejected and values are always bound as parameters.
func TestQueryRejectsUnsupportedAndInjectionShapedInputs(t *testing.T) {
	repo := freshRepository(t)
	ctx := context.Background()
	at := sessiontest.At("10:00")
	evaluatedAt := sessiontest.At("10:01")
	sessiontest.Login(t, repo, sessiontest.Key("tenant-a", "alice", "192.0.2.10"), firstSessionID, at, "user")

	rejected := []struct {
		name   string
		filter session.Filter
	}{
		{name: "injected field", filter: sessiontest.Filter("tenant_id) = 'tenant-a' OR TRUE --", "eq", "tenant-a")},
		{name: "injected operator", filter: sessiontest.Filter("tenantId", "eq OR TRUE --", "tenant-a")},
		{name: "injected value for a UUID field", filter: sessiontest.Filter("sessionId", "eq", "' OR TRUE --")},
	}
	for _, test := range rejected {
		t.Run(test.name, func(t *testing.T) {
			_, err := repo.Query(ctx, sessiontest.Spec(evaluatedAt, test.filter))
			if !errors.Is(err, repository.ErrInvalidQuery) {
				t.Fatalf("Query() error = %v, want ErrInvalidQuery", err)
			}
		})
	}

	// An injection-shaped value on a string field is a value, so it matches
	// nothing and leaves the store intact.
	injected := sessiontest.Query(t, repo, sessiontest.Spec(evaluatedAt,
		sessiontest.Filter("tenantId", "eq", "tenant-a' OR TRUE --"),
	))
	if len(injected.Sessions) != 0 {
		t.Fatalf("matching sessions = %+v, want none", injected.Sessions)
	}
	surviving := sessiontest.Query(t, repo, sessiontest.Spec(evaluatedAt,
		sessiontest.Filter("tenantId", "eq", "tenant-a"),
	))
	if len(surviving.Sessions) != 1 {
		t.Fatalf("matching sessions after the injection attempts = %+v, want the seeded session", surviving.Sessions)
	}
}

func interval(from, to string) query.IntervalValue {
	return query.IntervalValue{From: sessiontest.Ptr(sessiontest.At(from)), To: sessiontest.Ptr(sessiontest.At(to))}
}
