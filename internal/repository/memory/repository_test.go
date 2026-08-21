package memory_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/query"
	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/repository/memory"
	"github.com/Bennybl/session-handler/internal/repository/repositorytest"
	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

// The sessions newFilterFixture seeds.
var (
	aliceID   = sessiontest.SessionID(1)
	bobID     = sessiontest.SessionID(2)
	charlieID = sessiontest.SessionID(3)
)

// The complete shared adapter contract, including event-ID deduplication.
func TestRepositoryContract(t *testing.T) {
	repositorytest.Run(t, newRepository)
}

// Every registered field and operator resolves against real stored data, all
// state-scoped filters in one query must be satisfied by the same state, and a
// match returns the session's complete history.
func TestQueryFiltersOperatorsAndHistory(t *testing.T) {
	t.Parallel()

	repo := newFilterFixture(t)
	filters := []struct {
		name    string
		filter  session.Filter
		wantIDs []string
	}{
		{name: "session ID equals", filter: sessiontest.Filter("sessionId", "eq", aliceID), wantIDs: []string{aliceID}},
		{name: "session ID in", filter: sessiontest.Filter("sessionId", "in", []string{aliceID, charlieID}), wantIDs: []string{aliceID, charlieID}},
		{name: "tenant equals", filter: sessiontest.Filter("tenantId", "eq", "tenant-a"), wantIDs: []string{aliceID, bobID}},
		{name: "tenant in", filter: sessiontest.Filter("tenantId", "in", []string{"tenant-b"}), wantIDs: []string{charlieID}},
		{name: "username equals", filter: sessiontest.Filter("username", "eq", "alice"), wantIDs: []string{aliceID, charlieID}},
		{name: "username in", filter: sessiontest.Filter("username", "in", []string{"bob"}), wantIDs: []string{bobID}},
		{name: "IP equals", filter: sessiontest.Filter("ip", "eq", "192.0.2.10"), wantIDs: []string{aliceID, bobID}},
		{name: "IP in", filter: sessiontest.Filter("ip", "in", []string{"192.0.2.20"}), wantIDs: []string{charlieID}},
		{name: "tags contain all", filter: sessiontest.Filter("tags", "containsAll", []string{"admin", "user"}), wantIDs: []string{bobID}},
		{name: "tags contain any", filter: sessiontest.Filter("tags", "containsAny", []string{"ops"}), wantIDs: []string{charlieID}},
		{name: "activity at", filter: sessiontest.Filter("activity", "at", sessiontest.At("10:10")), wantIDs: []string{aliceID}},
		{name: "activity overlaps", filter: sessiontest.Filter("activity", "overlaps", interval("10:05", "10:12")), wantIDs: []string{aliceID}},
		{name: "login time equals", filter: sessiontest.Filter("loginTime", "eq", sessiontest.At("10:15")), wantIDs: []string{bobID}},
		{name: "login time greater than", filter: sessiontest.Filter("loginTime", "gt", sessiontest.At("10:15")), wantIDs: []string{charlieID}},
		{name: "login time greater or equal", filter: sessiontest.Filter("loginTime", "gte", sessiontest.At("10:15")), wantIDs: []string{bobID, charlieID}},
		{name: "login time less than", filter: sessiontest.Filter("loginTime", "lt", sessiontest.At("10:15")), wantIDs: []string{aliceID}},
		{name: "login time less or equal", filter: sessiontest.Filter("loginTime", "lte", sessiontest.At("10:15")), wantIDs: []string{aliceID, bobID}},
		{name: "login time between", filter: sessiontest.Filter("loginTime", "between", interval("10:10", "10:20")), wantIDs: []string{bobID}},
	}
	for _, test := range filters {
		result := sessiontest.Query(t, repo, sessiontest.Spec(sessiontest.At("10:45"), test.filter))
		if got := sortedIDs(result); !reflect.DeepEqual(got, test.wantIDs) {
			t.Errorf("%s: matching sessions = %v, want %v", test.name, got, test.wantIDs)
		}
	}

	// Alice carried "user" until 10:30 and "admin" afterwards, so no single
	// state is both active at 10:15 and tagged admin.
	acrossStates := sessiontest.Query(t, repo, sessiontest.Spec(sessiontest.At("10:45"),
		sessiontest.Filter("sessionId", "eq", aliceID),
		sessiontest.Filter("activity", "at", sessiontest.At("10:15")),
		sessiontest.Filter("tags", "containsAll", []string{"admin"}),
	))
	if len(acrossStates.Sessions) != 0 {
		t.Errorf("matching sessions = %+v, want none; the filters matched different states", acrossStates.Sessions)
	}

	// One matching state selects the session; the response carries all of them.
	matched := sessiontest.Query(t, repo, sessiontest.Spec(sessiontest.At("12:00"),
		sessiontest.Filter("sessionId", "eq", aliceID),
		sessiontest.Filter("activity", "overlaps", interval("10:00", "11:00")),
	))
	if len(matched.Sessions) != 1 {
		t.Fatalf("matching sessions = %+v, want only Alice", matched.Sessions)
	}
	alice := matched.Sessions[0]
	if alice.LogoutAt == nil || !alice.LogoutAt.Equal(sessiontest.At("11:00")) {
		t.Errorf("LogoutAt = %v, want 11:00", alice.LogoutAt)
	}
	if len(alice.States) != 2 {
		t.Fatalf("states = %+v, want the complete two-state history", alice.States)
	}
	if alice.States[0].ValidTo == nil || !alice.States[0].ValidTo.Equal(sessiontest.At("10:30")) {
		t.Errorf("first state ended at %v, want 10:30", alice.States[0].ValidTo)
	}
}

// A cursor pins the instant the first page was evaluated at, so later pages stay
// consistent even when every session has logged out by the time they are asked
// for.
func TestQueryCursorPinsTheDefaultActivityTime(t *testing.T) {
	t.Parallel()

	repo := newRepository(t)
	for index, username := range []string{"alice", "bob"} {
		key := sessiontest.Key("tenant-a", username, fmt.Sprintf("192.0.2.%d", index+10))
		sessiontest.Login(t, repo, key, sessiontest.SessionID(index+1), sessiontest.At("10:00"), "user")
		sessiontest.Logout(t, repo, key, sessiontest.At("10:30"))
	}

	first := sessiontest.Query(t, repo, session.QuerySpec{
		Page:        session.PageRequest{Limit: 1},
		EvaluatedAt: sessiontest.At("10:15"),
	})
	if len(first.Sessions) != 1 || first.NextCursor == "" {
		t.Fatalf("first page = %d sessions, cursor %q; want 1 session and a cursor", len(first.Sessions), first.NextCursor)
	}

	second := sessiontest.Query(t, repo, session.QuerySpec{
		Page:        session.PageRequest{Limit: 1, Cursor: first.NextCursor},
		EvaluatedAt: sessiontest.At("11:30"),
	})
	if len(second.Sessions) != 1 || second.NextCursor != "" {
		t.Fatalf("second page = %d sessions, cursor %q; want 1 session and no cursor", len(second.Sessions), second.NextCursor)
	}
	if first.Sessions[0].ID == second.Sessions[0].ID {
		t.Errorf("both pages returned session %q", first.Sessions[0].ID)
	}
}

// Mutations and queries run together without racing, and a closed store refuses
// both rather than serving stale data.
func TestConcurrentAccessAndClosedRepository(t *testing.T) {
	repo := newRepository(t)
	at := sessiontest.At("10:00")
	keys := make([]session.SessionKey, 8)
	for index := range keys {
		keys[index] = sessiontest.Key("tenant-a", fmt.Sprintf("user-%d", index), fmt.Sprintf("192.0.2.%d", index+1))
		sessiontest.Login(t, repo, keys[index], sessiontest.SessionID(index+1), at, "initial")
	}

	var wait sync.WaitGroup
	failures := make(chan error, len(keys)+4)
	for index, key := range keys {
		wait.Add(1)
		go func() {
			defer wait.Done()
			err := repo.Mutate(context.Background(), key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
				return session.DecideUpdate(snapshot, session.UpdateCommand{
					EventID: sessiontest.NextEventID(), Key: key,
					Tags: []string{fmt.Sprintf("updated-%d", index)}, Timestamp: at.Add(time.Minute),
				})
			})
			if err != nil {
				failures <- fmt.Errorf("mutate %s: %w", key.Username, err)
			}
		}()
	}
	for range 4 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 25 {
				if _, err := repo.Query(context.Background(), session.QuerySpec{EvaluatedAt: at.Add(time.Hour)}); err != nil {
					failures <- fmt.Errorf("query: %w", err)
					return
				}
			}
		}()
	}
	wait.Wait()
	close(failures)
	for err := range failures {
		t.Errorf("concurrent operation error = %v", err)
	}

	closed := memory.New()
	if err := closed.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	mutateError := closed.Mutate(context.Background(), key, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
		return nil, nil
	})
	if !errors.Is(mutateError, repository.ErrClosed) {
		t.Errorf("Mutate() on a closed store = %v, want ErrClosed", mutateError)
	}
	if _, err := closed.Query(context.Background(), session.QuerySpec{EvaluatedAt: at}); !errors.Is(err, repository.ErrClosed) {
		t.Errorf("Query() on a closed store = %v, want ErrClosed", err)
	}
}

func newRepository(t *testing.T) repository.SessionRepository {
	t.Helper()
	repo := memory.New()
	t.Cleanup(func() { _ = repo.Close() })
	return repo
}

// newFilterFixture seeds three sessions that differ in tenant, username, IP,
// login time, and tags, so one store exercises every registered filter.
//
//	alice   tenant-a alice 192.0.2.10  10:00 [user] -> 10:30 [admin] -> out 11:00
//	bob     tenant-a bob   192.0.2.10  10:15 [admin user]
//	charlie tenant-b alice 192.0.2.20  10:20 [ops]
func newFilterFixture(t *testing.T) repository.SessionRepository {
	t.Helper()
	repo := newRepository(t)
	alice := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	sessiontest.Login(t, repo, alice, aliceID, sessiontest.At("10:00"), "user")
	sessiontest.Update(t, repo, alice, sessiontest.At("10:30"), "admin")
	sessiontest.Logout(t, repo, alice, sessiontest.At("11:00"))
	sessiontest.Login(t, repo, sessiontest.Key("tenant-a", "bob", "192.0.2.10"), bobID, sessiontest.At("10:15"), "admin", "user")
	sessiontest.Login(t, repo, sessiontest.Key("tenant-b", "alice", "192.0.2.20"), charlieID, sessiontest.At("10:20"), "ops")
	return repo
}

func interval(from, to string) query.IntervalValue {
	return query.IntervalValue{From: sessiontest.Ptr(sessiontest.At(from)), To: sessiontest.Ptr(sessiontest.At(to))}
}

// sortedIDs returns the matching session IDs in a stable order, because these
// checks assert which sessions matched rather than how they were ordered.
func sortedIDs(result session.QueryResult) []string {
	ids := sessiontest.SessionIDs(result)
	sort.Strings(ids)
	return ids
}
