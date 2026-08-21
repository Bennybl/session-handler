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
)

const (
	aliceID       = "00000000-0000-0000-0000-000000000001"
	bobID         = "00000000-0000-0000-0000-000000000002"
	charlieID     = "00000000-0000-0000-0000-000000000003"
	loginEventID  = "20000000-0000-4000-8000-000000000001"
	updateEventID = "20000000-0000-4000-8000-000000000002"
	logoutEventID = "20000000-0000-4000-8000-000000000003"
)

func TestRepositoryContract(t *testing.T) {
	repositorytest.Run(t, func(t *testing.T) repository.SessionRepository {
		return memory.New()
	})
}

func TestEventIDContract(t *testing.T) {
	repositorytest.EventIDCase().Run(t, func(t *testing.T) repository.SessionRepository {
		return memory.New()
	})
}

func TestEveryRegisteredFilterAndOperator(t *testing.T) {
	t.Parallel()

	repo, fixture := newFilterFixture(t)
	tests := []struct {
		name    string
		filter  session.Filter
		wantIDs []string
	}{
		{name: "session ID equals", filter: filter("sessionId", "eq", aliceID), wantIDs: []string{aliceID}},
		{name: "session ID in", filter: filter("sessionId", "in", []string{aliceID, charlieID}), wantIDs: []string{aliceID, charlieID}},
		{name: "tenant equals", filter: filter("tenantId", "eq", "tenant-a"), wantIDs: []string{aliceID, bobID}},
		{name: "tenant in", filter: filter("tenantId", "in", []string{"tenant-b"}), wantIDs: []string{charlieID}},
		{name: "username equals", filter: filter("username", "eq", "alice"), wantIDs: []string{aliceID, charlieID}},
		{name: "username in", filter: filter("username", "in", []string{"bob"}), wantIDs: []string{bobID}},
		{name: "IP equals", filter: filter("ip", "eq", "192.0.2.10"), wantIDs: []string{aliceID, bobID}},
		{name: "IP in", filter: filter("ip", "in", []string{"192.0.2.20"}), wantIDs: []string{charlieID}},
		{name: "tags contain all", filter: filter("tags", "containsAll", []string{"admin", "user"}), wantIDs: []string{bobID}},
		{name: "tags contain any", filter: filter("tags", "containsAny", []string{"ops"}), wantIDs: []string{charlieID}},
		{name: "activity at", filter: filter("activity", "at", fixture.at(10, 10)), wantIDs: []string{aliceID}},
		{name: "activity overlaps", filter: filter("activity", "overlaps", interval(fixture.at(10, 5), fixture.at(10, 12))), wantIDs: []string{aliceID}},
		{name: "login time equals", filter: filter("loginTime", "eq", fixture.at(10, 15)), wantIDs: []string{bobID}},
		{name: "login time greater than", filter: filter("loginTime", "gt", fixture.at(10, 15)), wantIDs: []string{charlieID}},
		{name: "login time greater than or equal", filter: filter("loginTime", "gte", fixture.at(10, 15)), wantIDs: []string{bobID, charlieID}},
		{name: "login time less than", filter: filter("loginTime", "lt", fixture.at(10, 15)), wantIDs: []string{aliceID}},
		{name: "login time less than or equal", filter: filter("loginTime", "lte", fixture.at(10, 15)), wantIDs: []string{aliceID, bobID}},
		{name: "login time between", filter: filter("loginTime", "between", interval(fixture.at(10, 10), fixture.at(10, 20))), wantIDs: []string{bobID}},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result, err := repo.Query(context.Background(), session.QuerySpec{
				Filters:     []session.Filter{test.filter},
				Page:        session.PageRequest{Limit: 50},
				EvaluatedAt: fixture.at(10, 45),
			})
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			if got := resultIDs(result); !reflect.DeepEqual(got, test.wantIDs) {
				t.Fatalf("Query() IDs = %v, want %v", got, test.wantIDs)
			}
		})
	}
}

func TestStateFiltersMustMatchTheSameHistoricalState(t *testing.T) {
	t.Parallel()

	repo, fixture := newFilterFixture(t)
	result, err := repo.Query(context.Background(), session.QuerySpec{
		Filters: []session.Filter{
			filter("sessionId", "eq", aliceID),
			filter("activity", "at", fixture.at(10, 15)),
			filter("tags", "containsAll", []string{"admin"}),
		},
		Page:        session.PageRequest{Limit: 50},
		EvaluatedAt: fixture.at(10, 45),
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(result.Sessions) != 0 {
		t.Fatalf("Query() returned states from different periods: %+v", result.Sessions)
	}
}

func TestCursorPreservesDefaultActivityTime(t *testing.T) {
	t.Parallel()

	repo := memory.New()
	t.Cleanup(func() { _ = repo.Close() })
	loginAt := mustTime(t, "2026-08-21T10:00:00Z")
	logoutAt := mustTime(t, "2026-08-21T10:30:00Z")
	for index, username := range []string{"alice", "bob"} {
		key := mustKey(t, "tenant-a", username, fmt.Sprintf("192.0.2.%d", index+10))
		id := fmt.Sprintf("00000000-0000-0000-0000-%012d", index+1)
		applyLogin(t, repo, key, id, loginAt, []string{"user"})
		applyLogout(t, repo, key, logoutAt)
	}

	first, err := repo.Query(context.Background(), session.QuerySpec{
		Page:        session.PageRequest{Limit: 1},
		EvaluatedAt: loginAt.Add(15 * time.Minute),
	})
	if err != nil {
		t.Fatalf("first Query() error = %v", err)
	}
	if len(first.Sessions) != 1 || first.NextCursor == "" {
		t.Fatalf("first page = %+v", first)
	}

	second, err := repo.Query(context.Background(), session.QuerySpec{
		Page:        session.PageRequest{Limit: 1, Cursor: first.NextCursor},
		EvaluatedAt: logoutAt.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("second Query() error = %v", err)
	}
	if len(second.Sessions) != 1 || second.NextCursor != "" {
		t.Fatalf("second page = %+v", second)
	}
	if first.Sessions[0].ID == second.Sessions[0].ID {
		t.Fatal("cursor returned the first-page session again")
	}
}

func TestQueryLoadsCompleteIsolatedHistory(t *testing.T) {
	t.Parallel()

	repo, fixture := newFilterFixture(t)
	result, err := repo.Query(context.Background(), session.QuerySpec{
		Filters: []session.Filter{
			filter("sessionId", "eq", aliceID),
			filter("activity", "overlaps", interval(fixture.at(10, 0), fixture.at(11, 0))),
		},
		Page:        session.PageRequest{Limit: 50},
		EvaluatedAt: fixture.at(12, 0),
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(result.Sessions) != 1 {
		t.Fatalf("Query() sessions = %+v", result.Sessions)
	}
	got := result.Sessions[0]
	if got.LogoutAt == nil || !got.LogoutAt.Equal(fixture.at(11, 0)) || len(got.States) != 2 {
		t.Fatalf("complete history = %+v", got)
	}
	if got.States[0].ValidTo == nil || !got.States[0].ValidTo.Equal(fixture.at(10, 30)) {
		t.Fatalf("first historical state = %+v", got.States[0])
	}
}

func TestConcurrentMutationAndQueries(t *testing.T) {
	repo := memory.New()
	t.Cleanup(func() { _ = repo.Close() })
	base := mustTime(t, "2026-08-21T10:00:00Z")
	keys := make([]session.SessionKey, 8)
	for index := range keys {
		keys[index] = mustKey(t, "tenant-a", fmt.Sprintf("user-%d", index), fmt.Sprintf("192.0.2.%d", index+1))
		applyLogin(t, repo, keys[index], fmt.Sprintf("session-%d", index), base, []string{"initial"})
	}

	var wait sync.WaitGroup
	errorsFound := make(chan error, len(keys)+4)
	for index, key := range keys {
		index, key := index, key
		wait.Add(1)
		go func() {
			defer wait.Done()
			err := repo.Mutate(context.Background(), key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
				return session.DecideUpdate(snapshot, session.UpdateCommand{
					EventID: updateEventID, Key: key, Tags: []string{fmt.Sprintf("updated-%d", index)}, Timestamp: base.Add(time.Minute),
				})
			})
			if err != nil {
				errorsFound <- err
			}
		}()
	}
	for range 4 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 25 {
				_, err := repo.Query(context.Background(), session.QuerySpec{EvaluatedAt: base.Add(time.Hour)})
				if err != nil {
					errorsFound <- err
					return
				}
			}
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("concurrent operation error = %v", err)
	}
}

func TestClosedRepositoryRejectsOperations(t *testing.T) {
	t.Parallel()

	repo := memory.New()
	if err := repo.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	key := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	if err := repo.Mutate(context.Background(), key, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
		return nil, nil
	}); !errors.Is(err, repository.ErrClosed) {
		t.Fatalf("Mutate() error = %v, want ErrClosed", err)
	}
	if _, err := repo.Query(context.Background(), session.QuerySpec{EvaluatedAt: time.Now()}); !errors.Is(err, repository.ErrClosed) {
		t.Fatalf("Query() error = %v, want ErrClosed", err)
	}
}

type filterFixture struct {
	repo repository.SessionRepository
	day  time.Time
}

func newFilterFixture(t *testing.T) (repository.SessionRepository, filterFixture) {
	t.Helper()
	repo := memory.New()
	t.Cleanup(func() { _ = repo.Close() })
	fixture := filterFixture{repo: repo, day: mustTime(t, "2026-08-21T00:00:00Z")}
	alice := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	bob := mustKey(t, "tenant-a", "bob", "192.0.2.10")
	charlie := mustKey(t, "tenant-b", "alice", "192.0.2.20")
	applyLogin(t, repo, alice, aliceID, fixture.at(10, 0), []string{"user"})
	applyUpdate(t, repo, alice, fixture.at(10, 30), []string{"admin"})
	applyLogout(t, repo, alice, fixture.at(11, 0))
	applyLogin(t, repo, bob, bobID, fixture.at(10, 15), []string{"admin", "user"})
	applyLogin(t, repo, charlie, charlieID, fixture.at(10, 20), []string{"ops"})
	return repo, fixture
}

func (f filterFixture) at(hour, minute int) time.Time {
	return f.day.Add(time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute)
}

func filter(field, operator string, value any) session.Filter {
	return session.Filter{Field: session.Field(field), Operator: session.Operator(operator), Value: value}
}

func interval(from, to time.Time) query.IntervalValue {
	return query.IntervalValue{From: &from, To: &to}
}

func resultIDs(result session.QueryResult) []string {
	ids := make([]string, len(result.Sessions))
	for index := range result.Sessions {
		ids[index] = result.Sessions[index].ID
	}
	sort.Strings(ids)
	return ids
}

func applyLogin(t *testing.T, repo repository.SessionRepository, key session.SessionKey, id string, at time.Time, tags []string) {
	t.Helper()
	if err := repo.Mutate(context.Background(), key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		return session.DecideLogin(snapshot, session.LoginCommand{EventID: loginEventID, SessionID: id, Key: key, Tags: tags, Timestamp: at})
	}); err != nil {
		t.Fatalf("login error = %v", err)
	}
}

func applyUpdate(t *testing.T, repo repository.SessionRepository, key session.SessionKey, at time.Time, tags []string) {
	t.Helper()
	if err := repo.Mutate(context.Background(), key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		return session.DecideUpdate(snapshot, session.UpdateCommand{EventID: updateEventID, Key: key, Tags: tags, Timestamp: at})
	}); err != nil {
		t.Fatalf("update error = %v", err)
	}
}

func applyLogout(t *testing.T, repo repository.SessionRepository, key session.SessionKey, at time.Time) {
	t.Helper()
	if err := repo.Mutate(context.Background(), key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		return session.DecideLogout(snapshot, session.LogoutCommand{EventID: logoutEventID, Key: key, Timestamp: at})
	}); err != nil {
		t.Fatalf("logout error = %v", err)
	}
}

func mustKey(t *testing.T, tenantID, username, ip string) session.SessionKey {
	t.Helper()
	key, err := session.NewSessionKey(tenantID, username, ip)
	if err != nil {
		t.Fatalf("NewSessionKey() error = %v", err)
	}
	return key
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("time.Parse() error = %v", err)
	}
	return parsed
}
