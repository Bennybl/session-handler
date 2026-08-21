package repositorytest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
)

const (
	contractSession1ID = "00000000-0000-0000-0000-000000000001"
	contractSession2ID = "00000000-0000-0000-0000-000000000002"
	contractSession3ID = "00000000-0000-0000-0000-000000000003"
)

type Factory func(t *testing.T) repository.SessionRepository

type Case struct {
	Name string
	Run  func(t *testing.T, factory Factory)
}

func Cases() []Case {
	return []Case{
		{Name: "lifecycle snapshots", Run: testLifecycleSnapshots},
		{Name: "callback rollback and isolation", Run: testCallbackRollbackAndIsolation},
		{Name: "same key serialization", Run: testSameKeySerialization},
		{Name: "generic state scoped filters", Run: testGenericStateScopedFilters},
		{Name: "pagination and stable cursors", Run: testPaginationAndStableCursors},
		{Name: "query result isolation", Run: testQueryResultIsolation},
	}
}

func Run(t *testing.T, factory Factory) {
	t.Helper()
	for _, contractCase := range Cases() {
		contractCase := contractCase
		t.Run(contractCase.Name, func(t *testing.T) {
			contractCase.Run(t, factory)
		})
	}
}

func testLifecycleSnapshots(t *testing.T, factory Factory) {
	t.Helper()
	repo := newRepository(t, factory)
	ctx := context.Background()
	key := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	loginAt := mustTime(t, "2026-08-21T10:00:00Z")
	updateAt := mustTime(t, "2026-08-21T10:30:00Z")
	logoutAt := mustTime(t, "2026-08-21T12:00:00Z")

	err := repo.Mutate(ctx, key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		if snapshot.LastEventAt != nil || snapshot.Active != nil {
			t.Fatalf("initial snapshot = %+v, want empty", snapshot)
		}
		return session.DecideLogin(snapshot, session.LoginCommand{
			SessionID: contractSession1ID,
			Key:       key,
			Tags:      []string{"user"},
			Timestamp: loginAt,
		})
	})
	if err != nil {
		t.Fatalf("login mutation error = %v", err)
	}

	err = repo.Mutate(ctx, key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		assertSnapshot(t, snapshot, contractSession1ID, loginAt, loginAt, []string{"user"})
		return session.DecideUpdate(snapshot, session.UpdateCommand{
			Key:       key,
			Tags:      []string{"admin", "user"},
			Timestamp: updateAt,
		})
	})
	if err != nil {
		t.Fatalf("update mutation error = %v", err)
	}

	err = repo.Mutate(ctx, key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		assertSnapshot(t, snapshot, contractSession1ID, loginAt, updateAt, []string{"admin", "user"})
		if len(snapshot.Active.States) != 2 || snapshot.Active.States[0].ValidTo == nil || !snapshot.Active.States[0].ValidTo.Equal(updateAt) {
			t.Fatalf("states after update = %+v", snapshot.Active.States)
		}
		return session.DecideLogout(snapshot, session.LogoutCommand{Key: key, Timestamp: logoutAt})
	})
	if err != nil {
		t.Fatalf("logout mutation error = %v", err)
	}

	probeError := errors.New("snapshot inspected")
	err = repo.Mutate(ctx, key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		if snapshot.Active != nil {
			t.Fatalf("active session after logout = %+v", snapshot.Active)
		}
		if snapshot.LastEventAt == nil || !snapshot.LastEventAt.Equal(logoutAt) {
			t.Fatalf("LastEventAt = %v, want %v", snapshot.LastEventAt, logoutAt)
		}
		return nil, probeError
	})
	if !errors.Is(err, probeError) {
		t.Fatalf("probe error = %v, want %v", err, probeError)
	}

	err = repo.Mutate(ctx, key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		return session.DecideLogin(snapshot, session.LoginCommand{
			SessionID: contractSession2ID,
			Key:       key,
			Tags:      []string{"user"},
			Timestamp: logoutAt.Add(time.Hour),
		})
	})
	if err != nil {
		t.Fatalf("relogin mutation error = %v", err)
	}
}

func testCallbackRollbackAndIsolation(t *testing.T, factory Factory) {
	t.Helper()
	repo := newRepository(t, factory)
	ctx := context.Background()
	key := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	loginAt := mustTime(t, "2026-08-21T10:00:00Z")
	seedLogin(t, repo, key, contractSession1ID, loginAt, []string{"user"})

	rollbackError := errors.New("rollback")
	err := repo.Mutate(ctx, key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		snapshot.Active.States[0].Tags[0] = "corrupted"
		return nil, rollbackError
	})
	if !errors.Is(err, rollbackError) {
		t.Fatalf("rollback error = %v, want %v", err, rollbackError)
	}

	inspectionComplete := errors.New("inspection complete")
	err = repo.Mutate(ctx, key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		if got := snapshot.Active.States[0].Tags; !reflect.DeepEqual(got, []string{"user"}) {
			t.Fatalf("stored tags after callback mutation = %v", got)
		}
		return nil, inspectionComplete
	})
	if !errors.Is(err, inspectionComplete) {
		t.Fatalf("inspection error = %v, want %v", err, inspectionComplete)
	}
}

func testSameKeySerialization(t *testing.T, factory Factory) {
	t.Helper()
	repo := newRepository(t, factory)
	ctx := context.Background()
	key := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	callbackError := errors.New("do not commit")
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondAttempting := make(chan struct{})
	secondEntered := make(chan struct{})
	results := make(chan error, 2)

	go func() {
		results <- repo.Mutate(ctx, key, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
			close(firstEntered)
			<-releaseFirst
			return nil, callbackError
		})
	}()
	awaitSignal(t, firstEntered, "first callback")

	go func() {
		close(secondAttempting)
		results <- repo.Mutate(ctx, key, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
			close(secondEntered)
			return nil, callbackError
		})
	}()
	awaitSignal(t, secondAttempting, "second mutation attempt")

	select {
	case <-secondEntered:
		t.Fatal("second same-key callback ran before the first mutation completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseFirst)
	awaitSignal(t, secondEntered, "second callback after release")
	for range 2 {
		if err := <-results; !errors.Is(err, callbackError) {
			t.Fatalf("mutation error = %v, want %v", err, callbackError)
		}
	}
}

func testGenericStateScopedFilters(t *testing.T, factory Factory) {
	t.Helper()
	repo := newRepository(t, factory)
	ctx := context.Background()
	at := mustTime(t, "2026-08-21T10:00:00Z")
	alice := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	bob := mustKey(t, "tenant-a", "bob", "192.0.2.10")
	seedLogin(t, repo, alice, contractSession1ID, at, []string{"admin", "user"})
	seedLogin(t, repo, bob, contractSession2ID, at, []string{"user"})

	result, err := repo.Query(ctx, session.QuerySpec{
		Filters: []session.Filter{
			{Field: session.Field("tenantId"), Operator: session.Operator("eq"), Value: "tenant-a"},
			{Field: session.Field("activity"), Operator: session.Operator("at"), Value: at.Add(time.Minute)},
			{Field: session.Field("tags"), Operator: session.Operator("containsAll"), Value: []string{"admin"}},
		},
		Page:        session.PageRequest{Limit: 50},
		EvaluatedAt: at.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(result.Sessions) != 1 || result.Sessions[0].ID != contractSession1ID {
		t.Fatalf("Query() sessions = %+v, want %s", result.Sessions, contractSession1ID)
	}
}

func testPaginationAndStableCursors(t *testing.T, factory Factory) {
	t.Helper()
	repo := newRepository(t, factory)
	ctx := context.Background()
	at := mustTime(t, "2026-08-21T10:00:00Z")
	ids := []string{contractSession1ID, contractSession2ID, contractSession3ID}
	for index, username := range []string{"alice", "bob", "charlie"} {
		key := mustKey(t, "tenant-a", username, "192.0.2.10")
		seedLogin(t, repo, key, ids[index], at.Add(time.Duration(index)*time.Second), []string{"user"})
	}

	spec := session.QuerySpec{Page: session.PageRequest{Limit: 2}, EvaluatedAt: at.Add(time.Minute)}
	first, err := repo.Query(ctx, spec)
	if err != nil {
		t.Fatalf("first Query() error = %v", err)
	}
	if len(first.Sessions) != 2 || first.NextCursor == "" {
		t.Fatalf("first page = %+v", first)
	}

	spec.Page.Cursor = first.NextCursor
	second, err := repo.Query(ctx, spec)
	if err != nil {
		t.Fatalf("second Query() error = %v", err)
	}
	if len(second.Sessions) != 1 || second.NextCursor != "" {
		t.Fatalf("second page = %+v", second)
	}
	if first.Sessions[0].ID == second.Sessions[0].ID || first.Sessions[1].ID == second.Sessions[0].ID {
		t.Fatal("a session appeared on more than one page")
	}
}

func testQueryResultIsolation(t *testing.T, factory Factory) {
	t.Helper()
	repo := newRepository(t, factory)
	ctx := context.Background()
	at := mustTime(t, "2026-08-21T10:00:00Z")
	key := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	seedLogin(t, repo, key, contractSession1ID, at, []string{"user"})
	spec := session.QuerySpec{Page: session.PageRequest{Limit: 50}, EvaluatedAt: at.Add(time.Minute)}

	first, err := repo.Query(ctx, spec)
	if err != nil {
		t.Fatalf("first Query() error = %v", err)
	}
	first.Sessions[0].States[0].Tags[0] = "corrupted"

	second, err := repo.Query(ctx, spec)
	if err != nil {
		t.Fatalf("second Query() error = %v", err)
	}
	if got := second.Sessions[0].States[0].Tags; !reflect.DeepEqual(got, []string{"user"}) {
		t.Fatalf("stored query tags = %v, want [user]", got)
	}
}

func newRepository(t *testing.T, factory Factory) repository.SessionRepository {
	t.Helper()
	if factory == nil {
		t.Fatal("repository contract factory is nil")
	}
	repo := factory(t)
	if repo == nil {
		t.Fatal("repository contract factory returned nil")
	}
	t.Cleanup(func() {
		if err := repo.Close(); err != nil && !errors.Is(err, repository.ErrClosed) {
			t.Errorf("Close() error = %v", err)
		}
	})
	return repo
}

func seedLogin(t *testing.T, repo repository.SessionRepository, key session.SessionKey, id string, at time.Time, tags []string) {
	t.Helper()
	err := repo.Mutate(context.Background(), key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		return session.DecideLogin(snapshot, session.LoginCommand{SessionID: id, Key: key, Tags: tags, Timestamp: at})
	})
	if err != nil {
		t.Fatalf("seed login error = %v", err)
	}
}

func assertSnapshot(t *testing.T, snapshot session.CurrentSessionSnapshot, id string, loginAt, lastEventAt time.Time, currentTags []string) {
	t.Helper()
	if snapshot.LastEventAt == nil || !snapshot.LastEventAt.Equal(lastEventAt) {
		t.Fatalf("LastEventAt = %v, want %v", snapshot.LastEventAt, lastEventAt)
	}
	if snapshot.Active == nil || snapshot.Active.ID != id || !snapshot.Active.LoginAt.Equal(loginAt) {
		t.Fatalf("Active = %+v", snapshot.Active)
	}
	current := snapshot.Active.States[len(snapshot.Active.States)-1]
	if !reflect.DeepEqual(current.Tags, currentTags) {
		t.Fatalf("current tags = %v, want %v", current.Tags, currentTags)
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
		t.Fatalf("time.Parse(%q) error = %v", value, err)
	}
	return parsed
}

func awaitSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
