// Package repositorytest holds the contract every SessionRepository adapter
// must satisfy, so the in-memory and PostgreSQL stores are held to one
// definition of correct behavior.
package repositorytest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

// Factory opens a repository for one contract case.
type Factory func(t *testing.T) repository.SessionRepository

// Case is one named contract requirement.
type Case struct {
	Name string
	Run  func(t *testing.T, factory Factory)
}

// Cases returns the complete contract.
func Cases() []Case {
	return []Case{
		{Name: "lifecycle snapshots", Run: testLifecycleSnapshots},
		{Name: "callback rollback and isolation", Run: testCallbackRollbackAndIsolation},
		{Name: "same key serialization", Run: testSameKeySerialization},
		{Name: "generic state scoped filters", Run: testGenericStateScopedFilters},
		{Name: "pagination and stable cursors", Run: testPaginationAndStableCursors},
		{Name: "query result isolation", Run: testQueryResultIsolation},
		EventIDCase(),
	}
}

// EventIDCase returns the stream-deduplication requirement on its own, for
// adapters that run it separately from the rest of the contract.
func EventIDCase() Case {
	return Case{Name: "event ID atomicity and deduplication", Run: testEventIDAtomicityAndDeduplication}
}

// Run executes the complete contract against the repositories factory builds.
// The cases share one test scope, so a failing requirement stops the rest. Each
// requirement is logged before it runs, and the log is printed on failure, so
// the last line names the requirement that broke.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	for _, contractCase := range Cases() {
		t.Logf("contract requirement: %s", contractCase.Name)
		contractCase.Run(t, factory)
	}
}

// A mutation reports the snapshot it saw, then advances the lifecycle. The
// repository must supply the state left by every committed event and nothing
// from rolled-back ones.
func testLifecycleSnapshots(t *testing.T, factory Factory) {
	t.Helper()
	repo, releaseRepo := newRepository(t, factory)
	defer releaseRepo()
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	loginAt, updateAt, logoutAt := sessiontest.At("10:00"), sessiontest.At("10:30"), sessiontest.At("12:00")
	sessionID := sessiontest.SessionID(1)

	if before := sessiontest.Snapshot(t, repo, key); before.LastEventAt != nil || before.Active != nil {
		t.Fatalf("snapshot before the first login = %+v, want an empty snapshot", before)
	}

	sessiontest.Login(t, repo, key, sessionID, loginAt, "user")
	assertActive(t, sessiontest.Snapshot(t, repo, key), sessionID, loginAt, loginAt, []string{"user"})

	sessiontest.Update(t, repo, key, updateAt, "admin", "user")
	afterUpdate := sessiontest.Snapshot(t, repo, key)
	assertActive(t, afterUpdate, sessionID, loginAt, updateAt, []string{"admin", "user"})
	if states := afterUpdate.Active.States; len(states) != 2 || states[0].ValidTo == nil || !states[0].ValidTo.Equal(updateAt) {
		t.Fatalf("states after update = %+v, want the first one closed at %v", states, updateAt)
	}

	sessiontest.Logout(t, repo, key, logoutAt)
	afterLogout := sessiontest.Snapshot(t, repo, key)
	if afterLogout.Active != nil {
		t.Fatalf("active session after logout = %+v, want none", afterLogout.Active)
	}
	if afterLogout.LastEventAt == nil || !afterLogout.LastEventAt.Equal(logoutAt) {
		t.Fatalf("LastEventAt after logout = %v, want %v", afterLogout.LastEventAt, logoutAt)
	}

	// The same key may open a second lifecycle once the first has ended.
	sessiontest.Login(t, repo, key, sessiontest.SessionID(2), logoutAt.Add(time.Hour), "user")
}

// A callback that fails must leave storage untouched, and the snapshot it was
// given must be a copy it cannot write through.
func testCallbackRollbackAndIsolation(t *testing.T, factory Factory) {
	t.Helper()
	repo, releaseRepo := newRepository(t, factory)
	defer releaseRepo()
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	sessiontest.Login(t, repo, key, sessiontest.SessionID(1), sessiontest.At("10:00"), "user")

	rollback := errors.New("rollback")
	err := repo.Mutate(context.Background(), key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		snapshot.Active.States[0].Tags[0] = "corrupted"
		return nil, rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("Mutate() error = %v, want %v", err, rollback)
	}

	stored := sessiontest.Snapshot(t, repo, key).Active.States[0].Tags
	if want := []string{"user"}; !reflect.DeepEqual(stored, want) {
		t.Fatalf("stored tags = %v, want %v; the callback wrote through its snapshot", stored, want)
	}
}

// Mutations on one key run one at a time, so same-key commands cannot overwrite
// each other.
func testSameKeySerialization(t *testing.T, factory Factory) {
	t.Helper()
	repo, releaseRepo := newRepository(t, factory)
	defer releaseRepo()
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	rollback := errors.New("do not commit")
	firstEntered, releaseFirst := make(chan struct{}), make(chan struct{})
	secondStarted, secondEntered := make(chan struct{}), make(chan struct{})
	results := make(chan error, 2)

	go func() {
		results <- repo.Mutate(context.Background(), key, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
			close(firstEntered)
			<-releaseFirst
			return nil, rollback
		})
	}()
	sessiontest.Await(t, firstEntered, "the first callback")

	go func() {
		close(secondStarted)
		results <- repo.Mutate(context.Background(), key, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
			close(secondEntered)
			return nil, rollback
		})
	}()
	sessiontest.Await(t, secondStarted, "the second mutation attempt")
	if !sessiontest.Blocked(secondEntered) {
		t.Fatal("the second same-key callback ran before the first mutation finished")
	}

	close(releaseFirst)
	sessiontest.Await(t, secondEntered, "the second callback after release")
	for range 2 {
		if err := <-results; !errors.Is(err, rollback) {
			t.Fatalf("Mutate() error = %v, want %v", err, rollback)
		}
	}
}

// Filters that apply to state must all be satisfied by one state, and filters
// that apply to the session combine with them.
func testGenericStateScopedFilters(t *testing.T, factory Factory) {
	t.Helper()
	repo, releaseRepo := newRepository(t, factory)
	defer releaseRepo()
	at := sessiontest.At("10:00")
	alice := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	bob := sessiontest.Key("tenant-a", "bob", "192.0.2.10")
	adminSession := sessiontest.SessionID(1)
	sessiontest.Login(t, repo, alice, adminSession, at, "admin", "user")
	sessiontest.Login(t, repo, bob, sessiontest.SessionID(2), at, "user")

	result := sessiontest.Query(t, repo, sessiontest.Spec(at.Add(time.Minute),
		sessiontest.Filter("tenantId", "eq", "tenant-a"),
		sessiontest.Filter("activity", "at", at.Add(time.Minute)),
		sessiontest.Filter("tags", "containsAll", []string{"admin"}),
	))

	if got := sessiontest.SessionIDs(result); !reflect.DeepEqual(got, []string{adminSession}) {
		t.Fatalf("matching sessions = %v, want only the admin session %q", got, adminSession)
	}
}

// Pages divide the result set without repeating or dropping a session, and the
// last page reports no further cursor.
func testPaginationAndStableCursors(t *testing.T, factory Factory) {
	t.Helper()
	repo, releaseRepo := newRepository(t, factory)
	defer releaseRepo()
	at := sessiontest.At("10:00")
	for index, username := range []string{"alice", "bob", "charlie"} {
		key := sessiontest.Key("tenant-a", username, "192.0.2.10")
		sessiontest.Login(t, repo, key, sessiontest.SessionID(index+1), at.Add(time.Duration(index)*time.Second), "user")
	}

	spec := session.QuerySpec{Page: session.PageRequest{Limit: 2}, EvaluatedAt: at.Add(time.Minute)}
	first := sessiontest.Query(t, repo, spec)
	if len(first.Sessions) != 2 || first.NextCursor == "" {
		t.Fatalf("first page = %d sessions, cursor %q; want 2 sessions and a cursor", len(first.Sessions), first.NextCursor)
	}

	spec.Page.Cursor = first.NextCursor
	second := sessiontest.Query(t, repo, spec)
	if len(second.Sessions) != 1 || second.NextCursor != "" {
		t.Fatalf("second page = %d sessions, cursor %q; want 1 session and no cursor", len(second.Sessions), second.NextCursor)
	}

	seen := append(sessiontest.SessionIDs(first), sessiontest.SessionIDs(second)...)
	if unique := distinct(seen); len(unique) != 3 {
		t.Fatalf("paged session IDs = %v, want three distinct sessions", seen)
	}
}

// Query results are copies, so a caller cannot write into stored state.
func testQueryResultIsolation(t *testing.T, factory Factory) {
	t.Helper()
	repo, releaseRepo := newRepository(t, factory)
	defer releaseRepo()
	at := sessiontest.At("10:00")
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	sessiontest.Login(t, repo, key, sessiontest.SessionID(1), at, "user")
	spec := sessiontest.Spec(at.Add(time.Minute))

	first := sessiontest.Query(t, repo, spec)
	first.Sessions[0].States[0].Tags[0] = "corrupted"

	second := sessiontest.Query(t, repo, spec)
	if want := []string{"user"}; !reflect.DeepEqual(second.Sessions[0].States[0].Tags, want) {
		t.Fatalf("stored tags = %v, want %v; the first result shared storage", second.Sessions[0].States[0].Tags, want)
	}
}

// The accepted event ID commits and rolls back together with the session it
// belongs to, and replaying it is a successful no-op.
func testEventIDAtomicityAndDeduplication(t *testing.T, factory Factory) {
	t.Helper()
	repo, releaseRepo := newRepository(t, factory)
	defer releaseRepo()
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	loginAt := sessiontest.At("10:00")
	updateAt, logoutAt := loginAt.Add(time.Hour), loginAt.Add(2*time.Hour)

	loginEvent := sessiontest.Login(t, repo, key, sessiontest.SessionID(1), loginAt, "user")
	if got := sessiontest.Snapshot(t, repo, key).LastEventID; got != loginEvent {
		t.Fatalf("LastEventID after login = %q, want %q", got, loginEvent)
	}
	updateEvent := sessiontest.Update(t, repo, key, updateAt, "admin")

	// A mutation that fails after deciding must not leave its event ID behind.
	rollback := errors.New("roll back event identity")
	err := repo.Mutate(context.Background(), key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		mutation, decisionError := session.DecideUpdate(snapshot, session.UpdateCommand{
			EventID: sessiontest.NextEventID(), Key: key, Tags: []string{"rolled-back"}, Timestamp: updateAt.Add(time.Minute),
		})
		if decisionError != nil {
			return nil, decisionError
		}
		return mutation, rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("rolled-back Mutate() error = %v, want %v", err, rollback)
	}
	assertEventIdentity(t, sessiontest.Snapshot(t, repo, key), updateEvent, 2)

	// Replaying the last accepted event succeeds and changes nothing, which is
	// what makes stream redelivery safe to acknowledge.
	sessiontest.Mutate(t, repo, key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		return session.DecideUpdate(snapshot, session.UpdateCommand{
			EventID: updateEvent, Key: key, Tags: []string{"ignored"}, Timestamp: loginAt,
		})
	})
	assertEventIdentity(t, sessiontest.Snapshot(t, repo, key), updateEvent, 2)

	logoutEvent := sessiontest.Logout(t, repo, key, logoutAt)
	afterLogout := sessiontest.Snapshot(t, repo, key)
	if afterLogout.LastEventID != logoutEvent || afterLogout.Active != nil {
		t.Fatalf("snapshot after logout = %+v, want event %q and no active session", afterLogout, logoutEvent)
	}

	reloginSession := sessiontest.SessionID(2)
	reloginEvent := sessiontest.Login(t, repo, key, reloginSession, logoutAt.Add(time.Hour), "user")
	afterRelogin := sessiontest.Snapshot(t, repo, key)
	if afterRelogin.LastEventID != reloginEvent {
		t.Fatalf("LastEventID after relogin = %q, want %q", afterRelogin.LastEventID, reloginEvent)
	}
	if afterRelogin.Active == nil || afterRelogin.Active.LastEventID != reloginEvent {
		t.Fatalf("active session after relogin = %+v, want event %q", afterRelogin.Active, reloginEvent)
	}

	// The identity is durable, not only visible to mutations.
	result := sessiontest.Query(t, repo, sessiontest.Spec(logoutAt.Add(2*time.Hour),
		sessiontest.Filter("sessionId", "eq", reloginSession),
	))
	if len(result.Sessions) != 1 || result.Sessions[0].LastEventID != reloginEvent {
		t.Fatalf("queried sessions = %+v, want one carrying event %q", result.Sessions, reloginEvent)
	}
}

func assertActive(t *testing.T, snapshot session.CurrentSessionSnapshot, sessionID string, loginAt, lastEventAt time.Time, tags []string) {
	t.Helper()
	if snapshot.LastEventAt == nil || !snapshot.LastEventAt.Equal(lastEventAt) {
		t.Fatalf("LastEventAt = %v, want %v", snapshot.LastEventAt, lastEventAt)
	}
	if snapshot.Active == nil {
		t.Fatalf("no active session, want %q", sessionID)
	}
	if snapshot.Active.ID != sessionID || !snapshot.Active.LoginAt.Equal(loginAt) {
		t.Fatalf("active session %q logged in at %v, want %q at %v", snapshot.Active.ID, snapshot.Active.LoginAt, sessionID, loginAt)
	}
	current := snapshot.Active.States[len(snapshot.Active.States)-1]
	if !reflect.DeepEqual(current.Tags, tags) {
		t.Fatalf("current tags = %v, want %v", current.Tags, tags)
	}
}

func assertEventIdentity(t *testing.T, snapshot session.CurrentSessionSnapshot, eventID string, states int) {
	t.Helper()
	if snapshot.LastEventID != eventID {
		t.Fatalf("LastEventID = %q, want %q", snapshot.LastEventID, eventID)
	}
	if snapshot.Active == nil || len(snapshot.Active.States) != states {
		t.Fatalf("active session = %+v, want one holding %d states", snapshot.Active, states)
	}
}

// newRepository opens a store for one contract case. The caller must call the
// returned release function before the next case runs, because the cases share
// one test scope and an adapter may rebuild its storage on open.
func newRepository(t *testing.T, factory Factory) (repository.SessionRepository, func()) {
	t.Helper()
	if factory == nil {
		t.Fatal("repository contract factory is nil")
	}
	repo := factory(t)
	if repo == nil {
		t.Fatal("repository contract factory returned nil")
	}
	return repo, func() {
		if err := repo.Close(); err != nil && !errors.Is(err, repository.ErrClosed) {
			t.Errorf("Close() error = %v", err)
		}
	}
}

func distinct(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	return unique
}
