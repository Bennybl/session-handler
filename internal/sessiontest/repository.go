package sessiontest

import (
	"context"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
)

// DecideAndApply loads key, runs a domain decision, and persists its mutation.
func DecideAndApply(t *testing.T, repo repository.SessionRepository, key session.SessionKey, decide func(session.CurrentSessionSnapshot) (session.Mutation, error)) {
	t.Helper()
	snapshot, err := repo.LoadCurrent(context.Background(), key)
	if err != nil {
		t.Fatalf("LoadCurrent() error = %v", err)
	}
	mutation, err := decide(snapshot)
	if err != nil {
		t.Fatalf("domain decision error = %v", err)
	}
	if err := repo.ApplyMutation(context.Background(), key, mutation); err != nil {
		t.Fatalf("ApplyMutation() error = %v", err)
	}
}

// Login records an accepted login and returns the event ID it used.
func Login(t *testing.T, repo repository.SessionRepository, key session.SessionKey, sessionID string, at time.Time, tags ...string) string {
	t.Helper()
	eventID := NextEventID()
	DecideAndApply(t, repo, key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		return session.DecideLogin(snapshot, session.LoginCommand{
			EventID: eventID, SessionID: sessionID, Key: key, Tags: tags, Timestamp: at,
		})
	})
	return eventID
}

// Update replaces the current state of the active session and returns the event
// ID it used.
func Update(t *testing.T, repo repository.SessionRepository, key session.SessionKey, at time.Time, tags ...string) string {
	t.Helper()
	eventID := NextEventID()
	DecideAndApply(t, repo, key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		return session.DecideUpdate(snapshot, session.UpdateCommand{
			EventID: eventID, Key: key, Tags: tags, Timestamp: at,
		})
	})
	return eventID
}

// Logout ends the active session and returns the event ID it used.
func Logout(t *testing.T, repo repository.SessionRepository, key session.SessionKey, at time.Time) string {
	t.Helper()
	eventID := NextEventID()
	DecideAndApply(t, repo, key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		return session.DecideLogout(snapshot, session.LogoutCommand{EventID: eventID, Key: key, Timestamp: at})
	})
	return eventID
}

// Snapshot returns the repository's detached current snapshot for key.
func Snapshot(t *testing.T, repo repository.SessionRepository, key session.SessionKey) session.CurrentSessionSnapshot {
	t.Helper()
	got, err := repo.LoadCurrent(context.Background(), key)
	if err != nil {
		t.Fatalf("LoadCurrent() error = %v", err)
	}
	return got
}

// Query runs spec against repo and fails the test when it reports an error.
func Query(t *testing.T, repo repository.SessionRepository, spec session.QuerySpec) session.QueryResult {
	t.Helper()
	result, err := repo.Query(context.Background(), spec)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	return result
}

// SessionIDs returns the session identifiers of result in page order.
func SessionIDs(result session.QueryResult) []string {
	ids := make([]string, len(result.Sessions))
	for index, value := range result.Sessions {
		ids[index] = value.ID
	}
	return ids
}

// Await blocks until signal is closed, failing the test if that takes too long.
func Await(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

// Blocked reports whether signal is still open after a short grace period. Use
// it to assert that an operation has not yet been allowed to proceed.
func Blocked(signal <-chan struct{}) bool {
	select {
	case <-signal:
		return false
	case <-time.After(150 * time.Millisecond):
		return true
	}
}
