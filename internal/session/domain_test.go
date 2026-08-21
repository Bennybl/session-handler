package session_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

func TestSessionIdentityAndKeyUniqueness(t *testing.T) {
	t.Parallel()

	alice := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	aliceOtherIP := sessiontest.Key("tenant-a", "alice", "192.0.2.11")
	bobSameIP := sessiontest.Key("tenant-a", "bob", "192.0.2.10")
	aliceOtherTenant := sessiontest.Key("tenant-b", "alice", "192.0.2.10")

	if alice.User() != aliceOtherIP.User() {
		t.Error("one tenant and username must identify one user across IPs")
	}
	if alice == aliceOtherIP {
		t.Error("different IPs must produce different session keys")
	}
	if alice == bobSameIP {
		t.Error("an IP must not identify a session on its own")
	}
	if alice.User() == aliceOtherTenant.User() {
		t.Error("one username in different tenants must identify different users")
	}
}

func TestNewSessionKeyValidatesAndCanonicalizesIP(t *testing.T) {
	t.Parallel()

	key, err := session.NewSessionKey("tenant-a", "alice", "2001:0db8:0:0:0:0:0:1")
	if err != nil {
		t.Fatalf("NewSessionKey() error = %v", err)
	}
	if key.IP != "2001:db8::1" {
		t.Errorf("IP = %q, want canonical 2001:db8::1", key.IP)
	}

	tests := []struct{ name, tenantID, username, ip string }{
		{name: "empty tenant", username: "alice", ip: "192.0.2.1"},
		{name: "empty username", tenantID: "tenant-a", ip: "192.0.2.1"},
		{name: "invalid IP", tenantID: "tenant-a", username: "alice", ip: "not-an-ip"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := session.NewSessionKey(test.tenantID, test.username, test.ip); !errors.Is(err, session.ErrInvalidInput) {
				t.Fatalf("error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestNormalizeTagsUsesSetSemantics(t *testing.T) {
	t.Parallel()

	got, err := session.NormalizeTags([]string{"user", "admin", "user"})
	if err != nil {
		t.Fatalf("NormalizeTags() error = %v", err)
	}
	if want := []string{"admin", "user"}; !reflect.DeepEqual(got, want) {
		t.Errorf("NormalizeTags() = %v, want %v", got, want)
	}
	if _, err := session.NormalizeTags([]string{"valid", ""}); !errors.Is(err, session.ErrInvalidInput) {
		t.Errorf("empty tag error = %v, want ErrInvalidInput", err)
	}
}

func TestDecideLifecycle(t *testing.T) {
	t.Parallel()

	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	loginAt, updateAt, logoutAt := sessiontest.At("10:00"), sessiontest.At("10:30"), sessiontest.At("12:00")
	sessionID := sessiontest.SessionID(1)
	loginEvent, updateEvent := sessiontest.EventID(1), sessiontest.EventID(2)
	logoutEvent, reloginEvent := sessiontest.EventID(3), sessiontest.EventID(4)

	// Login on an empty snapshot opens a lifecycle holding one open state.
	login, err := session.DecideLogin(session.CurrentSessionSnapshot{}, session.LoginCommand{
		EventID: loginEvent, SessionID: sessionID, Key: key, Tags: []string{"user"}, Timestamp: loginAt,
	})
	started := decided[session.StartSession](t, login, err)
	if started.Session.ID != sessionID {
		t.Errorf("started session ID = %q, want %q", started.Session.ID, sessionID)
	}
	if !started.Session.LoginAt.Equal(loginAt) {
		t.Errorf("LoginAt = %v, want %v", started.Session.LoginAt, loginAt)
	}
	if len(started.Session.States) != 1 || !started.Session.States[0].ValidFrom.Equal(loginAt) {
		t.Errorf("initial states = %+v, want one state valid from %v", started.Session.States, loginAt)
	}

	// Update closes the current state and supplies its replacement.
	afterLogin := session.CurrentSessionSnapshot{
		LastEventAt: sessiontest.Ptr(loginAt), LastEventID: loginEvent, Active: &started.Session,
	}
	update, err := session.DecideUpdate(afterLogin, session.UpdateCommand{
		EventID: updateEvent, Key: key, Tags: []string{"admin", "user"}, Timestamp: updateAt,
	})
	replaced := decided[session.ReplaceState](t, update, err)
	if replaced.SessionID != sessionID || replaced.EventID != updateEvent {
		t.Errorf("replacement identity = session %q event %q, want %q and %q", replaced.SessionID, replaced.EventID, sessionID, updateEvent)
	}
	if !replaced.CloseCurrentAt.Equal(updateAt) {
		t.Errorf("CloseCurrentAt = %v, want %v", replaced.CloseCurrentAt, updateAt)
	}
	if want := []string{"admin", "user"}; !reflect.DeepEqual(replaced.State.Tags, want) {
		t.Errorf("replacement tags = %v, want %v", replaced.State.Tags, want)
	}

	// Logout closes the current state and the lifecycle at the same instant.
	active := started.Session
	active.LastEventID = updateEvent
	active.States = []session.SessionState{
		{Tags: []string{"user"}, ValidFrom: loginAt, ValidTo: sessiontest.Ptr(updateAt)},
		replaced.State,
	}
	afterUpdate := session.CurrentSessionSnapshot{
		LastEventAt: sessiontest.Ptr(updateAt), LastEventID: updateEvent, Active: &active,
	}
	logout, err := session.DecideLogout(afterUpdate, session.LogoutCommand{
		EventID: logoutEvent, Key: key, Timestamp: logoutAt,
	})
	ended := decided[session.EndSession](t, logout, err)
	if ended.SessionID != sessionID || ended.EventID != logoutEvent {
		t.Errorf("end identity = session %q event %q, want %q and %q", ended.SessionID, ended.EventID, sessionID, logoutEvent)
	}
	if !ended.LogoutAt.Equal(logoutAt) || !ended.CloseCurrentAt.Equal(logoutAt) {
		t.Errorf("end times = logout %v, close %v, want both %v", ended.LogoutAt, ended.CloseCurrentAt, logoutAt)
	}

	// Relogin after logout starts a lifecycle with its own identity.
	afterLogout := session.CurrentSessionSnapshot{LastEventAt: sessiontest.Ptr(logoutAt), LastEventID: logoutEvent}
	relogin, err := session.DecideLogin(afterLogout, session.LoginCommand{
		EventID: reloginEvent, SessionID: sessiontest.SessionID(2), Key: key, Tags: []string{"user"}, Timestamp: logoutAt.Add(time.Hour),
	})
	restarted := decided[session.StartSession](t, relogin, err)
	if restarted.Session.ID == sessionID {
		t.Errorf("relogin reused session ID %q, want a distinct lifecycle", sessionID)
	}
}

func TestDecideRejectsInvalidTransitionsWithoutMutatingSnapshot(t *testing.T) {
	t.Parallel()

	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	at := sessiontest.At("10:00")
	activeSnapshot := sessiontest.ActiveSession(key, sessiontest.SessionID(1), sessiontest.EventID(1), at, "user")

	tests := []struct {
		name     string
		snapshot session.CurrentSessionSnapshot
		decide   func(session.CurrentSessionSnapshot) (session.Mutation, error)
	}{
		{
			name:     "login while active",
			snapshot: activeSnapshot,
			decide: func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
				return session.DecideLogin(snapshot, session.LoginCommand{
					EventID: sessiontest.EventID(2), SessionID: sessiontest.SessionID(2), Key: key,
					Tags: []string{"user"}, Timestamp: at.Add(time.Minute),
				})
			},
		},
		{
			name: "update while inactive",
			decide: func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
				return session.DecideUpdate(snapshot, session.UpdateCommand{
					EventID: sessiontest.EventID(2), Key: key, Tags: []string{"admin"}, Timestamp: at,
				})
			},
		},
		{
			name: "logout while inactive",
			decide: func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
				return session.DecideLogout(snapshot, session.LogoutCommand{
					EventID: sessiontest.EventID(2), Key: key, Timestamp: at,
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := snapshotContents(t, test.snapshot)
			if _, err := test.decide(test.snapshot); !errors.Is(err, session.ErrInvalidTransition) {
				t.Fatalf("error = %v, want ErrInvalidTransition", err)
			}
			if after := snapshotContents(t, test.snapshot); after != before {
				t.Fatalf("snapshot mutated:\n got %s\nwant %s", after, before)
			}
		})
	}
}

func TestDecideRejectsStaleAndAcceptsEqualTimestamp(t *testing.T) {
	t.Parallel()

	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	at := sessiontest.At("10:00")
	snapshot := sessiontest.ActiveSession(key, sessiontest.SessionID(1), sessiontest.EventID(1), at, "user")
	update := func(timestamp time.Time) error {
		_, err := session.DecideUpdate(snapshot, session.UpdateCommand{
			EventID: sessiontest.EventID(2), Key: key, Tags: []string{"admin"}, Timestamp: timestamp,
		})
		return err
	}

	if err := update(at.Add(-time.Nanosecond)); !errors.Is(err, session.ErrStaleEvent) {
		t.Errorf("error just before the latest event = %v, want ErrStaleEvent", err)
	}
	if err := update(at); err != nil {
		t.Errorf("error at the latest event time = %v, want acceptance", err)
	}
}

// decided asserts that a decision succeeded and produced the wanted mutation.
func decided[Want session.Mutation](t *testing.T, mutation session.Mutation, err error) Want {
	t.Helper()
	if err != nil {
		t.Fatalf("Decide() error = %v", err)
	}
	got, ok := mutation.(Want)
	if !ok {
		t.Fatalf("mutation = %T, want %T", mutation, got)
	}
	return got
}

// snapshotContents renders a snapshot including the values behind its pointers,
// so a test can prove a decision left its input untouched.
func snapshotContents(t *testing.T, snapshot session.CurrentSessionSnapshot) string {
	t.Helper()
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	return string(encoded)
}
