package session

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestSessionIdentityAndKeyUniqueness(t *testing.T) {
	t.Parallel()

	aliceIP1, err := NewSessionKey("tenant-a", "alice", "192.0.2.10")
	if err != nil {
		t.Fatalf("NewSessionKey() error = %v", err)
	}
	aliceIP2, err := NewSessionKey("tenant-a", "alice", "192.0.2.11")
	if err != nil {
		t.Fatalf("NewSessionKey() error = %v", err)
	}
	bobSameIP, err := NewSessionKey("tenant-a", "bob", "192.0.2.10")
	if err != nil {
		t.Fatalf("NewSessionKey() error = %v", err)
	}
	aliceOtherTenant, err := NewSessionKey("tenant-b", "alice", "192.0.2.10")
	if err != nil {
		t.Fatalf("NewSessionKey() error = %v", err)
	}

	if aliceIP1.User() != aliceIP2.User() {
		t.Fatal("the same tenant and username must identify the same user")
	}
	if aliceIP1 == aliceIP2 {
		t.Fatal("different IPs must produce different session keys")
	}
	if aliceIP1 == bobSameIP {
		t.Fatal("an IP must not uniquely identify a user or session")
	}
	if aliceIP1.User() == aliceOtherTenant.User() {
		t.Fatal("the same username in different tenants must identify different users")
	}
}

func TestNewSessionKeyValidatesAndCanonicalizesIP(t *testing.T) {
	t.Parallel()

	key, err := NewSessionKey("tenant-a", "alice", "2001:0db8:0:0:0:0:0:1")
	if err != nil {
		t.Fatalf("NewSessionKey() error = %v", err)
	}
	if key.IP != "2001:db8::1" {
		t.Fatalf("IP = %q, want canonical IPv6", key.IP)
	}

	tests := []struct {
		name     string
		tenantID string
		username string
		ip       string
	}{
		{name: "empty tenant", username: "alice", ip: "192.0.2.1"},
		{name: "empty username", tenantID: "tenant-a", ip: "192.0.2.1"},
		{name: "invalid IP", tenantID: "tenant-a", username: "alice", ip: "not-an-ip"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewSessionKey(tt.tenantID, tt.username, tt.ip); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestNormalizeTagsUsesSetSemantics(t *testing.T) {
	t.Parallel()

	got, err := NormalizeTags([]string{"user", "admin", "user"})
	if err != nil {
		t.Fatalf("NormalizeTags() error = %v", err)
	}
	want := []string{"admin", "user"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NormalizeTags() = %v, want %v", got, want)
	}

	if _, err := NormalizeTags([]string{"valid", ""}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty tag error = %v, want ErrInvalidInput", err)
	}
}

func TestDecideLifecycle(t *testing.T) {
	t.Parallel()

	key := mustSessionKey(t, "tenant-a", "alice", "192.0.2.10")
	loginAt := mustTime(t, "2026-08-21T10:00:00Z")
	updateAt := mustTime(t, "2026-08-21T10:30:00Z")
	logoutAt := mustTime(t, "2026-08-21T12:00:00Z")

	login, err := DecideLogin(CurrentSessionSnapshot{}, LoginCommand{
		SessionID: "session-1",
		Key:       key,
		Tags:      []string{"user"},
		Timestamp: loginAt,
	})
	if err != nil {
		t.Fatalf("login Decide() error = %v", err)
	}
	started, ok := login.(StartSession)
	if !ok {
		t.Fatalf("login mutation = %T, want StartSession", login)
	}
	if started.Session.ID != "session-1" || !started.Session.LoginAt.Equal(loginAt) {
		t.Fatalf("started session = %+v", started.Session)
	}
	if len(started.Session.States) != 1 || !started.Session.States[0].ValidFrom.Equal(loginAt) {
		t.Fatalf("initial states = %+v", started.Session.States)
	}

	loginSnapshot := CurrentSessionSnapshot{LastEventAt: timePointer(loginAt), Active: sessionPointer(started.Session)}
	update, err := DecideUpdate(loginSnapshot, UpdateCommand{
		Key:       key,
		Tags:      []string{"admin", "user"},
		Timestamp: updateAt,
	})
	if err != nil {
		t.Fatalf("update Decide() error = %v", err)
	}
	replaced, ok := update.(ReplaceState)
	if !ok {
		t.Fatalf("update mutation = %T, want ReplaceState", update)
	}
	if replaced.SessionID != "session-1" || !replaced.CloseCurrentAt.Equal(updateAt) {
		t.Fatalf("replace mutation = %+v", replaced)
	}
	if !reflect.DeepEqual(replaced.State.Tags, []string{"admin", "user"}) {
		t.Fatalf("replacement tags = %v", replaced.State.Tags)
	}

	activeAfterUpdate := started.Session
	activeAfterUpdate.States[0].ValidTo = timePointer(updateAt)
	activeAfterUpdate.States = append(activeAfterUpdate.States, replaced.State)
	updateSnapshot := CurrentSessionSnapshot{LastEventAt: timePointer(updateAt), Active: &activeAfterUpdate}
	logout, err := DecideLogout(updateSnapshot, LogoutCommand{
		Key:       key,
		Timestamp: logoutAt,
	})
	if err != nil {
		t.Fatalf("logout Decide() error = %v", err)
	}
	ended, ok := logout.(EndSession)
	if !ok {
		t.Fatalf("logout mutation = %T, want EndSession", logout)
	}
	if ended.SessionID != "session-1" || !ended.LogoutAt.Equal(logoutAt) || !ended.CloseCurrentAt.Equal(logoutAt) {
		t.Fatalf("end mutation = %+v", ended)
	}

	completed := activeAfterUpdate
	completed.States[1].ValidTo = timePointer(logoutAt)
	completed.LogoutAt = timePointer(logoutAt)
	relogin, err := DecideLogin(CurrentSessionSnapshot{LastEventAt: timePointer(logoutAt)}, LoginCommand{
		SessionID: "session-2",
		Key:       key,
		Tags:      []string{"user"},
		Timestamp: logoutAt.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("relogin Decide() error = %v", err)
	}
	restarted := relogin.(StartSession)
	if restarted.Session.ID == completed.ID {
		t.Fatal("relogin must create a distinct session ID")
	}
}

func TestDecideRejectsInvalidTransitionsWithoutMutatingSnapshot(t *testing.T) {
	t.Parallel()

	key := mustSessionKey(t, "tenant-a", "alice", "192.0.2.10")
	at := mustTime(t, "2026-08-21T10:00:00Z")
	active := Session{
		ID:      "session-1",
		Key:     key,
		LoginAt: at,
		States:  []SessionState{{Tags: []string{"user"}, ValidFrom: at}},
	}

	tests := []struct {
		name     string
		snapshot CurrentSessionSnapshot
		decide   func(CurrentSessionSnapshot) (Mutation, error)
	}{
		{
			name:     "login while active",
			snapshot: CurrentSessionSnapshot{Active: &active},
			decide: func(snapshot CurrentSessionSnapshot) (Mutation, error) {
				return DecideLogin(snapshot, LoginCommand{SessionID: "session-2", Key: key, Tags: []string{"user"}, Timestamp: at.Add(time.Minute)})
			},
		},
		{
			name: "update while inactive",
			decide: func(snapshot CurrentSessionSnapshot) (Mutation, error) {
				return DecideUpdate(snapshot, UpdateCommand{Key: key, Tags: []string{"admin"}, Timestamp: at})
			},
		},
		{
			name: "logout while inactive",
			decide: func(snapshot CurrentSessionSnapshot) (Mutation, error) {
				return DecideLogout(snapshot, LogoutCommand{Key: key, Timestamp: at})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := cloneSnapshotForTest(tt.snapshot)
			if _, err := tt.decide(tt.snapshot); !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("error = %v, want ErrInvalidTransition", err)
			}
			if !reflect.DeepEqual(tt.snapshot, before) {
				t.Fatalf("snapshot mutated: got %+v, want %+v", tt.snapshot, before)
			}
		})
	}
}

func TestDecideRejectsStaleAndAcceptsEqualTimestamp(t *testing.T) {
	t.Parallel()

	key := mustSessionKey(t, "tenant-a", "alice", "192.0.2.10")
	at := mustTime(t, "2026-08-21T10:00:00Z")
	active := Session{ID: "session-1", Key: key, LoginAt: at, States: []SessionState{{Tags: []string{"user"}, ValidFrom: at}}}
	snapshot := CurrentSessionSnapshot{LastEventAt: timePointer(at), Active: &active}

	if _, err := DecideUpdate(snapshot, UpdateCommand{Key: key, Tags: []string{"admin"}, Timestamp: at.Add(-time.Nanosecond)}); !errors.Is(err, ErrStaleEvent) {
		t.Fatalf("stale error = %v, want ErrStaleEvent", err)
	}
	if _, err := DecideUpdate(snapshot, UpdateCommand{Key: key, Tags: []string{"admin"}, Timestamp: at}); err != nil {
		t.Fatalf("equal timestamp error = %v", err)
	}
}

func mustSessionKey(t *testing.T, tenantID, username, ip string) SessionKey {
	t.Helper()
	key, err := NewSessionKey(tenantID, username, ip)
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

func timePointer(value time.Time) *time.Time {
	return &value
}

func sessionPointer(value Session) *Session {
	return &value
}

func cloneSnapshotForTest(snapshot CurrentSessionSnapshot) CurrentSessionSnapshot {
	clone := snapshot
	if snapshot.LastEventAt != nil {
		clone.LastEventAt = timePointer(*snapshot.LastEventAt)
	}
	if snapshot.Active != nil {
		active := *snapshot.Active
		active.States = append([]SessionState(nil), snapshot.Active.States...)
		for index := range active.States {
			active.States[index].Tags = append([]string(nil), active.States[index].Tags...)
			if active.States[index].ValidTo != nil {
				active.States[index].ValidTo = timePointer(*active.States[index].ValidTo)
			}
		}
		clone.Active = &active
	}
	return clone
}
