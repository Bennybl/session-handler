package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/Bennybl/session-handler/internal/query"
	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/repository/repositorytest"
	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

func TestRepositoryContract(t *testing.T) {
	repositorytest.Run(t, func(t *testing.T) repository.SessionRepository {
		t.Helper()
		repo, err := Open(context.Background())
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		return repo
	})
}

func TestQuerySupportsEveryRegistryEntry(t *testing.T) {
	repo, err := Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = repo.Close() }()
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	sessionID := sessiontest.SessionID(101)
	loginAt, evaluatedAt := sessiontest.At("10:00"), sessiontest.At("10:30")
	sessiontest.Login(t, repo, key, sessionID, loginAt, "user")
	interval := func(from, to string) query.IntervalValue {
		return query.IntervalValue{From: sessiontest.Ptr(sessiontest.At(from)), To: sessiontest.Ptr(sessiontest.At(to))}
	}
	filters := []session.Filter{
		sessiontest.Filter("sessionId", "eq", sessionID), sessiontest.Filter("sessionId", "in", []string{sessiontest.SessionID(102), sessionID}),
		sessiontest.Filter("tenantId", "eq", "tenant-a"), sessiontest.Filter("tenantId", "in", []string{"tenant-b", "tenant-a"}),
		sessiontest.Filter("username", "eq", "alice"), sessiontest.Filter("username", "in", []string{"bob", "alice"}),
		sessiontest.Filter("ip", "eq", "192.0.2.10"), sessiontest.Filter("ip", "in", []string{"192.0.2.11", "192.0.2.10"}),
		sessiontest.Filter("tags", "containsAll", []string{"user"}), sessiontest.Filter("tags", "containsAny", []string{"missing", "user"}),
		sessiontest.Filter("activity", "at", evaluatedAt), sessiontest.Filter("activity", "overlaps", interval("10:01", "10:30")),
		sessiontest.Filter("loginTime", "eq", loginAt), sessiontest.Filter("loginTime", "gt", sessiontest.At("09:59")),
		sessiontest.Filter("loginTime", "gte", loginAt), sessiontest.Filter("loginTime", "lt", sessiontest.At("10:01")),
		sessiontest.Filter("loginTime", "lte", loginAt), sessiontest.Filter("loginTime", "between", interval("09:59", "10:01")),
	}
	for _, filter := range filters {
		result := sessiontest.Query(t, repo, sessiontest.Spec(evaluatedAt, filter))
		if len(result.Sessions) != 1 || result.Sessions[0].ID != sessionID {
			t.Errorf("%s.%s returned %+v", filter.Field, filter.Operator, result.Sessions)
		}
	}
	interleaved := sessiontest.Query(t, repo, sessiontest.Spec(evaluatedAt,
		sessiontest.Filter("tags", "containsAny", []string{"user"}),
		sessiontest.Filter("tenantId", "eq", "tenant-a"),
		sessiontest.Filter("activity", "at", evaluatedAt),
		sessiontest.Filter("username", "eq", "alice"),
	))
	if len(interleaved.Sessions) != 1 {
		t.Fatalf("interleaved session/state filters returned %+v", interleaved.Sessions)
	}
	updateAt := sessiontest.At("11:00")
	sessiontest.Update(t, repo, key, updateAt, "admin")
	mismatched := sessiontest.Query(t, repo, sessiontest.Spec(sessiontest.At("11:01"), sessiontest.Filter("activity", "at", evaluatedAt), sessiontest.Filter("tags", "containsAll", []string{"admin"})))
	if len(mismatched.Sessions) != 0 {
		t.Fatalf("state filters matched different rows: %+v", mismatched.Sessions)
	}
	if _, err := repo.Query(context.Background(), sessiontest.Spec(evaluatedAt, sessiontest.Filter("tenantId) OR 1=1 --", "eq", "tenant-a"))); !errors.Is(err, repository.ErrInvalidQuery) {
		t.Fatalf("injected field error = %v", err)
	}
}

func TestSchemaRejectsDuplicateActiveSessionAndOpenState(t *testing.T) {
	repo, err := Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = repo.Close() }()
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	at := sessiontest.At("10:00")
	if _, err := repo.db.Exec(`INSERT INTO sessions (id, tenant_id, username, ip, login_at_ns, last_event_id) VALUES (?, ?, ?, ?, ?, ?)`, sessiontest.SessionID(1), key.TenantID, key.Username, key.IP, toNanos(at), sessiontest.EventID(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.db.Exec(`INSERT INTO sessions (id, tenant_id, username, ip, login_at_ns, last_event_id) VALUES (?, ?, ?, ?, ?, ?)`, sessiontest.SessionID(2), key.TenantID, key.Username, key.IP, toNanos(at), sessiontest.EventID(2)); err == nil {
		t.Fatal("duplicate active session was accepted")
	}
	if _, err := repo.db.Exec(`INSERT INTO session_states (session_id, valid_from_ns) VALUES (?, ?)`, sessiontest.SessionID(1), toNanos(at)); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.db.Exec(`INSERT INTO session_states (session_id, valid_from_ns) VALUES (?, ?)`, sessiontest.SessionID(1), toNanos(at)); err == nil {
		t.Fatal("duplicate open state was accepted")
	}
}
