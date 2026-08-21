package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/postgres/migrations"
	"github.com/Bennybl/session-handler/internal/repository"
	postgresrepo "github.com/Bennybl/session-handler/internal/repository/postgres"
	"github.com/Bennybl/session-handler/internal/repository/repositorytest"
	"github.com/Bennybl/session-handler/internal/session"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	repositoryTestSchema = "session_handler_repository_test"
	firstSessionID       = "00000000-0000-0000-0000-000000000101"
)

func TestMutateContract(t *testing.T) {
	mutationCases := map[string]bool{
		"lifecycle snapshots":             true,
		"callback rollback and isolation": true,
		"same key serialization":          true,
	}
	for _, contractCase := range repositorytest.Cases() {
		contractCase := contractCase
		if !mutationCases[contractCase.Name] {
			continue
		}
		t.Run(contractCase.Name, func(t *testing.T) {
			contractCase.Run(t, func(t *testing.T) repository.SessionRepository {
				return newRepository(t, true)
			})
		})
	}
}

func TestMutateDifferentKeysDoNotShareAnAdvisoryLock(t *testing.T) {
	repo := newRepository(t, true)
	firstKey := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	secondKey := mustKey(t, "tenant-a", "bob", "192.0.2.10")
	callbackError := errors.New("do not persist")
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondEntered := make(chan struct{})
	results := make(chan error, 2)

	go func() {
		results <- repo.Mutate(context.Background(), firstKey, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
			close(firstEntered)
			<-releaseFirst
			return nil, callbackError
		})
	}()
	awaitSignal(t, firstEntered, "first-key callback")

	go func() {
		results <- repo.Mutate(context.Background(), secondKey, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
			close(secondEntered)
			return nil, callbackError
		})
	}()
	awaitSignal(t, secondEntered, "different-key callback")
	close(releaseFirst)

	for range 2 {
		if err := <-results; !errors.Is(err, callbackError) {
			t.Fatalf("Mutate() error = %v, want %v", err, callbackError)
		}
	}
}

func TestMutateLocksExistingSessionRowsBeforeTheCallback(t *testing.T) {
	db := openTestDatabase(t, true)
	repo := mustRepository(t, db)
	key := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	loginAt := mustTime(t, "2026-08-21T10:00:00Z")
	seedLogin(t, repo, key, firstSessionID, loginAt)

	locker, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx() error = %v", err)
	}
	t.Cleanup(func() { _ = locker.Rollback() })
	var lockedID string
	if err := locker.QueryRow(`
		SELECT id FROM sessions
		WHERE tenant_id = $1 AND username = $2 AND ip = $3::inet
		FOR UPDATE
	`, key.TenantID, key.Username, key.IP).Scan(&lockedID); err != nil {
		t.Fatalf("lock session row: %v", err)
	}

	callbackEntered := make(chan struct{})
	result := make(chan error, 1)
	callbackError := errors.New("inspection only")
	go func() {
		result <- repo.Mutate(context.Background(), key, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
			close(callbackEntered)
			return nil, callbackError
		})
	}()
	select {
	case <-callbackEntered:
		t.Fatal("callback entered before the existing session row lock was released")
	case <-time.After(150 * time.Millisecond):
	}
	if err := locker.Rollback(); err != nil {
		t.Fatalf("release session row lock: %v", err)
	}
	awaitSignal(t, callbackEntered, "callback after row unlock")
	if err := <-result; !errors.Is(err, callbackError) {
		t.Fatalf("Mutate() error = %v, want %v", err, callbackError)
	}
}

func TestMutateWaitingForTheSameKeyHonorsContextCancellation(t *testing.T) {
	repo := newRepository(t, true)
	key := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstResult := make(chan error, 1)
	callbackError := errors.New("release lock")

	go func() {
		firstResult <- repo.Mutate(context.Background(), key, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
			close(firstEntered)
			<-releaseFirst
			return nil, callbackError
		})
	}()
	awaitSignal(t, firstEntered, "first same-key callback")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	secondCallbackEntered := false
	err := repo.Mutate(ctx, key, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
		secondCallbackEntered = true
		return nil, callbackError
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting Mutate() error = %v, want context deadline exceeded", err)
	}
	if secondCallbackEntered {
		t.Fatal("waiting mutation invoked its callback after context cancellation")
	}
	close(releaseFirst)
	if err := <-firstResult; !errors.Is(err, callbackError) {
		t.Fatalf("first Mutate() error = %v, want %v", err, callbackError)
	}
}

func TestMutatePersistsAcrossRepositoryInstances(t *testing.T) {
	firstDB := openTestDatabase(t, true)
	first := mustRepository(t, firstDB)
	key := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	loginAt := mustTime(t, "2026-08-21T10:00:00Z")
	seedLogin(t, first, key, firstSessionID, loginAt)
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	secondDB := openTestDatabase(t, false)
	second := mustRepository(t, secondDB)
	t.Cleanup(func() { _ = second.Close() })
	inspectionComplete := errors.New("inspection complete")
	err := second.Mutate(context.Background(), key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		if snapshot.LastEventAt == nil || !snapshot.LastEventAt.Equal(loginAt) {
			t.Fatalf("LastEventAt = %v, want %v", snapshot.LastEventAt, loginAt)
		}
		if snapshot.Active == nil || snapshot.Active.ID != firstSessionID {
			t.Fatalf("Active = %+v, want persisted session %s", snapshot.Active, firstSessionID)
		}
		return nil, inspectionComplete
	})
	if !errors.Is(err, inspectionComplete) {
		t.Fatalf("inspection error = %v, want %v", err, inspectionComplete)
	}
}

func TestMutatePersistenceFailureRollsBackEarlierStatements(t *testing.T) {
	db := openTestDatabase(t, true)
	repo := mustRepository(t, db)
	key := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	loginAt := mustTime(t, "2026-08-21T10:00:00Z")
	seedLogin(t, repo, key, firstSessionID, loginAt)

	if _, err := db.Exec(`
		CREATE FUNCTION reject_forced_state() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF 'force-db-error' = ANY(NEW.tags) THEN
				RAISE EXCEPTION 'forced state insert failure';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER reject_forced_state_insert
			BEFORE INSERT ON session_states
			FOR EACH ROW EXECUTE FUNCTION reject_forced_state();
	`); err != nil {
		t.Fatalf("install failure trigger: %v", err)
	}

	updateAt := loginAt.Add(time.Hour)
	err := repo.Mutate(context.Background(), key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		return session.DecideUpdate(snapshot, session.UpdateCommand{
			Key: key, Tags: []string{"force-db-error"}, Timestamp: updateAt,
		})
	})
	if err == nil {
		t.Fatal("Mutate() error = nil, want forced persistence failure")
	}

	inspectionComplete := errors.New("inspection complete")
	err = repo.Mutate(context.Background(), key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		if snapshot.LastEventAt == nil || !snapshot.LastEventAt.Equal(loginAt) {
			t.Fatalf("LastEventAt after rollback = %v, want %v", snapshot.LastEventAt, loginAt)
		}
		if snapshot.Active == nil || len(snapshot.Active.States) != 1 {
			t.Fatalf("Active after rollback = %+v", snapshot.Active)
		}
		state := snapshot.Active.States[0]
		if state.ValidTo != nil || !reflect.DeepEqual(state.Tags, []string{"user"}) {
			t.Fatalf("state after rollback = %+v", state)
		}
		return nil, inspectionComplete
	})
	if !errors.Is(err, inspectionComplete) {
		t.Fatalf("inspection error = %v, want %v", err, inspectionComplete)
	}
}

func TestMutateSnapshotLastEventIncludesHistoricalStateEnd(t *testing.T) {
	db := openTestDatabase(t, true)
	repo := mustRepository(t, db)
	key := mustKey(t, "tenant-a", "alice", "192.0.2.10")
	loginAt := mustTime(t, "2026-08-21T10:00:00Z")
	currentFrom := mustTime(t, "2026-08-21T11:00:00Z")
	historicalEnd := mustTime(t, "2026-08-21T12:00:00Z")
	if _, err := db.Exec(`
		INSERT INTO sessions (id, tenant_id, username, ip, login_at)
		VALUES ($1::uuid, $2, $3, $4::inet, $5)
	`, firstSessionID, key.TenantID, key.Username, key.IP, loginAt); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO session_states (session_id, tags, valid_from, valid_to)
		VALUES ($1::uuid, ARRAY['user'], $2, $3)
	`, firstSessionID, loginAt, historicalEnd); err != nil {
		t.Fatalf("seed historical state: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO session_states (session_id, tags, valid_from)
		VALUES ($1::uuid, ARRAY['admin'], $2)
	`, firstSessionID, currentFrom); err != nil {
		t.Fatalf("seed current state: %v", err)
	}

	inspectionComplete := errors.New("inspection complete")
	var got *time.Time
	err := repo.Mutate(context.Background(), key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		if snapshot.LastEventAt != nil {
			value := *snapshot.LastEventAt
			got = &value
		}
		return nil, inspectionComplete
	})
	if !errors.Is(err, inspectionComplete) {
		t.Fatalf("inspection error = %v, want %v", err, inspectionComplete)
	}
	if got == nil || !got.Equal(historicalEnd) {
		t.Fatalf("LastEventAt = %v, want historical valid_to %v", got, historicalEnd)
	}
}

func newRepository(t *testing.T, reset bool) repository.SessionRepository {
	t.Helper()
	return mustRepository(t, openTestDatabase(t, reset))
}

func mustRepository(t *testing.T, db *sql.DB) *postgresrepo.Repository {
	t.Helper()
	repo, err := postgresrepo.New(db)
	if err != nil {
		t.Fatalf("postgres.New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := repo.Close(); err != nil && !errors.Is(err, repository.ErrClosed) {
			t.Errorf("Close() error = %v", err)
		}
	})
	return repo
}

func openTestDatabase(t *testing.T, reset bool) *sql.DB {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	admin, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open test PostgreSQL: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("connect to test PostgreSQL: %v", err)
	}
	if reset {
		if _, err := admin.ExecContext(ctx, `DROP SCHEMA IF EXISTS `+repositoryTestSchema+` CASCADE; CREATE SCHEMA `+repositoryTestSchema); err != nil {
			t.Fatalf("reset repository test schema: %v", err)
		}
	} else if _, err := admin.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS `+repositoryTestSchema); err != nil {
		t.Fatalf("create repository test schema: %v", err)
	}
	if err := admin.Close(); err != nil {
		t.Fatalf("close test administration connection: %v", err)
	}

	parsedURL, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	parameters := parsedURL.Query()
	parameters.Set("search_path", repositoryTestSchema)
	parsedURL.RawQuery = parameters.Encode()
	db, err := sql.Open("pgx", parsedURL.String())
	if err != nil {
		t.Fatalf("open repository test schema: %v", err)
	}
	db.SetMaxOpenConns(10)
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("connect to repository test schema: %v", err)
	}
	if reset {
		if err := migrations.Apply(ctx, db); err != nil {
			t.Fatalf("apply repository test migrations: %v", err)
		}
	}
	return db
}

func seedLogin(t *testing.T, repo repository.SessionRepository, key session.SessionKey, id string, at time.Time) {
	t.Helper()
	if err := repo.Mutate(context.Background(), key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		return session.DecideLogin(snapshot, session.LoginCommand{
			SessionID: id, Key: key, Tags: []string{"user"}, Timestamp: at,
		})
	}); err != nil {
		t.Fatalf("seed login: %v", err)
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

func awaitSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
