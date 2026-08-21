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
	"github.com/Bennybl/session-handler/internal/sessiontest"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const testSchema = "session_handler_repository_test"

var firstSessionID = sessiontest.SessionID(101)

func TestMutateContract(t *testing.T) {
	runContractCases(t, "lifecycle snapshots", "callback rollback and isolation", "same key serialization")
}

func TestEventIDContract(t *testing.T) {
	repositorytest.EventIDCase().Run(t, freshRepository)
}

// Two different keys hash to different advisory locks, so unrelated sessions
// mutate concurrently.
func TestMutateDifferentKeysDoNotShareAnAdvisoryLock(t *testing.T) {
	repo := freshRepository(t)
	rollback := errors.New("do not persist")
	firstEntered, releaseFirst := make(chan struct{}), make(chan struct{})
	secondEntered := make(chan struct{})
	results := make(chan error, 2)

	go func() {
		results <- repo.Mutate(context.Background(), sessiontest.Key("tenant-a", "alice", "192.0.2.10"),
			func(session.CurrentSessionSnapshot) (session.Mutation, error) {
				close(firstEntered)
				<-releaseFirst
				return nil, rollback
			})
	}()
	sessiontest.Await(t, firstEntered, "the first key's callback")

	go func() {
		results <- repo.Mutate(context.Background(), sessiontest.Key("tenant-a", "bob", "192.0.2.10"),
			func(session.CurrentSessionSnapshot) (session.Mutation, error) {
				close(secondEntered)
				return nil, rollback
			})
	}()
	sessiontest.Await(t, secondEntered, "the second key's callback while the first is held")
	close(releaseFirst)

	for range 2 {
		if err := <-results; !errors.Is(err, rollback) {
			t.Fatalf("Mutate() error = %v, want %v", err, rollback)
		}
	}
}

// The callback only runs once the transaction holds the existing session rows,
// so a concurrent writer cannot change them underneath it.
func TestMutateLocksExistingSessionRowsBeforeTheCallback(t *testing.T) {
	db := openTestDatabase(t, true)
	repo := openRepository(t, db)
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	sessiontest.Login(t, repo, key, firstSessionID, sessiontest.At("10:00"), "user")

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
		t.Fatalf("lock the session row: %v", err)
	}

	rollback := errors.New("inspection only")
	callbackEntered := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- repo.Mutate(context.Background(), key, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
			close(callbackEntered)
			return nil, rollback
		})
	}()
	if !sessiontest.Blocked(callbackEntered) {
		t.Fatal("the callback ran while another transaction held the session row")
	}

	if err := locker.Rollback(); err != nil {
		t.Fatalf("release the session row lock: %v", err)
	}
	sessiontest.Await(t, callbackEntered, "the callback after the row unlocks")
	if err := <-result; !errors.Is(err, rollback) {
		t.Fatalf("Mutate() error = %v, want %v", err, rollback)
	}
}

// A mutation waiting for a same-key lock gives up when its context ends, and
// never runs its callback.
func TestMutateHonorsCancellationWhileWaiting(t *testing.T) {
	repo := freshRepository(t)
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	rollback := errors.New("release lock")
	firstEntered, releaseFirst := make(chan struct{}), make(chan struct{})
	firstResult := make(chan error, 1)

	go func() {
		firstResult <- repo.Mutate(context.Background(), key, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
			close(firstEntered)
			<-releaseFirst
			return nil, rollback
		})
	}()
	sessiontest.Await(t, firstEntered, "the holding callback")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	waitingCallbackRan := false
	err := repo.Mutate(ctx, key, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
		waitingCallbackRan = true
		return nil, rollback
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("waiting Mutate() error = %v, want context.DeadlineExceeded", err)
	}
	if waitingCallbackRan {
		t.Error("the waiting mutation ran its callback after its context ended")
	}

	close(releaseFirst)
	if err := <-firstResult; !errors.Is(err, rollback) {
		t.Fatalf("holding Mutate() error = %v, want %v", err, rollback)
	}
}

func TestMutatePersistsAcrossRepositoryInstances(t *testing.T) {
	loginAt := sessiontest.At("10:00")
	first := openRepository(t, openTestDatabase(t, true))
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	sessiontest.Login(t, first, key, firstSessionID, loginAt, "user")
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	// A second repository over the same schema sees the committed lifecycle.
	second := openRepository(t, openTestDatabase(t, false))
	snapshot := sessiontest.Snapshot(t, second, key)
	if snapshot.LastEventAt == nil || !snapshot.LastEventAt.Equal(loginAt) {
		t.Errorf("LastEventAt = %v, want %v", snapshot.LastEventAt, loginAt)
	}
	if snapshot.Active == nil || snapshot.Active.ID != firstSessionID {
		t.Errorf("active session = %+v, want the persisted %q", snapshot.Active, firstSessionID)
	}
}

// When persisting the new state fails, the statements already issued in the
// same transaction are rolled back with it.
func TestMutateRollsBackWhenPersistenceFails(t *testing.T) {
	db := openTestDatabase(t, true)
	repo := openRepository(t, db)
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	loginAt := sessiontest.At("10:00")
	loginEvent := sessiontest.Login(t, repo, key, firstSessionID, loginAt, "user")
	rejectForcedStates(t, db)

	err := repo.Mutate(context.Background(), key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		return session.DecideUpdate(snapshot, session.UpdateCommand{
			EventID: sessiontest.NextEventID(), Key: key,
			Tags: []string{forcedFailureTag}, Timestamp: loginAt.Add(time.Hour),
		})
	})
	if err == nil {
		t.Fatal("Mutate() error = nil, want the forced persistence failure")
	}

	// Nothing from the failed update survived: not the closed state, not the
	// new state, and not the event identity.
	snapshot := sessiontest.Snapshot(t, repo, key)
	if snapshot.LastEventAt == nil || !snapshot.LastEventAt.Equal(loginAt) {
		t.Errorf("LastEventAt = %v, want the login time %v", snapshot.LastEventAt, loginAt)
	}
	if snapshot.LastEventID != loginEvent {
		t.Errorf("LastEventID = %q, want the login event %q", snapshot.LastEventID, loginEvent)
	}
	if snapshot.Active == nil || len(snapshot.Active.States) != 1 {
		t.Fatalf("active session = %+v, want one surviving state", snapshot.Active)
	}
	if state := snapshot.Active.States[0]; state.ValidTo != nil || !reflect.DeepEqual(state.Tags, []string{"user"}) {
		t.Errorf("surviving state = %+v, want the open login state tagged [user]", state)
	}
}

// The latest event time comes from the whole lifecycle history, including a
// state that was closed after the current one began.
func TestMutateSnapshotIncludesHistoricalStateEnd(t *testing.T) {
	db := openTestDatabase(t, true)
	repo := openRepository(t, db)
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	historicalEnd := sessiontest.At("12:00")
	execute(t, db, `
		INSERT INTO sessions (id, tenant_id, username, ip, login_at)
		VALUES ($1::uuid, $2, $3, $4::inet, $5)
	`, firstSessionID, key.TenantID, key.Username, key.IP, sessiontest.At("10:00"))
	execute(t, db, `
		INSERT INTO session_states (session_id, tags, valid_from, valid_to)
		VALUES ($1::uuid, ARRAY['user'], $2, $3)
	`, firstSessionID, sessiontest.At("10:00"), historicalEnd)
	execute(t, db, `
		INSERT INTO session_states (session_id, tags, valid_from)
		VALUES ($1::uuid, ARRAY['admin'], $2)
	`, firstSessionID, sessiontest.At("11:00"))

	got := sessiontest.Snapshot(t, repo, key).LastEventAt
	if got == nil || !got.Equal(historicalEnd) {
		t.Fatalf("LastEventAt = %v, want the historical valid_to %v", got, historicalEnd)
	}
}

// runContractCases runs the named subset of the shared adapter contract.
func runContractCases(t *testing.T, names ...string) {
	t.Helper()
	wanted := make(map[string]bool, len(names))
	for _, name := range names {
		wanted[name] = true
	}
	for _, contractCase := range repositorytest.Cases() {
		if !wanted[contractCase.Name] {
			continue
		}
		delete(wanted, contractCase.Name)
		t.Run(contractCase.Name, func(t *testing.T) {
			contractCase.Run(t, freshRepository)
		})
	}
	for name := range wanted {
		t.Fatalf("contract case %q is no longer defined", name)
	}
}

func freshRepository(t *testing.T) repository.SessionRepository {
	t.Helper()
	return openRepository(t, openTestDatabase(t, true))
}

func openRepository(t *testing.T, db *sql.DB) *postgresrepo.Repository {
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

// openTestDatabase connects to an isolated schema in the test database. Pass
// reset to drop everything and re-run the migrations first; pass false to reuse
// the schema an earlier connection left behind.
func openTestDatabase(t *testing.T, reset bool) *sql.DB {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	admin, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open the test database: %v", err)
	}
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("connect to the test database: %v", err)
	}
	statement := `CREATE SCHEMA IF NOT EXISTS ` + testSchema
	if reset {
		statement = `DROP SCHEMA IF EXISTS ` + testSchema + ` CASCADE; CREATE SCHEMA ` + testSchema
	}
	if _, err := admin.ExecContext(ctx, statement); err != nil {
		t.Fatalf("prepare the test schema: %v", err)
	}
	if err := admin.Close(); err != nil {
		t.Fatalf("close the administrative connection: %v", err)
	}

	db, err := sql.Open("pgx", schemaURL(t, databaseURL, testSchema))
	if err != nil {
		t.Fatalf("open the test schema: %v", err)
	}
	db.SetMaxOpenConns(10)
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("connect to the test schema: %v", err)
	}
	if reset {
		if err := migrations.Apply(ctx, db); err != nil {
			t.Fatalf("apply migrations to the test schema: %v", err)
		}
	}
	return db
}

// schemaURL points a connection string at one schema.
func schemaURL(t *testing.T, databaseURL, schema string) string {
	t.Helper()
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	parameters := parsed.Query()
	parameters.Set("search_path", schema)
	parsed.RawQuery = parameters.Encode()
	return parsed.String()
}

// forcedFailureTag is the tag rejectForcedStates makes unstorable.
const forcedFailureTag = "force-db-error"

// rejectForcedStates installs a trigger that fails any state insert carrying
// forcedFailureTag, so a test can make persistence fail after the decision.
func rejectForcedStates(t *testing.T, db *sql.DB) {
	t.Helper()
	execute(t, db, `
		CREATE FUNCTION reject_forced_state() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF '`+forcedFailureTag+`' = ANY(NEW.tags) THEN
				RAISE EXCEPTION 'forced state insert failure';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER reject_forced_state_insert
			BEFORE INSERT ON session_states
			FOR EACH ROW EXECUTE FUNCTION reject_forced_state();
	`)
}

func execute(t *testing.T, db *sql.DB, statement string, arguments ...any) {
	t.Helper()
	if _, err := db.Exec(statement, arguments...); err != nil {
		t.Fatalf("execute %.60q: %v", statement, err)
	}
}
