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

// Locking is per session key: unrelated keys proceed together, a key whose rows
// another transaction holds waits for them, and a waiting mutation gives up when
// its context ends without ever running its callback.
func TestMutateLockingAndCancellation(t *testing.T) {
	db := openTestDatabase(t, true)
	repo := openRepository(t, db)
	alice := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	bob := sessiontest.Key("tenant-a", "bob", "192.0.2.10")
	rollback := errors.New("do not persist")

	// Different keys hash to different advisory locks.
	firstEntered, releaseFirst := make(chan struct{}), make(chan struct{})
	otherEntered := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		results <- repo.Mutate(context.Background(), alice, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
			close(firstEntered)
			<-releaseFirst
			return nil, rollback
		})
	}()
	sessiontest.Await(t, firstEntered, "the first key's callback")
	go func() {
		results <- repo.Mutate(context.Background(), bob, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
			close(otherEntered)
			return nil, rollback
		})
	}()
	sessiontest.Await(t, otherEntered, "the second key's callback while the first is held")

	// A mutation on the held key waits, and its context deadline releases it.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	waitingCallbackRan := false
	err := repo.Mutate(ctx, alice, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
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
	for range 2 {
		if err := <-results; !errors.Is(err, rollback) {
			t.Errorf("Mutate() error = %v, want %v", err, rollback)
		}
	}

	// An existing session row locked by another transaction holds the callback
	// back until that transaction releases it.
	sessiontest.Login(t, repo, alice, firstSessionID, sessiontest.At("10:00"), "user")
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
	`, alice.TenantID, alice.Username, alice.IP).Scan(&lockedID); err != nil {
		t.Fatalf("lock the session row: %v", err)
	}

	callbackEntered := make(chan struct{})
	lockedResult := make(chan error, 1)
	go func() {
		lockedResult <- repo.Mutate(context.Background(), alice, func(session.CurrentSessionSnapshot) (session.Mutation, error) {
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
	if err := <-lockedResult; !errors.Is(err, rollback) {
		t.Errorf("Mutate() error = %v, want %v", err, rollback)
	}
}

// Committed data outlives the repository that wrote it, a failed write rolls
// back everything the same transaction had already done, and the snapshot's
// latest event time comes from the whole lifecycle history.
func TestMutatePersistenceRollbackAndSnapshot(t *testing.T) {
	loginAt := sessiontest.At("10:00")
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")

	first := openRepository(t, openTestDatabase(t, true))
	loginEvent := sessiontest.Login(t, first, key, firstSessionID, loginAt, "user")
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	db := openTestDatabase(t, false)
	repo := openRepository(t, db)
	persisted := sessiontest.Snapshot(t, repo, key)
	if persisted.LastEventAt == nil || !persisted.LastEventAt.Equal(loginAt) {
		t.Errorf("LastEventAt = %v, want %v", persisted.LastEventAt, loginAt)
	}
	if persisted.Active == nil || persisted.Active.ID != firstSessionID {
		t.Errorf("active session = %+v, want the persisted %q", persisted.Active, firstSessionID)
	}

	// Make the state insert fail, so the update fails after closing the old
	// state and writing the new event ID in the same transaction.
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

	rolledBack := sessiontest.Snapshot(t, repo, key)
	if rolledBack.LastEventAt == nil || !rolledBack.LastEventAt.Equal(loginAt) {
		t.Errorf("LastEventAt = %v, want the login time %v", rolledBack.LastEventAt, loginAt)
	}
	if rolledBack.LastEventID != loginEvent {
		t.Errorf("LastEventID = %q, want the login event %q", rolledBack.LastEventID, loginEvent)
	}
	if rolledBack.Active == nil || len(rolledBack.Active.States) != 1 {
		t.Fatalf("active session = %+v, want one surviving state", rolledBack.Active)
	}
	if state := rolledBack.Active.States[0]; state.ValidTo != nil || !reflect.DeepEqual(state.Tags, []string{"user"}) {
		t.Errorf("surviving state = %+v, want the open login state tagged [user]", state)
	}

	// A state closed after the current one began is still the latest event.
	historyDB := openTestDatabase(t, true)
	historyRepo := openRepository(t, historyDB)
	historicalEnd := sessiontest.At("12:00")
	execute(t, historyDB, `
		INSERT INTO sessions (id, tenant_id, username, ip, login_at)
		VALUES ($1::uuid, $2, $3, $4::inet, $5)
	`, firstSessionID, key.TenantID, key.Username, key.IP, sessiontest.At("10:00"))
	execute(t, historyDB, `
		INSERT INTO session_states (session_id, tags, valid_from, valid_to)
		VALUES ($1::uuid, ARRAY['user'], $2, $3)
	`, firstSessionID, sessiontest.At("10:00"), historicalEnd)
	execute(t, historyDB, `
		INSERT INTO session_states (session_id, tags, valid_from)
		VALUES ($1::uuid, ARRAY['admin'], $2)
	`, firstSessionID, sessiontest.At("11:00"))

	if got := sessiontest.Snapshot(t, historyRepo, key).LastEventAt; got == nil || !got.Equal(historicalEnd) {
		t.Errorf("LastEventAt = %v, want the historical valid_to %v", got, historicalEnd)
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
		t.Logf("contract requirement: %s", contractCase.Name)
		contractCase.Run(t, freshRepository)
	}
	for name := range wanted {
		t.Errorf("contract case %q is no longer defined", name)
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
