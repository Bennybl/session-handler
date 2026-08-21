package migrations_test

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"reflect"
	"sort"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Bennybl/session-handler/internal/postgres/migrations"
	"github.com/Bennybl/session-handler/internal/sessiontest"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const testSchema = "session_handler_migration_test"

func TestApplyCreatesSchemaIsRepeatableAndRollsBackFailures(t *testing.T) {
	db := testDatabase(t)
	resetSchema(t, db)

	// Applying twice must leave the same schema and ledger behind, and a
	// migration that fails partway must leave nothing behind at all.
	for _, attempt := range []string{"first", "second"} {
		if err := migrations.Apply(context.Background(), db); err != nil {
			t.Fatalf("%s Apply() error = %v", attempt, err)
		}
	}

	want := []string{"schema_migrations", "session_states", "sessions"}
	if got := tableNames(t, db); !reflect.DeepEqual(got, want) {
		t.Errorf("tables = %v, want %v", got, want)
	}
	var applied int
	if err := db.QueryRow(`SELECT count(*) FROM schema_migrations`).Scan(&applied); err != nil {
		t.Fatalf("count applied migrations: %v", err)
	}
	if applied != 2 {
		t.Errorf("applied migrations = %d, want 2 recorded once each", applied)
	}

	resetSchema(t, db)
	broken := fstest.MapFS{
		"sql/001_broken.sql": &fstest.MapFile{Data: []byte(`
			CREATE TABLE should_rollback (id integer PRIMARY KEY);
			SELECT function_that_does_not_exist();
		`)},
	}

	if err := migrations.ApplyFS(context.Background(), db, broken); err == nil {
		t.Fatal("ApplyFS() error = nil, want the migration to fail")
	}

	for _, relation := range []string{"should_rollback", "schema_migrations"} {
		if name, exists := regclass(t, db, relation); exists {
			t.Errorf("the failed migration left %q behind", name)
		}
	}
}

// A username identifies one user per tenant, an IP identifies nobody on its own,
// and only one lifecycle per session key may be active at a time. Foreign keys
// and temporal checks hold, the generated ranges answer point queries, and every
// index the queries rely on exists.
func TestSchemaConstraintsRangesAndIndexes(t *testing.T) {
	db := migratedDatabase(t)
	loginAt := sessiontest.At("10:00")

	if err := insertSession(db, sessiontest.SessionID(1), "tenant-a", "alice", "192.0.2.10", loginAt); err != nil {
		t.Fatalf("insert the first active session: %v", err)
	}
	if err := insertSession(db, sessiontest.SessionID(2), "tenant-a", "alice", "192.0.2.10", loginAt.Add(time.Hour)); err == nil {
		t.Error("a second active lifecycle for one session key was accepted")
	}

	// After the first lifecycle closes, the same key may open another.
	execute(t, db, `UPDATE sessions SET logout_at = $1 WHERE id = $2::uuid`, sessiontest.At("10:30"), sessiontest.SessionID(1))
	if err := insertSession(db, sessiontest.SessionID(2), "tenant-a", "alice", "192.0.2.10", loginAt.Add(time.Hour)); err != nil {
		t.Errorf("a sequential lifecycle for one session key was rejected: %v", err)
	}

	if err := insertSession(db, sessiontest.SessionID(3), "tenant-b", "alice", "192.0.2.10", loginAt); err != nil {
		t.Errorf("the same username in another tenant was rejected: %v", err)
	}
	if err := insertSession(db, sessiontest.SessionID(4), "tenant-a", "bob", "192.0.2.10", loginAt); err != nil {
		t.Errorf("a shared IP was rejected: %v", err)
	}

	// Foreign keys and temporal checks, on their own key because one key may
	// hold only one active lifecycle at a time.
	sessionID := sessiontest.SessionID(12)
	if err := insertSessionWithLogout(db, sessiontest.SessionID(11), "dana", "192.0.2.30", loginAt, loginAt.Add(-time.Second)); err == nil {
		t.Error("a lifecycle ending before it started was accepted")
	}
	if err := insertSession(db, sessionID, "tenant-a", "dana", "192.0.2.30", loginAt); err != nil {
		t.Fatalf("insert a valid session: %v", err)
	}
	if err := insertState(db, sessiontest.SessionID(99), loginAt, nil); err == nil {
		t.Error("a state without a parent session was accepted")
	}
	if err := insertState(db, sessionID, loginAt, sessiontest.Ptr(loginAt.Add(-time.Second))); err == nil {
		t.Error("a state ending before it started was accepted")
	}

	// The generated ranges let PostgreSQL answer temporal queries directly.
	sessionID = sessiontest.SessionID(20)
	if err := insertSession(db, sessionID, "tenant-a", "erin", "192.0.2.40", loginAt); err != nil {
		t.Fatalf("insert a session: %v", err)
	}
	if err := insertState(db, sessionID, loginAt, nil); err != nil {
		t.Fatalf("insert a state: %v", err)
	}

	during := loginAt.Add(time.Minute)
	if !containsPoint(t, db, `SELECT lifecycle @> $2::timestamptz FROM sessions WHERE id = $1::uuid`, sessionID, during) {
		t.Errorf("the generated lifecycle range does not contain %v", during)
	}
	if !containsPoint(t, db, `SELECT activity @> $2::timestamptz FROM session_states WHERE session_id = $1::uuid`, sessionID, during) {
		t.Errorf("the generated activity range does not contain %v", during)
	}

	want := []string{
		"schema_migrations_pkey",
		"session_states_activity_gist_idx",
		"session_states_pkey",
		"session_states_session_order_idx",
		"session_states_tags_gin_idx",
		"sessions_lifecycle_gist_idx",
		"sessions_one_active_per_key_idx",
		"sessions_pkey",
		"sessions_sort_idx",
		"sessions_user_login_idx",
	}
	if got := indexNames(t, db); !reflect.DeepEqual(got, want) {
		t.Errorf("indexes = %v, want %v", got, want)
	}
}

func insertSession(db *sql.DB, id, tenantID, username, ip string, loginAt time.Time) error {
	_, err := db.Exec(`
		INSERT INTO sessions (id, tenant_id, username, ip, login_at)
		VALUES ($1::uuid, $2, $3, $4::inet, $5)
	`, id, tenantID, username, ip, loginAt)
	return err
}

func insertSessionWithLogout(db *sql.DB, id, username, ip string, loginAt, logoutAt time.Time) error {
	_, err := db.Exec(`
		INSERT INTO sessions (id, tenant_id, username, ip, login_at, logout_at)
		VALUES ($1::uuid, 'tenant-a', $2, $3::inet, $4, $5)
	`, id, username, ip, loginAt, logoutAt)
	return err
}

func insertState(db *sql.DB, sessionID string, validFrom time.Time, validTo *time.Time) error {
	_, err := db.Exec(`
		INSERT INTO session_states (session_id, tags, valid_from, valid_to)
		VALUES ($1::uuid, ARRAY['user'], $2, $3)
	`, sessionID, validFrom, validTo)
	return err
}

func containsPoint(t *testing.T, db *sql.DB, statement, sessionID string, at time.Time) bool {
	t.Helper()
	var contains bool
	if err := db.QueryRow(statement, sessionID, at).Scan(&contains); err != nil {
		t.Fatalf("query a generated range: %v", err)
	}
	return contains
}

// regclass reports whether a relation exists in the test schema.
func regclass(t *testing.T, db *sql.DB, relation string) (string, bool) {
	t.Helper()
	var name sql.NullString
	if err := db.QueryRow(`SELECT to_regclass($1)::text`, relation).Scan(&name); err != nil {
		t.Fatalf("look up relation %q: %v", relation, err)
	}
	return name.String, name.Valid
}

func testDatabase(t *testing.T) *sql.DB {
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
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS `+testSchema); err != nil {
		t.Fatalf("create the test schema: %v", err)
	}
	if err := admin.Close(); err != nil {
		t.Fatalf("close the administrative connection: %v", err)
	}

	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	parameters := parsed.Query()
	parameters.Set("search_path", testSchema)
	parsed.RawQuery = parameters.Encode()
	db, err := sql.Open("pgx", parsed.String())
	if err != nil {
		t.Fatalf("open the test schema: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("connect to the test schema: %v", err)
	}
	return db
}

func migratedDatabase(t *testing.T) *sql.DB {
	t.Helper()
	db := testDatabase(t)
	resetSchema(t, db)
	if err := migrations.Apply(context.Background(), db); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	return db
}

func resetSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	execute(t, db, `DROP SCHEMA IF EXISTS `+testSchema+` CASCADE; CREATE SCHEMA `+testSchema)
}

func execute(t *testing.T, db *sql.DB, statement string, arguments ...any) {
	t.Helper()
	if _, err := db.Exec(statement, arguments...); err != nil {
		t.Fatalf("execute %.60q: %v", statement, err)
	}
}

func tableNames(t *testing.T, db *sql.DB) []string {
	t.Helper()
	return stringColumn(t, db, `
		SELECT tablename FROM pg_tables
		WHERE schemaname = current_schema()
		ORDER BY tablename
	`)
}

func indexNames(t *testing.T, db *sql.DB) []string {
	t.Helper()
	names := stringColumn(t, db, `
		SELECT indexname FROM pg_indexes
		WHERE schemaname = current_schema()
	`)
	sort.Strings(names)
	return names
}

func stringColumn(t *testing.T, db *sql.DB, statement string) []string {
	t.Helper()
	rows, err := db.Query(statement)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("scan: %v", err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate rows: %v", err)
	}
	return values
}
