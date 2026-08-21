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
	_ "github.com/jackc/pgx/v5/stdlib"
)

const testSchema = "session_handler_migration_test"

func TestApplyCreatesSchemaAndIsRepeatable(t *testing.T) {
	db := testDatabase(t)
	resetDatabase(t, db)

	if err := migrations.Apply(context.Background(), db); err != nil {
		t.Fatalf("first Apply() error = %v", err)
	}
	if err := migrations.Apply(context.Background(), db); err != nil {
		t.Fatalf("second Apply() error = %v", err)
	}

	wantTables := []string{"schema_migrations", "session_states", "sessions"}
	if got := tableNames(t, db); !reflect.DeepEqual(got, wantTables) {
		t.Fatalf("table names = %v, want %v", got, wantTables)
	}
	var applied int
	if err := db.QueryRow(`SELECT count(*) FROM schema_migrations`).Scan(&applied); err != nil {
		t.Fatalf("count schema migrations: %v", err)
	}
	if applied != 1 {
		t.Fatalf("applied migration count = %d, want 1", applied)
	}
}

func TestApplyRollsBackAFailingMigration(t *testing.T) {
	db := testDatabase(t)
	resetDatabase(t, db)
	source := fstest.MapFS{
		"sql/001_broken.sql": &fstest.MapFile{Data: []byte(`
			CREATE TABLE should_rollback (id integer PRIMARY KEY);
			SELECT function_that_does_not_exist();
		`)},
	}

	if err := migrations.ApplyFS(context.Background(), db, source); err == nil {
		t.Fatal("ApplyFS() error = nil, want migration failure")
	}
	var relation sql.NullString
	if err := db.QueryRow(`SELECT to_regclass('should_rollback')::text`).Scan(&relation); err != nil {
		t.Fatalf("check rolled-back table: %v", err)
	}
	if relation.Valid {
		t.Fatalf("failed migration left table %q behind", relation.String)
	}
	if err := db.QueryRow(`SELECT to_regclass('schema_migrations')::text`).Scan(&relation); err != nil {
		t.Fatalf("check migration metadata rollback: %v", err)
	}
	if relation.Valid {
		t.Fatalf("failed migration left metadata table %q behind", relation.String)
	}
}

func TestSchemaIdentityAndActiveLifecycleConstraints(t *testing.T) {
	db := migratedDatabase(t)
	ctx := context.Background()
	loginAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)

	if _, err := db.ExecContext(ctx, `
		INSERT INTO sessions (id, tenant_id, username, ip, login_at)
		VALUES ('00000000-0000-0000-0000-000000000001', 'tenant-a', 'alice', '192.0.2.10', $1)
	`, loginAt); err != nil {
		t.Fatalf("insert first active session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO sessions (id, tenant_id, username, ip, login_at)
		VALUES ('00000000-0000-0000-0000-000000000002', 'tenant-a', 'alice', '192.0.2.10', $1)
	`, loginAt.Add(time.Hour)); err == nil {
		t.Fatal("second active lifecycle for one full session key was accepted")
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE sessions SET logout_at = $1
		WHERE id = '00000000-0000-0000-0000-000000000001'
	`, loginAt.Add(30*time.Minute)); err != nil {
		t.Fatalf("close first lifecycle: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO sessions (id, tenant_id, username, ip, login_at)
		VALUES ('00000000-0000-0000-0000-000000000002', 'tenant-a', 'alice', '192.0.2.10', $1)
	`, loginAt.Add(time.Hour)); err != nil {
		t.Fatalf("insert sequential lifecycle: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO sessions (id, tenant_id, username, ip, login_at)
		VALUES ('00000000-0000-0000-0000-000000000003', 'tenant-b', 'alice', '192.0.2.10', $1)
	`, loginAt); err != nil {
		t.Fatalf("same username in another tenant should be allowed: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO sessions (id, tenant_id, username, ip, login_at)
		VALUES ('00000000-0000-0000-0000-000000000004', 'tenant-a', 'bob', '192.0.2.10', $1)
	`, loginAt); err != nil {
		t.Fatalf("shared IP should be allowed: %v", err)
	}
}

func TestSchemaForeignKeysAndTemporalChecks(t *testing.T) {
	db := migratedDatabase(t)
	ctx := context.Background()
	loginAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO sessions (id, tenant_id, username, ip, login_at, logout_at)
		VALUES ('00000000-0000-0000-0000-000000000011', 'tenant-a', 'alice', '192.0.2.10', $1, $2)
	`, loginAt, loginAt.Add(-time.Second)); err == nil {
		t.Fatal("logout before login was accepted")
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO sessions (id, tenant_id, username, ip, login_at)
		VALUES ('00000000-0000-0000-0000-000000000012', 'tenant-a', 'alice', '192.0.2.10', $1)
	`, loginAt); err != nil {
		t.Fatalf("insert valid session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO session_states (session_id, tags, valid_from)
		VALUES ('00000000-0000-0000-0000-000000000099', ARRAY['user'], $1)
	`, loginAt); err == nil {
		t.Fatal("state without a session parent was accepted")
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO session_states (session_id, tags, valid_from, valid_to)
		VALUES ('00000000-0000-0000-0000-000000000012', ARRAY['user'], $1, $2)
	`, loginAt, loginAt.Add(-time.Second)); err == nil {
		t.Fatal("state ending before it starts was accepted")
	}
}

func TestSchemaGeneratedRangesAndIndexes(t *testing.T) {
	db := migratedDatabase(t)
	ctx := context.Background()
	loginAt := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO sessions (id, tenant_id, username, ip, login_at)
		VALUES ('00000000-0000-0000-0000-000000000020', 'tenant-a', 'alice', '192.0.2.10', $1)
	`, loginAt); err != nil {
		t.Fatalf("insert session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO session_states (session_id, tags, valid_from)
		VALUES ('00000000-0000-0000-0000-000000000020', ARRAY['admin', 'user'], $1)
	`, loginAt); err != nil {
		t.Fatalf("insert state: %v", err)
	}

	var lifecycleContains, activityContains bool
	if err := db.QueryRowContext(ctx, `
		SELECT lifecycle @> $1::timestamptz FROM sessions
		WHERE id = '00000000-0000-0000-0000-000000000020'
	`, loginAt.Add(time.Minute)).Scan(&lifecycleContains); err != nil {
		t.Fatalf("query generated lifecycle: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT activity @> $1::timestamptz FROM session_states
		WHERE session_id = '00000000-0000-0000-0000-000000000020'
	`, loginAt.Add(time.Minute)).Scan(&activityContains); err != nil {
		t.Fatalf("query generated activity: %v", err)
	}
	if !lifecycleContains || !activityContains {
		t.Fatalf("generated ranges contain point = lifecycle %t, activity %t", lifecycleContains, activityContains)
	}

	wantIndexes := []string{
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
	if got := indexNames(t, db); !reflect.DeepEqual(got, wantIndexes) {
		t.Fatalf("index names = %v, want %v", got, wantIndexes)
	}
}

func testDatabase(t *testing.T) *sql.DB {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	admin, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("connect to test PostgreSQL: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS `+testSchema); err != nil {
		t.Fatalf("create isolated test schema: %v", err)
	}
	if err := admin.Close(); err != nil {
		t.Fatalf("close administrative test connection: %v", err)
	}

	parsedURL, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	parameters := parsedURL.Query()
	parameters.Set("search_path", testSchema)
	parsedURL.RawQuery = parameters.Encode()
	db, err := sql.Open("pgx", parsedURL.String())
	if err != nil {
		t.Fatalf("open isolated test schema: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("connect to isolated test schema: %v", err)
	}
	return db
}

func migratedDatabase(t *testing.T) *sql.DB {
	t.Helper()
	db := testDatabase(t)
	resetDatabase(t, db)
	if err := migrations.Apply(context.Background(), db); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	return db
}

func resetDatabase(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`DROP SCHEMA IF EXISTS ` + testSchema + ` CASCADE; CREATE SCHEMA ` + testSchema); err != nil {
		t.Fatalf("reset isolated test schema: %v", err)
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
	values := stringColumn(t, db, `
		SELECT indexname FROM pg_indexes
		WHERE schemaname = current_schema()
	`)
	sort.Strings(values)
	return values
}

func stringColumn(t *testing.T, db *sql.DB, statement string) []string {
	t.Helper()
	rows, err := db.Query(statement)
	if err != nil {
		t.Fatalf("query string column: %v", err)
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("scan string column: %v", err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate string column: %v", err)
	}
	return values
}
