package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

const migrationLockID int64 = 7_640_821_447_115_901

//go:embed sql/*.sql
var embeddedSQL embed.FS

func Apply(ctx context.Context, db *sql.DB) error {
	return ApplyFS(ctx, db, embeddedSQL)
}

func ApplyFS(ctx context.Context, db *sql.DB, source fs.FS) error {
	if ctx == nil {
		return fmt.Errorf("migration context is required")
	}
	if db == nil {
		return fmt.Errorf("migration database is required")
	}
	if source == nil {
		return fmt.Errorf("migration source is required")
	}

	files, err := migrationFiles(source)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version text PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("create migration ledger: %w", err)
	}

	for _, name := range files {
		var applied bool
		if err := tx.QueryRowContext(ctx, `
			SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)
		`, name).Scan(&applied); err != nil {
			return fmt.Errorf("check migration %s: %w", name, err)
		}
		if applied {
			continue
		}

		statement, err := fs.ReadFile(source, "sql/"+name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx, string(statement)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO schema_migrations (version) VALUES ($1)
		`, name); err != nil {
			return fmt.Errorf("record migration %s: %w", name, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}

func migrationFiles(source fs.FS) ([]string, error) {
	entries, err := fs.ReadDir(source, "sql")
	if err != nil {
		return nil, fmt.Errorf("read migration directory: %w", err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".sql") {
			continue
		}
		separator := strings.IndexByte(name, '_')
		if separator < 1 || !allDigits(name[:separator]) {
			return nil, fmt.Errorf("invalid migration filename %q", name)
		}
		files = append(files, name)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no migration files found")
	}
	sort.Strings(files)
	return files, nil
}

func allDigits(value string) bool {
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}
