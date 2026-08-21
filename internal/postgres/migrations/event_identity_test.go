package migrations_test

import (
	"context"
	"testing"
	"time"
)

func TestEventIDMigrationAddsLatestIdentityToSessions(t *testing.T) {
	db := migratedDatabase(t)
	ctx := context.Background()
	var dataType, nullable string
	if err := db.QueryRowContext(ctx, `
		SELECT data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = 'sessions'
		  AND column_name = 'last_event_id'
	`).Scan(&dataType, &nullable); err != nil {
		t.Fatalf("query last_event_id column: %v", err)
	}
	if dataType != "uuid" || nullable != "YES" {
		t.Fatalf("last_event_id column = type %q nullable %q, want uuid YES", dataType, nullable)
	}

	eventID := "10000000-0000-4000-8000-000000000001"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO sessions (id, tenant_id, username, ip, login_at, last_event_id)
		VALUES ('00000000-0000-0000-0000-000000000001', 'tenant-a', 'alice', '192.0.2.10', $1, $2::uuid)
	`, time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC), eventID); err != nil {
		t.Fatalf("insert session event identity: %v", err)
	}
	var stored string
	if err := db.QueryRowContext(ctx, `SELECT last_event_id::text FROM sessions WHERE id = '00000000-0000-0000-0000-000000000001'`).Scan(&stored); err != nil {
		t.Fatalf("read session event identity: %v", err)
	}
	if stored != eventID {
		t.Fatalf("stored event ID = %q, want %q", stored, eventID)
	}
}
