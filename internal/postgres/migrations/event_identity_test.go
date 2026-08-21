package migrations_test

import (
	"testing"

	"github.com/Bennybl/session-handler/internal/sessiontest"
)

// The second migration adds the column that carries the latest accepted event
// ID. It is nullable because a session created before the first event has none.
func TestEventIDMigrationAddsLatestIdentityToSessions(t *testing.T) {
	db := migratedDatabase(t)

	var dataType, nullable string
	err := db.QueryRow(`
		SELECT data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = 'sessions'
		  AND column_name = 'last_event_id'
	`).Scan(&dataType, &nullable)
	if err != nil {
		t.Fatalf("describe the last_event_id column: %v", err)
	}
	if dataType != "uuid" || nullable != "YES" {
		t.Fatalf("last_event_id is %s and nullable %s, want a nullable uuid", dataType, nullable)
	}

	sessionID, eventID := sessiontest.SessionID(1), sessiontest.EventID(1)
	execute(t, db, `
		INSERT INTO sessions (id, tenant_id, username, ip, login_at, last_event_id)
		VALUES ($1::uuid, 'tenant-a', 'alice', '192.0.2.10', $2, $3::uuid)
	`, sessionID, sessiontest.At("10:00"), eventID)

	var stored string
	if err := db.QueryRow(`SELECT last_event_id::text FROM sessions WHERE id = $1::uuid`, sessionID).Scan(&stored); err != nil {
		t.Fatalf("read back the event identity: %v", err)
	}
	if stored != eventID {
		t.Fatalf("stored event ID = %q, want %q", stored, eventID)
	}
}
