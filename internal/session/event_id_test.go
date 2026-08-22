package session_test

import (
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

func TestDuplicateTrustedEventPrecedesStaleAndTransitionChecks(t *testing.T) {
	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	at := sessiontest.At("10:00")
	eventID := sessiontest.EventID(1)
	mutation, err := session.DecideUpdate(session.CurrentSessionSnapshot{LastEventAt: &at, LastEventID: eventID}, session.UpdateCommand{EventID: eventID, Key: key, Timestamp: at.Add(-time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if mutation.Kind() != session.MutationDuplicateEvent {
		t.Fatalf("kind = %s", mutation.Kind())
	}
}
