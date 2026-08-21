package session_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

// Every command carries a UUID event ID. It is required, validated, lowercased,
// and a repeat of the last accepted one is a no-op that outranks every other
// check, which is what makes stream redelivery safe.
func TestEventIDValidationNormalizationAndDuplicates(t *testing.T) {
	t.Parallel()

	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	at := sessiontest.At("10:00")
	login := func(eventID string) (session.Mutation, error) {
		return session.DecideLogin(session.CurrentSessionSnapshot{}, session.LoginCommand{
			EventID: eventID, SessionID: sessiontest.SessionID(1), Key: key,
			Tags: []string{"user"}, Timestamp: at,
		})
	}

	for _, eventID := range []string{"", "not-a-uuid", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa"} {
		if _, err := login(eventID); !errors.Is(err, session.ErrInvalidInput) {
			t.Errorf("DecideLogin(eventID=%q) error = %v, want ErrInvalidInput", eventID, err)
		}
	}

	canonical := sessiontest.EventID(1)
	mutation, err := login(strings.ToUpper(canonical))
	started := decided[session.StartSession](t, mutation, err)
	if started.EventID != canonical {
		t.Errorf("mutation event ID = %q, want lower-case %q", started.EventID, canonical)
	}
	if started.Session.LastEventID != canonical {
		t.Errorf("session event ID = %q, want lower-case %q", started.Session.LastEventID, canonical)
	}

	// This snapshot has no active session and the command is an hour stale, so
	// both the transition and staleness checks would reject the event if the
	// duplicate check did not come first.
	snapshot := session.CurrentSessionSnapshot{LastEventAt: sessiontest.Ptr(at), LastEventID: canonical}
	repeat, err := session.DecideUpdate(snapshot, session.UpdateCommand{
		EventID: strings.ToUpper(canonical), Key: key, Tags: []string{"ignored"}, Timestamp: at.Add(-time.Hour),
	})
	duplicate := decided[session.DuplicateEvent](t, repeat, err)
	if duplicate.EventID != canonical {
		t.Errorf("duplicate event ID = %q, want normalized %q", duplicate.EventID, canonical)
	}
}
