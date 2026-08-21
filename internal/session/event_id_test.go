package session_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

func TestEventIDIsRequiredValidatedAndNormalized(t *testing.T) {
	t.Parallel()

	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	login := func(eventID string) (session.Mutation, error) {
		return session.DecideLogin(session.CurrentSessionSnapshot{}, session.LoginCommand{
			EventID: eventID, SessionID: sessiontest.SessionID(1), Key: key,
			Tags: []string{"user"}, Timestamp: sessiontest.At("10:00"),
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
}

func TestDuplicateEventIDReturnsNoOpBeforeTimestampAndTransitionChecks(t *testing.T) {
	t.Parallel()

	key := sessiontest.Key("tenant-a", "alice", "192.0.2.10")
	at := sessiontest.At("10:00")
	accepted := sessiontest.EventID(1)

	// The snapshot has no active session and the command is an hour stale, so
	// both the transition and staleness checks would reject this event if the
	// duplicate check did not come first.
	snapshot := session.CurrentSessionSnapshot{LastEventAt: sessiontest.Ptr(at), LastEventID: accepted}
	mutation, err := session.DecideUpdate(snapshot, session.UpdateCommand{
		EventID: strings.ToUpper(accepted), Key: key, Tags: []string{"ignored"}, Timestamp: at.Add(-time.Hour),
	})
	duplicate := decided[session.DuplicateEvent](t, mutation, err)

	if duplicate.EventID != accepted {
		t.Errorf("duplicate event ID = %q, want normalized %q", duplicate.EventID, accepted)
	}
}
