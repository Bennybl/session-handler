package session

import (
	"errors"
	"testing"
	"time"
)

const (
	eventIDUpper = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"
	eventIDLower = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	eventID2     = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaab"
	eventID3     = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaac"
	eventID4     = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaad"
)

func TestEventIDIsRequiredValidatedAndNormalized(t *testing.T) {
	t.Parallel()
	key := mustSessionKey(t, "tenant-a", "alice", "192.0.2.10")
	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)

	for _, eventID := range []string{"", "not-a-uuid", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa"} {
		_, err := DecideLogin(CurrentSessionSnapshot{}, LoginCommand{
			EventID: eventID, SessionID: "session-1", Key: key, Tags: []string{"user"}, Timestamp: at,
		})
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("DecideLogin(eventID=%q) error = %v, want ErrInvalidInput", eventID, err)
		}
	}

	mutation, err := DecideLogin(CurrentSessionSnapshot{}, LoginCommand{
		EventID: eventIDUpper, SessionID: "session-1", Key: key, Tags: []string{"user"}, Timestamp: at,
	})
	if err != nil {
		t.Fatalf("DecideLogin() error = %v", err)
	}
	started := mutation.(StartSession)
	if started.EventID != eventIDLower || started.Session.LastEventID != eventIDLower {
		t.Fatalf("normalized event IDs = mutation %q, session %q", started.EventID, started.Session.LastEventID)
	}
}

func TestDuplicateEventIDReturnsNoOpBeforeTimestampAndTransitionChecks(t *testing.T) {
	t.Parallel()
	key := mustSessionKey(t, "tenant-a", "alice", "192.0.2.10")
	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	snapshot := CurrentSessionSnapshot{LastEventAt: &at, LastEventID: eventIDLower}

	mutation, err := DecideUpdate(snapshot, UpdateCommand{
		EventID: eventIDUpper,
		Key:     key, Tags: []string{"ignored"}, Timestamp: at.Add(-time.Hour),
	})
	if err != nil {
		t.Fatalf("DecideUpdate(duplicate) error = %v", err)
	}
	duplicate, ok := mutation.(DuplicateEvent)
	if !ok || duplicate.EventID != eventIDLower {
		t.Fatalf("duplicate mutation = %#v, want normalized DuplicateEvent", mutation)
	}
}
