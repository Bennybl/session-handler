package integration_test

import (
	"reflect"
	"testing"

	"github.com/Bennybl/session-handler/internal/repository/memory"
)

// Events enter only through the stream and leave only through the query API.
// This snapshot is the baseline the PostgreSQL and NATS run must reproduce.
func TestMemoryStdinLifecycleIsQueryableOnlyThroughHTTP(t *testing.T) {
	t.Parallel()

	repo := memory.New()
	t.Cleanup(func() { _ = repo.Close() })
	harness := newApplicationHarness(t, repo, fixtureIDGenerator(t))
	deadLetters := &recordingDeadLetters{}
	consumeStdin(t, harness.service, fixtureEvents(), deadLetters)

	want := querySnapshot{
		// Alice's repeated login was deduplicated, so no session exists for it.
		AllIDs:    []string{aliceSessionID, bobSessionID, otherAliceSessionID},
		ActiveIDs: []string{bobSessionID, otherAliceSessionID},
		AdminIDs:  []string{aliceSessionID},
		// Alice was tagged "user", then "admin user".
		AliceStates:  2,
		PaginatedIDs: []string{aliceSessionID, bobSessionID, otherAliceSessionID},
	}
	if got := exerciseHTTPQueries(t, harness.handler); !reflect.DeepEqual(got, want) {
		t.Fatalf("query snapshot = %+v, want %+v", got, want)
	}

	// The trailing update had no active session to change.
	if got := deadLetters.count(); got != 1 {
		t.Fatalf("dead letters = %d, want 1 for the invalid transition", got)
	}
}

// The in-memory store keeps nothing across process lifetimes.
func TestMemoryResetStartsWithNoSessions(t *testing.T) {
	t.Parallel()

	first := memory.New()
	firstHarness := newApplicationHarness(t, first, fixtureIDGenerator(t))
	consumeStdin(t, firstHarness.service, fixtureEvents()[:1], &recordingDeadLetters{})
	if got := queryHTTP(t, firstHarness.handler, map[string]any{}); len(got.Sessions) != 1 {
		t.Fatalf("sessions in the first store = %+v, want 1", got.Sessions)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close the first store: %v", err)
	}

	second := memory.New()
	t.Cleanup(func() { _ = second.Close() })
	secondHarness := newApplicationHarness(t, second, fixtureIDGenerator(t))
	if got := queryHTTP(t, secondHarness.handler, map[string]any{}); len(got.Sessions) != 0 {
		t.Fatalf("sessions in a fresh store = %+v, want none", got.Sessions)
	}
}
