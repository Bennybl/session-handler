package integration_test

import (
	"reflect"
	"testing"

	"github.com/Bennybl/session-handler/internal/repository/memory"
)

func TestMemoryStdinLifecycleIsQueryableOnlyThroughHTTP(t *testing.T) {
	t.Parallel()
	repo := memory.New()
	t.Cleanup(func() { _ = repo.Close() })
	harness := newApplicationHarness(t, repo, fixtureIDGenerator(t))
	deadLetters := &recordingDeadLetters{}
	consumeStdin(t, harness.service, fixtureEvents(), deadLetters)

	got := exerciseHTTPQueries(t, harness.handler)
	want := querySnapshot{
		AllIDs:       []string{aliceSessionID, bobSessionID, otherAliceSessionID},
		ActiveIDs:    []string{bobSessionID, otherAliceSessionID},
		AdminIDs:     []string{aliceSessionID},
		AliceStates:  2,
		PaginatedIDs: []string{aliceSessionID, bobSessionID, otherAliceSessionID},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("query snapshot = %+v, want %+v", got, want)
	}
	if deadLetters.count() != 1 {
		t.Fatalf("dead letters = %d, want 1 invalid transition", deadLetters.count())
	}
}

func TestMemoryResetStartsWithNoSessions(t *testing.T) {
	t.Parallel()
	first := memory.New()
	firstHarness := newApplicationHarness(t, first, fixtureIDGenerator(t))
	consumeStdin(t, firstHarness.service, fixtureEvents()[:1], &recordingDeadLetters{})
	if got := queryHTTP(t, firstHarness.handler, map[string]any{}); len(got.Sessions) != 1 {
		t.Fatalf("first memory store sessions = %+v", got.Sessions)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first memory Close() error = %v", err)
	}

	second := memory.New()
	t.Cleanup(func() { _ = second.Close() })
	secondHarness := newApplicationHarness(t, second, fixtureIDGenerator(t))
	if got := queryHTTP(t, secondHarness.handler, map[string]any{}); len(got.Sessions) != 0 {
		t.Fatalf("fresh memory store sessions = %+v, want none", got.Sessions)
	}
}
