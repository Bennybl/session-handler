package query

import (
	"encoding/base64"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

// A cursor survives a round trip, and is refused when it is malformed, altered,
// or presented with a different query or storage adapter than it was made for.
func TestCursorRoundTripAndRejection(t *testing.T) {
	t.Parallel()

	at := sessiontest.At("10:00")
	want := Cursor{
		Storage:     "memory",
		Fingerprint: "query-a",
		EvaluatedAt: at,
		After: SortKey{
			TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10",
			LoginAt: at, SessionID: sessiontest.SessionID(1),
		},
	}

	encoded, err := EncodeCursor(want)
	if err != nil {
		t.Fatalf("EncodeCursor() error = %v", err)
	}
	got, err := DecodeCursor(encoded, "memory", "query-a")
	if err != nil {
		t.Fatalf("DecodeCursor() error = %v", err)
	}
	if got != want {
		t.Errorf("DecodeCursor() = %+v, want %+v", got, want)
	}

	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode the fixture cursor: %v", err)
	}
	withTrailingGarbage := base64.RawURLEncoding.EncodeToString(append(raw, 'x'))

	rejected := []struct {
		name        string
		value       string
		storage     string
		fingerprint string
	}{
		{name: "not base64", value: "not-base64", storage: "memory", fingerprint: "query-a"},
		{name: "trailing garbage", value: withTrailingGarbage, storage: "memory", fingerprint: "query-a"},
		{name: "another storage adapter", value: encoded, storage: "postgres", fingerprint: "query-a"},
		{name: "another query", value: encoded, storage: "memory", fingerprint: "query-b"},
	}
	for _, test := range rejected {
		if _, err := DecodeCursor(test.value, test.storage, test.fingerprint); !errors.Is(err, repository.ErrInvalidCursor) {
			t.Errorf("%s: DecodeCursor() error = %v, want ErrInvalidCursor", test.name, err)
		}
	}
}

// Sessions order by tenant, then username, then IP, then login time, then ID.
func TestSortKeyProvidesStableSessionOrdering(t *testing.T) {
	t.Parallel()

	at := sessiontest.At("10:00")
	// Each session differs from the first in exactly one sort field, so the
	// resulting order shows the precedence of those fields.
	sessions := []session.Session{
		{ID: "by-tenant", Key: sessiontest.Key("tenant-b", "alice", "192.0.2.1"), LoginAt: at},
		{ID: "by-username", Key: sessiontest.Key("tenant-a", "bob", "192.0.2.1"), LoginAt: at},
		{ID: "by-ip", Key: sessiontest.Key("tenant-a", "alice", "192.0.2.2"), LoginAt: at},
		{ID: "by-login", Key: sessiontest.Key("tenant-a", "alice", "192.0.2.1"), LoginAt: at.Add(time.Second)},
	}

	sort.Slice(sessions, func(left, right int) bool {
		return CompareSortKeys(SortKeyFor(sessions[left]), SortKeyFor(sessions[right])) < 0
	})

	want := []string{"by-login", "by-ip", "by-username", "by-tenant"}
	for index, id := range want {
		if sessions[index].ID != id {
			t.Errorf("sessions[%d] = %q, want %q", index, sessions[index].ID, id)
		}
	}
}
