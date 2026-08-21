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

func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()

	at := sessiontest.At("10:00")
	want := Cursor{
		Storage:     "postgres",
		Fingerprint: "query-fingerprint",
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
	got, err := DecodeCursor(encoded, "postgres", "query-fingerprint")
	if err != nil {
		t.Fatalf("DecodeCursor() error = %v", err)
	}
	if got != want {
		t.Fatalf("DecodeCursor() = %+v, want %+v", got, want)
	}
}

func TestCursorRejectsMalformedAndMismatchedValues(t *testing.T) {
	t.Parallel()

	at := sessiontest.At("10:00")
	encoded, err := EncodeCursor(Cursor{
		Storage:     "memory",
		Fingerprint: "query-a",
		EvaluatedAt: at,
		After: SortKey{
			TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10",
			LoginAt: at, SessionID: sessiontest.SessionID(1),
		},
	})
	if err != nil {
		t.Fatalf("EncodeCursor() error = %v", err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode fixture cursor: %v", err)
	}
	withTrailingGarbage := base64.RawURLEncoding.EncodeToString(append(raw, 'x'))

	tests := []struct {
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
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := DecodeCursor(test.value, test.storage, test.fingerprint); !errors.Is(err, repository.ErrInvalidCursor) {
				t.Fatalf("DecodeCursor() error = %v, want ErrInvalidCursor", err)
			}
		})
	}
}

func TestSortKeyProvidesStableSessionOrdering(t *testing.T) {
	t.Parallel()

	at := sessiontest.At("10:00")
	// Each session differs from the first in exactly one sort field, so the
	// resulting order shows the precedence of tenant, username, IP, then login.
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
