package query

import (
	"encoding/base64"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
)

func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()

	at := mustTime(t, "2026-08-21T10:00:00Z")
	want := Cursor{
		Storage:     "postgres",
		Fingerprint: "query-fingerprint",
		EvaluatedAt: at,
		After: SortKey{
			TenantID:  "tenant-a",
			Username:  "alice",
			IP:        "192.0.2.10",
			LoginAt:   at,
			SessionID: "11111111-1111-4111-8111-111111111111",
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

	at := mustTime(t, "2026-08-21T10:00:00Z")
	encoded, err := EncodeCursor(Cursor{
		Storage:     "memory",
		Fingerprint: "query-a",
		EvaluatedAt: at,
		After:       SortKey{TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10", LoginAt: at, SessionID: "session-1"},
	})
	if err != nil {
		t.Fatalf("EncodeCursor() error = %v", err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode test cursor error = %v", err)
	}
	withTrailingGarbage := base64.RawURLEncoding.EncodeToString(append(raw, 'x'))

	tests := []struct {
		name        string
		value       string
		storage     string
		fingerprint string
	}{
		{name: "malformed", value: "not-base64", storage: "memory", fingerprint: "query-a"},
		{name: "trailing garbage", value: withTrailingGarbage, storage: "memory", fingerprint: "query-a"},
		{name: "storage mismatch", value: encoded, storage: "postgres", fingerprint: "query-a"},
		{name: "query mismatch", value: encoded, storage: "memory", fingerprint: "query-b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := DecodeCursor(tt.value, tt.storage, tt.fingerprint); !errors.Is(err, repository.ErrInvalidCursor) {
				t.Fatalf("DecodeCursor() error = %v, want ErrInvalidCursor", err)
			}
		})
	}
}

func TestSortKeyProvidesStableSessionOrdering(t *testing.T) {
	t.Parallel()

	at := mustTime(t, "2026-08-21T10:00:00Z")
	sessions := []session.Session{
		{ID: "session-3", Key: session.SessionKey{TenantID: "tenant-b", Username: "alice", IP: "192.0.2.1"}, LoginAt: at},
		{ID: "session-2", Key: session.SessionKey{TenantID: "tenant-a", Username: "bob", IP: "192.0.2.1"}, LoginAt: at},
		{ID: "session-1", Key: session.SessionKey{TenantID: "tenant-a", Username: "alice", IP: "192.0.2.2"}, LoginAt: at},
		{ID: "session-0", Key: session.SessionKey{TenantID: "tenant-a", Username: "alice", IP: "192.0.2.1"}, LoginAt: at.Add(time.Second)},
	}

	sort.Slice(sessions, func(i, j int) bool {
		return CompareSortKeys(SortKeyFor(sessions[i]), SortKeyFor(sessions[j])) < 0
	})
	want := []string{"session-0", "session-1", "session-2", "session-3"}
	for index, id := range want {
		if sessions[index].ID != id {
			t.Fatalf("sessions[%d].ID = %q, want %q", index, sessions[index].ID, id)
		}
	}
}
