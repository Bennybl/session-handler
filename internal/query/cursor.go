package query

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
)

const cursorVersion = 1

type SortKey struct {
	TenantID  string    `json:"tenantId"`
	Username  string    `json:"username"`
	IP        string    `json:"ip"`
	LoginAt   time.Time `json:"loginAt"`
	SessionID string    `json:"sessionId"`
}

type Cursor struct {
	Storage     string
	Fingerprint string
	EvaluatedAt time.Time
	After       SortKey
}

type cursorPayload struct {
	Version     int       `json:"version"`
	Storage     string    `json:"storage"`
	Fingerprint string    `json:"fingerprint"`
	EvaluatedAt time.Time `json:"evaluatedAt"`
	After       SortKey   `json:"after"`
}

func EncodeCursor(cursor Cursor) (string, error) {
	if err := validateCursor(cursor); err != nil {
		return "", err
	}
	payload := cursorPayload{
		Version:     cursorVersion,
		Storage:     cursor.Storage,
		Fingerprint: cursor.Fingerprint,
		EvaluatedAt: cursor.EvaluatedAt.UTC(),
		After:       cursor.After,
	}
	payload.After.LoginAt = payload.After.LoginAt.UTC()
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", invalidCursor("cannot encode cursor: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func DecodeCursor(encoded, expectedStorage, expectedFingerprint string) (Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return Cursor{}, invalidCursor("cursor is not valid base64")
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	var payload cursorPayload
	if err := decoder.Decode(&payload); err != nil {
		return Cursor{}, invalidCursor("cursor payload is invalid: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Cursor{}, invalidCursor("cursor contains trailing data")
	}
	if payload.Version != cursorVersion {
		return Cursor{}, invalidCursor("unsupported cursor version %d", payload.Version)
	}
	cursor := Cursor{
		Storage:     payload.Storage,
		Fingerprint: payload.Fingerprint,
		EvaluatedAt: payload.EvaluatedAt.UTC(),
		After:       payload.After,
	}
	cursor.After.LoginAt = cursor.After.LoginAt.UTC()
	if err := validateCursor(cursor); err != nil {
		return Cursor{}, err
	}
	if cursor.Storage != expectedStorage {
		return Cursor{}, invalidCursor("cursor belongs to a different storage adapter")
	}
	if cursor.Fingerprint != expectedFingerprint {
		return Cursor{}, invalidCursor("cursor belongs to a different query")
	}
	return cursor, nil
}

func validateCursor(cursor Cursor) error {
	if cursor.Storage == "" || cursor.Fingerprint == "" || cursor.EvaluatedAt.IsZero() {
		return invalidCursor("cursor metadata is incomplete")
	}
	if cursor.After.TenantID == "" || cursor.After.Username == "" || cursor.After.IP == "" || cursor.After.LoginAt.IsZero() || cursor.After.SessionID == "" {
		return invalidCursor("cursor sort key is incomplete")
	}
	return nil
}

func SortKeyFor(value session.Session) SortKey {
	return SortKey{
		TenantID:  value.Key.TenantID,
		Username:  value.Key.Username,
		IP:        value.Key.IP,
		LoginAt:   value.LoginAt,
		SessionID: value.ID,
	}
}

func CompareSortKeys(left, right SortKey) int {
	if comparison := strings.Compare(left.TenantID, right.TenantID); comparison != 0 {
		return comparison
	}
	if comparison := strings.Compare(left.Username, right.Username); comparison != 0 {
		return comparison
	}
	if comparison := strings.Compare(left.IP, right.IP); comparison != 0 {
		return comparison
	}
	if comparison := left.LoginAt.Compare(right.LoginAt); comparison != 0 {
		return comparison
	}
	return strings.Compare(left.SessionID, right.SessionID)
}

func invalidCursor(format string, args ...any) error {
	return fmt.Errorf("%w: %s", repository.ErrInvalidCursor, fmt.Sprintf(format, args...))
}
