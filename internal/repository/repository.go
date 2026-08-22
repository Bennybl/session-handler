package repository

import (
	"context"
	"errors"

	"github.com/Bennybl/session-handler/internal/session"
)

var (
	ErrClosed        = errors.New("repository is closed")
	ErrInvalidQuery  = errors.New("invalid query")
	ErrInvalidCursor = errors.New("invalid cursor")
)

// SessionRepository is the storage-neutral boundary for session lifecycle data.
//
// The caller must serialize the complete LoadCurrent, domain-decision, and
// ApplyMutation sequence for each key. The event Source's broker partition
// worker is the only application-level mechanism that supplies that guarantee.
type SessionRepository interface {
	LoadCurrent(ctx context.Context, key session.SessionKey) (session.CurrentSessionSnapshot, error)
	ApplyMutation(ctx context.Context, key session.SessionKey, mutation session.Mutation) error
	Query(ctx context.Context, spec session.QuerySpec) (session.QueryResult, error)
	Close() error
}
