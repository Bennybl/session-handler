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

// MutationFunc is a side-effect-free domain transition. A repository may invoke
// it multiple times when its concurrency mechanism retries a transaction, so it
// must not perform external side effects.
type MutationFunc func(session.CurrentSessionSnapshot) (session.Mutation, error)

// SessionRepository is the storage-neutral boundary for session lifecycle data.
//
// Mutate is an atomic read-modify-write operation scoped to one SessionKey. It
// serializes same-key mutations, supplies a consistent and isolated snapshot,
// invokes fn, and commits the returned mutation or rolls back the entire
// operation. Errors returned by fn are propagated without changing storage.
type SessionRepository interface {
	Mutate(ctx context.Context, key session.SessionKey, fn MutationFunc) error
	Query(ctx context.Context, spec session.QuerySpec) (session.QueryResult, error)
	Close() error
}
