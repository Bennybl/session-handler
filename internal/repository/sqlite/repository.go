package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"sync"
	"time"

	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

const storageID = "sqlite"

type Repository struct {
	mu     sync.RWMutex
	db     *sql.DB
	closed bool
}

func Open(ctx context.Context) (*Repository, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("open SQLite: %w", err)
	}
	// A bare in-memory SQLite database belongs to one connection. Retaining
	// exactly that connection also provides a simple SQL serialization point.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to SQLite: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable SQLite foreign keys: %w", err)
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply SQLite schema: %w", err)
	}
	return &Repository{db: db}, nil
}

func (r *Repository) Mutate(ctx context.Context, key session.SessionKey, fn repository.MutationFunc) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	if fn == nil {
		return fmt.Errorf("%w: mutation callback is required", session.ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return repository.ErrClosed
	}
	return r.mutate(ctx, key, fn)
}

func (r *Repository) Query(ctx context.Context, spec session.QuerySpec) (session.QueryResult, error) {
	if ctx == nil {
		return session.QueryResult{}, fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return session.QueryResult{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return session.QueryResult{}, repository.ErrClosed
	}
	return r.query(ctx, spec)
}

func (r *Repository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return repository.ErrClosed
	}
	r.closed = true
	if err := r.db.Close(); err != nil {
		return fmt.Errorf("close SQLite: %w", err)
	}
	return nil
}

func toNanos(value time.Time) int64   { return value.UTC().UnixNano() }
func fromNanos(value int64) time.Time { return time.Unix(0, value).UTC() }

func invalidMutation(format string, args ...any) error {
	return fmt.Errorf("%w: %s", session.ErrInvalidInput, fmt.Sprintf(format, args...))
}

var _ repository.SessionRepository = (*Repository)(nil)
