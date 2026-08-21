package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	advisoryLockSeed int64 = 7_640_821_447_115_902
	maxAttempts            = 3
)

type Repository struct {
	mu     sync.RWMutex
	db     *sql.DB
	closed bool
}

func Open(ctx context.Context, databaseURL string) (*Repository, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	if strings.TrimSpace(databaseURL) == "" {
		return nil, fmt.Errorf("%w: database URL is required", session.ErrInvalidInput)
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL: %w", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	return New(db)
}

func New(db *sql.DB) (*Repository, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: database is required", session.ErrInvalidInput)
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
	if err := validateKey(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return repository.ErrClosed
	}

	return runWithRetries(ctx, func() error {
		return r.mutateOnce(ctx, key, fn)
	})
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
		return fmt.Errorf("close PostgreSQL: %w", err)
	}
	return nil
}

func (r *Repository) mutateOnce(ctx context.Context, key session.SessionKey, fn repository.MutationFunc) error {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin PostgreSQL mutation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `
		SELECT pg_advisory_xact_lock(
			hashtextextended(jsonb_build_array($1::text, $2::text, $3::text)::text, $4)
		)
	`, key.TenantID, key.Username, key.IP, advisoryLockSeed); err != nil {
		return fmt.Errorf("lock session key: %w", err)
	}

	snapshot, err := loadSnapshot(ctx, tx, key)
	if err != nil {
		return err
	}
	mutation, err := fn(cloneSnapshot(snapshot))
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if mutation == nil {
		return invalidMutation("mutation callback returned no mutation")
	}
	if err := persistMutation(ctx, tx, key, snapshot, mutation); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit PostgreSQL mutation: %w", err)
	}
	return nil
}

func validateKey(key session.SessionKey) error {
	normalized, err := session.NewSessionKey(key.TenantID, key.Username, key.IP)
	if err != nil {
		return err
	}
	if normalized != key {
		return fmt.Errorf("%w: session key must contain a canonical IP address", session.ErrInvalidInput)
	}
	return nil
}

func retryable(err error) bool {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return false
	}
	return postgresError.Code == "40001" || postgresError.Code == "40P01"
}

func runWithRetries(ctx context.Context, attempt func() error) error {
	var err error
	for range maxAttempts {
		err = attempt()
		if err == nil || !retryable(err) {
			return err
		}
		if contextError := ctx.Err(); contextError != nil {
			return contextError
		}
	}
	return err
}

func invalidMutation(format string, args ...any) error {
	return fmt.Errorf("%w: %s", session.ErrInvalidInput, fmt.Sprintf(format, args...))
}

var _ repository.SessionRepository = (*Repository)(nil)
