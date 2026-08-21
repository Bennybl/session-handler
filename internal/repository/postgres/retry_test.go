package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestRunWithRetriesReturnsContextErrorAfterRetryableFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	err := runWithRetries(ctx, func() error {
		attempts++
		cancel()
		return fmt.Errorf("database attempt: %w", &pgconn.PgError{
			Code:    "40001",
			Message: "serialization failure",
		})
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runWithRetries() error = %v, want context canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("runWithRetries() attempts = %d, want 1", attempts)
	}
}
