package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/session"
)

func TestMutationFuncUsesDomainSnapshotAndTypedMutation(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	snapshot := session.CurrentSessionSnapshot{LastEventAt: &at}
	want := session.EndSession{SessionID: "session-1", CloseCurrentAt: at, LogoutAt: at}

	fn := MutationFunc(func(got session.CurrentSessionSnapshot) (session.Mutation, error) {
		if got.LastEventAt == nil || !got.LastEventAt.Equal(at) {
			t.Fatalf("snapshot LastEventAt = %v, want %v", got.LastEventAt, at)
		}
		return want, nil
	})

	got, err := fn(snapshot)
	if err != nil {
		t.Fatalf("MutationFunc() error = %v", err)
	}
	if got != want {
		t.Fatalf("MutationFunc() = %#v, want %#v", got, want)
	}
}

func TestRepositoryErrorsAreDistinct(t *testing.T) {
	t.Parallel()

	known := []error{ErrClosed, ErrInvalidQuery, ErrInvalidCursor}
	for index, candidate := range known {
		if candidate == nil {
			t.Fatalf("error %d is nil", index)
		}
		for otherIndex, other := range known {
			if index != otherIndex && errors.Is(candidate, other) {
				t.Fatalf("error %d aliases error %d", index, otherIndex)
			}
		}
	}
}

type interfaceProbe struct{}

func (*interfaceProbe) Mutate(context.Context, session.SessionKey, MutationFunc) error {
	return nil
}

func (*interfaceProbe) Query(context.Context, session.QuerySpec) (session.QueryResult, error) {
	return session.QueryResult{}, nil
}

func (*interfaceProbe) Close() error {
	return nil
}

var _ SessionRepository = (*interfaceProbe)(nil)
