package repository_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

// A mutation callback reads the domain snapshot and answers with a typed
// mutation, which is the whole contract between a repository and the domain.
func TestMutationFuncUsesDomainSnapshotAndTypedMutation(t *testing.T) {
	t.Parallel()

	at := sessiontest.At("10:00")
	snapshot := session.CurrentSessionSnapshot{LastEventAt: &at}
	want := session.EndSession{
		EventID: sessiontest.EventID(1), SessionID: sessiontest.SessionID(1),
		CloseCurrentAt: at, LogoutAt: at,
	}

	decide := repository.MutationFunc(func(got session.CurrentSessionSnapshot) (session.Mutation, error) {
		if got.LastEventAt == nil || !got.LastEventAt.Equal(at) {
			t.Fatalf("LastEventAt = %v, want %v", got.LastEventAt, at)
		}
		return want, nil
	})

	got, err := decide(snapshot)
	if err != nil {
		t.Fatalf("MutationFunc() error = %v", err)
	}
	if got != want {
		t.Fatalf("MutationFunc() = %#v, want %#v", got, want)
	}
}

// Callers distinguish these failures, so none may wrap another.
func TestRepositoryErrorsAreDistinct(t *testing.T) {
	t.Parallel()

	named := map[string]error{
		"ErrClosed":        repository.ErrClosed,
		"ErrInvalidQuery":  repository.ErrInvalidQuery,
		"ErrInvalidCursor": repository.ErrInvalidCursor,
	}
	for name, candidate := range named {
		if candidate == nil {
			t.Errorf("%s is nil", name)
			continue
		}
		for otherName, other := range named {
			if name != otherName && errors.Is(candidate, other) {
				t.Errorf("%s matches %s, want distinct errors", name, otherName)
			}
		}
	}
}

// interfaceProbe pins the SessionRepository method set at compile time.
type interfaceProbe struct{}

func (*interfaceProbe) Mutate(context.Context, session.SessionKey, repository.MutationFunc) error {
	return nil
}

func (*interfaceProbe) Query(context.Context, session.QuerySpec) (session.QueryResult, error) {
	return session.QueryResult{}, nil
}

func (*interfaceProbe) Close() error { return nil }

var _ repository.SessionRepository = (*interfaceProbe)(nil)
