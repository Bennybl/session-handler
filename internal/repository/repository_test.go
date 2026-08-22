package repository_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
)

// The repository boundary loads snapshots and applies typed mutations, plus a
// set of failures callers distinguish, so none of them may wrap another.
func TestRepositoryBoundaryTypesAndErrors(t *testing.T) {
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

func (*interfaceProbe) LoadCurrent(context.Context, session.SessionKey) (session.CurrentSessionSnapshot, error) {
	return session.CurrentSessionSnapshot{}, nil
}

func (*interfaceProbe) ApplyMutation(context.Context, session.SessionKey, session.Mutation) error {
	return nil
}

func (*interfaceProbe) Query(context.Context, session.QuerySpec) (session.QueryResult, error) {
	return session.QueryResult{}, nil
}

func (*interfaceProbe) Close() error { return nil }

var _ repository.SessionRepository = (*interfaceProbe)(nil)
