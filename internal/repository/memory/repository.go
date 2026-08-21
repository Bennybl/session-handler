package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Bennybl/session-handler/internal/query"
	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
)

const storageID = "memory"

type Repository struct {
	mu       sync.RWMutex
	sessions map[session.SessionKey][]session.Session
	latest   map[session.SessionKey]time.Time
	eventIDs map[session.SessionKey]string
	ids      map[string]struct{}
	closed   bool
}

func New() *Repository {
	return &Repository{
		sessions: make(map[session.SessionKey][]session.Session),
		latest:   make(map[session.SessionKey]time.Time),
		eventIDs: make(map[session.SessionKey]string),
		ids:      make(map[string]struct{}),
	}
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

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return repository.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	mutation, err := fn(r.snapshot(key))
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if mutation == nil {
		return fmt.Errorf("%w: mutation callback returned no mutation", session.ErrInvalidInput)
	}
	return r.apply(key, mutation)
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

	prepared, err := query.Prepare(spec, storageID, predicateRegistry)
	if err != nil {
		return session.QueryResult{}, err
	}
	values := make([]session.Session, 0)
	for _, histories := range r.sessions {
		for index := range histories {
			if err := ctx.Err(); err != nil {
				return session.QueryResult{}, err
			}
			if matches(histories[index], prepared) {
				values = append(values, cloneSession(histories[index]))
			}
		}
	}

	sort.Slice(values, func(left, right int) bool {
		return query.CompareSortKeys(query.SortKeyFor(values[left]), query.SortKeyFor(values[right])) < 0
	})
	start := seekAfter(values, prepared.After)
	end := start + prepared.Limit
	if end > len(values) {
		end = len(values)
	}
	page := values[start:end]
	if end == len(values) {
		return session.QueryResult{Sessions: page}, nil
	}

	nextCursor, err := query.EncodeCursor(query.Cursor{
		Storage:     storageID,
		Fingerprint: prepared.Fingerprint,
		EvaluatedAt: prepared.EvaluatedAt,
		After:       query.SortKeyFor(page[len(page)-1]),
	})
	if err != nil {
		return session.QueryResult{}, err
	}
	return session.QueryResult{Sessions: page, NextCursor: nextCursor}, nil
}

func (r *Repository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return repository.ErrClosed
	}
	r.closed = true
	r.sessions = nil
	r.latest = nil
	r.eventIDs = nil
	r.ids = nil
	return nil
}

func (r *Repository) snapshot(key session.SessionKey) session.CurrentSessionSnapshot {
	var snapshot session.CurrentSessionSnapshot
	if latest, exists := r.latest[key]; exists {
		value := latest
		snapshot.LastEventAt = &value
	}
	snapshot.LastEventID = r.eventIDs[key]
	history := r.sessions[key]
	for index := len(history) - 1; index >= 0; index-- {
		if history[index].LogoutAt == nil {
			active := cloneSession(history[index])
			snapshot.Active = &active
			break
		}
	}
	return snapshot
}

func (r *Repository) apply(key session.SessionKey, mutation session.Mutation) error {
	switch value := mutation.(type) {
	case session.StartSession:
		return r.start(key, value)
	case *session.StartSession:
		if value == nil {
			return invalidMutation("start mutation is nil")
		}
		return r.start(key, *value)
	case session.ReplaceState:
		return r.replace(key, value)
	case *session.ReplaceState:
		if value == nil {
			return invalidMutation("state replacement mutation is nil")
		}
		return r.replace(key, *value)
	case session.EndSession:
		return r.end(key, value)
	case *session.EndSession:
		if value == nil {
			return invalidMutation("end mutation is nil")
		}
		return r.end(key, *value)
	case session.DuplicateEvent:
		return r.duplicate(key, value)
	case *session.DuplicateEvent:
		if value == nil {
			return invalidMutation("duplicate mutation is nil")
		}
		return r.duplicate(key, *value)
	default:
		return invalidMutation("unsupported mutation type %T", mutation)
	}
}

func (r *Repository) start(key session.SessionKey, mutation session.StartSession) error {
	value := mutation.Session
	if value.ID == "" || value.Key != key || value.LoginAt.IsZero() || value.LogoutAt != nil || len(value.States) != 1 || value.LastEventID != mutation.EventID {
		return invalidMutation("start mutation is inconsistent")
	}
	if err := validateAcceptedEventID(mutation.EventID, r.eventIDs[key]); err != nil {
		return err
	}
	state := value.States[0]
	if state.ValidFrom.IsZero() || !state.ValidFrom.Equal(value.LoginAt) || state.ValidTo != nil {
		return invalidMutation("initial state is inconsistent")
	}
	if _, exists := r.ids[value.ID]; exists {
		return invalidMutation("session ID %q already exists", value.ID)
	}
	if activeIndex(r.sessions[key]) >= 0 {
		return invalidMutation("session key already has an active lifecycle")
	}
	if latest, exists := r.latest[key]; exists && value.LoginAt.Before(latest) {
		return invalidMutation("start mutation precedes latest event")
	}
	value = cloneSession(value)
	r.sessions[key] = append(r.sessions[key], value)
	r.latest[key] = value.LoginAt
	r.eventIDs[key] = mutation.EventID
	r.ids[value.ID] = struct{}{}
	return nil
}

func (r *Repository) replace(key session.SessionKey, mutation session.ReplaceState) error {
	if err := validateAcceptedEventID(mutation.EventID, r.eventIDs[key]); err != nil {
		return err
	}
	history := r.sessions[key]
	index := activeIndex(history)
	if index < 0 || history[index].ID != mutation.SessionID || mutation.CloseCurrentAt.IsZero() {
		return invalidMutation("state replacement does not identify the active session")
	}
	active := &history[index]
	if len(active.States) == 0 || active.States[len(active.States)-1].ValidTo != nil {
		return invalidMutation("active session has no open state")
	}
	if mutation.State.ValidFrom.IsZero() || !mutation.State.ValidFrom.Equal(mutation.CloseCurrentAt) || mutation.State.ValidTo != nil {
		return invalidMutation("replacement state is inconsistent")
	}
	if latest := r.latest[key]; mutation.CloseCurrentAt.Before(latest) {
		return invalidMutation("state replacement precedes latest event")
	}
	closedAt := mutation.CloseCurrentAt
	active.States[len(active.States)-1].ValidTo = &closedAt
	active.States = append(active.States, cloneState(mutation.State))
	active.LastEventID = mutation.EventID
	r.sessions[key] = history
	r.latest[key] = mutation.CloseCurrentAt
	r.eventIDs[key] = mutation.EventID
	return nil
}

func (r *Repository) end(key session.SessionKey, mutation session.EndSession) error {
	if err := validateAcceptedEventID(mutation.EventID, r.eventIDs[key]); err != nil {
		return err
	}
	history := r.sessions[key]
	index := activeIndex(history)
	if index < 0 || history[index].ID != mutation.SessionID || mutation.CloseCurrentAt.IsZero() || !mutation.CloseCurrentAt.Equal(mutation.LogoutAt) {
		return invalidMutation("end mutation does not identify the active session")
	}
	active := &history[index]
	if len(active.States) == 0 || active.States[len(active.States)-1].ValidTo != nil {
		return invalidMutation("active session has no open state")
	}
	if latest := r.latest[key]; mutation.LogoutAt.Before(latest) {
		return invalidMutation("end mutation precedes latest event")
	}
	closedAt := mutation.CloseCurrentAt
	logoutAt := mutation.LogoutAt
	active.States[len(active.States)-1].ValidTo = &closedAt
	active.LogoutAt = &logoutAt
	active.LastEventID = mutation.EventID
	r.sessions[key] = history
	r.latest[key] = logoutAt
	r.eventIDs[key] = mutation.EventID
	return nil
}

func (r *Repository) duplicate(key session.SessionKey, mutation session.DuplicateEvent) error {
	normalized, err := session.NormalizeEventID(mutation.EventID)
	if err != nil || normalized != mutation.EventID || r.eventIDs[key] != mutation.EventID {
		return invalidMutation("duplicate mutation does not match the latest event ID")
	}
	return nil
}

func validateAcceptedEventID(eventID, previous string) error {
	normalized, err := session.NormalizeEventID(eventID)
	if err != nil || normalized != eventID || eventID == previous {
		return invalidMutation("accepted event ID is invalid or already current")
	}
	return nil
}

func activeIndex(history []session.Session) int {
	for index := len(history) - 1; index >= 0; index-- {
		if history[index].LogoutAt == nil {
			return index
		}
	}
	return -1
}

func seekAfter(values []session.Session, after *query.SortKey) int {
	if after == nil {
		return 0
	}
	return sort.Search(len(values), func(index int) bool {
		return query.CompareSortKeys(query.SortKeyFor(values[index]), *after) > 0
	})
}

func cloneSession(value session.Session) session.Session {
	result := value
	if value.LogoutAt != nil {
		logoutAt := *value.LogoutAt
		result.LogoutAt = &logoutAt
	}
	result.States = make([]session.SessionState, len(value.States))
	for index := range value.States {
		result.States[index] = cloneState(value.States[index])
	}
	return result
}

func cloneState(value session.SessionState) session.SessionState {
	result := value
	result.Tags = append([]string(nil), value.Tags...)
	if value.ValidTo != nil {
		validTo := *value.ValidTo
		result.ValidTo = &validTo
	}
	return result
}

func invalidMutation(format string, args ...any) error {
	return fmt.Errorf("%w: %s", session.ErrInvalidInput, fmt.Sprintf(format, args...))
}

var _ repository.SessionRepository = (*Repository)(nil)
