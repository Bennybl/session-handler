package session

import (
	"fmt"
	"time"
)

type MutationKind string

const (
	MutationStartSession MutationKind = "start_session"
	MutationReplaceState MutationKind = "replace_state"
	MutationEndSession   MutationKind = "end_session"
)

type Mutation interface {
	Kind() MutationKind
	EventTime() time.Time
}

type StartSession struct {
	Session Session
}

func (StartSession) Kind() MutationKind { return MutationStartSession }

func (m StartSession) EventTime() time.Time { return m.Session.LoginAt }

type ReplaceState struct {
	SessionID      string
	CloseCurrentAt time.Time
	State          SessionState
}

func (ReplaceState) Kind() MutationKind { return MutationReplaceState }

func (m ReplaceState) EventTime() time.Time { return m.CloseCurrentAt }

type EndSession struct {
	SessionID      string
	CloseCurrentAt time.Time
	LogoutAt       time.Time
}

func (EndSession) Kind() MutationKind { return MutationEndSession }

func (m EndSession) EventTime() time.Time { return m.LogoutAt }

func DecideLogin(snapshot CurrentSessionSnapshot, command LoginCommand) (Mutation, error) {
	if err := validateTransitionInput(snapshot, command.Key, command.Timestamp); err != nil {
		return nil, err
	}
	return decideLogin(snapshot, command)
}

func DecideUpdate(snapshot CurrentSessionSnapshot, command UpdateCommand) (Mutation, error) {
	if err := validateTransitionInput(snapshot, command.Key, command.Timestamp); err != nil {
		return nil, err
	}
	return decideUpdate(snapshot, command)
}

func DecideLogout(snapshot CurrentSessionSnapshot, command LogoutCommand) (Mutation, error) {
	if err := validateTransitionInput(snapshot, command.Key, command.Timestamp); err != nil {
		return nil, err
	}
	return decideLogout(snapshot, command)
}

func validateTransitionInput(snapshot CurrentSessionSnapshot, key SessionKey, timestamp time.Time) error {
	if err := key.validate(); err != nil {
		return err
	}
	if timestamp.IsZero() {
		return fmt.Errorf("%w: timestamp is required", ErrInvalidInput)
	}
	if snapshot.LastEventAt != nil && timestamp.Before(*snapshot.LastEventAt) {
		return fmt.Errorf("%w: timestamp precedes the latest accepted event", ErrStaleEvent)
	}
	if snapshot.Active != nil && snapshot.Active.Key != key {
		return fmt.Errorf("%w: active session does not match the command key", ErrInvalidInput)
	}
	return nil
}

func decideLogin(snapshot CurrentSessionSnapshot, command LoginCommand) (Mutation, error) {
	if snapshot.Active != nil {
		return nil, fmt.Errorf("%w: cannot log in an active session", ErrInvalidTransition)
	}
	if command.SessionID == "" {
		return nil, fmt.Errorf("%w: a new session ID is required for login", ErrInvalidInput)
	}
	tags, err := NormalizeTags(command.Tags)
	if err != nil {
		return nil, err
	}

	return StartSession{Session: Session{
		ID:      command.SessionID,
		Key:     command.Key,
		LoginAt: command.Timestamp,
		States: []SessionState{{
			Tags:      tags,
			ValidFrom: command.Timestamp,
		}},
	}}, nil
}

func decideUpdate(snapshot CurrentSessionSnapshot, command UpdateCommand) (Mutation, error) {
	active, _, err := currentState(snapshot)
	if err != nil {
		return nil, err
	}
	tags, err := NormalizeTags(command.Tags)
	if err != nil {
		return nil, err
	}

	return ReplaceState{
		SessionID:      active.ID,
		CloseCurrentAt: command.Timestamp,
		State: SessionState{
			Tags:      tags,
			ValidFrom: command.Timestamp,
		},
	}, nil
}

func decideLogout(snapshot CurrentSessionSnapshot, command LogoutCommand) (Mutation, error) {
	active, _, err := currentState(snapshot)
	if err != nil {
		return nil, err
	}

	return EndSession{
		SessionID:      active.ID,
		CloseCurrentAt: command.Timestamp,
		LogoutAt:       command.Timestamp,
	}, nil
}

func currentState(snapshot CurrentSessionSnapshot) (*Session, SessionState, error) {
	if snapshot.Active == nil {
		return nil, SessionState{}, fmt.Errorf("%w: session is not active", ErrInvalidTransition)
	}
	if snapshot.Active.ID == "" || snapshot.Active.LogoutAt != nil || len(snapshot.Active.States) == 0 {
		return nil, SessionState{}, fmt.Errorf("%w: active session snapshot is malformed", ErrInvalidInput)
	}
	current := snapshot.Active.States[len(snapshot.Active.States)-1]
	if current.ValidTo != nil {
		return nil, SessionState{}, fmt.Errorf("%w: active session has no open state", ErrInvalidInput)
	}
	return snapshot.Active, current, nil
}
