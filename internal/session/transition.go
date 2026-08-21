package session

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

type MutationKind string

const (
	MutationStartSession   MutationKind = "start_session"
	MutationReplaceState   MutationKind = "replace_state"
	MutationEndSession     MutationKind = "end_session"
	MutationDuplicateEvent MutationKind = "duplicate_event"
)

type Mutation interface {
	Kind() MutationKind
	EventTime() time.Time
	AcceptedEventID() string
}

type StartSession struct {
	EventID string
	Session Session
}

func (StartSession) Kind() MutationKind { return MutationStartSession }

func (m StartSession) EventTime() time.Time { return m.Session.LoginAt }

func (m StartSession) AcceptedEventID() string { return m.EventID }

type ReplaceState struct {
	EventID        string
	SessionID      string
	CloseCurrentAt time.Time
	State          SessionState
}

func (ReplaceState) Kind() MutationKind { return MutationReplaceState }

func (m ReplaceState) EventTime() time.Time { return m.CloseCurrentAt }

func (m ReplaceState) AcceptedEventID() string { return m.EventID }

type EndSession struct {
	EventID        string
	SessionID      string
	CloseCurrentAt time.Time
	LogoutAt       time.Time
}

func (EndSession) Kind() MutationKind { return MutationEndSession }

func (m EndSession) EventTime() time.Time { return m.LogoutAt }

func (m EndSession) AcceptedEventID() string { return m.EventID }

type DuplicateEvent struct {
	EventID   string
	Timestamp time.Time
}

func (DuplicateEvent) Kind() MutationKind { return MutationDuplicateEvent }

func (m DuplicateEvent) EventTime() time.Time { return m.Timestamp }

func (m DuplicateEvent) AcceptedEventID() string { return m.EventID }

func DecideLogin(snapshot CurrentSessionSnapshot, command LoginCommand) (Mutation, error) {
	eventID, duplicate, err := prepareTransition(snapshot, command.Key, command.Timestamp, command.EventID)
	if err != nil {
		return nil, err
	}
	if duplicate {
		return DuplicateEvent{EventID: eventID, Timestamp: command.Timestamp}, nil
	}
	command.EventID = eventID
	return decideLogin(snapshot, command)
}

func DecideUpdate(snapshot CurrentSessionSnapshot, command UpdateCommand) (Mutation, error) {
	eventID, duplicate, err := prepareTransition(snapshot, command.Key, command.Timestamp, command.EventID)
	if err != nil {
		return nil, err
	}
	if duplicate {
		return DuplicateEvent{EventID: eventID, Timestamp: command.Timestamp}, nil
	}
	command.EventID = eventID
	return decideUpdate(snapshot, command)
}

func DecideLogout(snapshot CurrentSessionSnapshot, command LogoutCommand) (Mutation, error) {
	eventID, duplicate, err := prepareTransition(snapshot, command.Key, command.Timestamp, command.EventID)
	if err != nil {
		return nil, err
	}
	if duplicate {
		return DuplicateEvent{EventID: eventID, Timestamp: command.Timestamp}, nil
	}
	command.EventID = eventID
	return decideLogout(snapshot, command)
}

func prepareTransition(snapshot CurrentSessionSnapshot, key SessionKey, timestamp time.Time, rawEventID string) (string, bool, error) {
	if err := key.validate(); err != nil {
		return "", false, err
	}
	eventID, err := NormalizeEventID(rawEventID)
	if err != nil {
		return "", false, err
	}
	if snapshot.LastEventID == eventID {
		return eventID, true, nil
	}
	if timestamp.IsZero() {
		return "", false, fmt.Errorf("%w: timestamp is required", ErrInvalidInput)
	}
	if snapshot.LastEventAt != nil && timestamp.Before(*snapshot.LastEventAt) {
		return "", false, fmt.Errorf("%w: timestamp precedes the latest accepted event", ErrStaleEvent)
	}
	if snapshot.Active != nil && snapshot.Active.Key != key {
		return "", false, fmt.Errorf("%w: active session does not match the command key", ErrInvalidInput)
	}
	return eventID, false, nil
}

func NormalizeEventID(value string) (string, error) {
	compact := strings.ReplaceAll(value, "-", "")
	if len(value) != 36 || len(compact) != 32 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return "", fmt.Errorf("%w: event ID must be a UUID", ErrInvalidInput)
	}
	if _, err := hex.DecodeString(compact); err != nil {
		return "", fmt.Errorf("%w: event ID must be a UUID", ErrInvalidInput)
	}
	return strings.ToLower(value), nil
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

	return StartSession{EventID: command.EventID, Session: Session{
		ID:          command.SessionID,
		Key:         command.Key,
		LoginAt:     command.Timestamp,
		LastEventID: command.EventID,
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
		EventID:        command.EventID,
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
		EventID:        command.EventID,
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
