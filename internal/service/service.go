package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
)

type EventType string

const (
	EventLogin  EventType = "LOGIN"
	EventUpdate EventType = "UPDATE"
	EventLogout EventType = "LOGOUT"
)

type Event struct {
	EventID   string
	Type      EventType
	Key       session.SessionKey
	Tags      []string
	Timestamp time.Time
}

// EventInput is untrusted event data received from a transport.
type EventInput struct {
	EventID   string
	Type      EventType
	TenantID  string
	Username  string
	IP        string
	Tags      []string
	Timestamp time.Time
}

// NewEvent performs the one-time conversion from transport input to a trusted
// event. Later layers may rely on its key, identity, type, and timestamp being
// canonical.
func NewEvent(input EventInput) (Event, error) {
	eventType, err := normalizeEventType(input.Type)
	if err != nil {
		return Event{}, err
	}
	key, err := session.NewSessionKey(input.TenantID, input.Username, input.IP)
	if err != nil {
		return Event{}, err
	}
	eventID, err := session.NormalizeEventID(input.EventID)
	if err != nil {
		return Event{}, err
	}
	if input.Timestamp.IsZero() {
		return Event{}, fmt.Errorf("%w: timestamp is required", session.ErrInvalidInput)
	}
	return Event{
		EventID:   eventID,
		Type:      eventType,
		Key:       key,
		Tags:      append([]string(nil), input.Tags...),
		Timestamp: input.Timestamp.UTC(),
	}, nil
}

type QueryRequest struct {
	Filters []session.Filter
	Page    session.PageRequest
}

type Dependencies struct {
	Repository   repository.SessionRepository
	Now          func() time.Time
	NewSessionID func() (string, error)
}

type SessionService struct {
	repository   repository.SessionRepository
	now          func() time.Time
	newSessionID func() (string, error)
}

func New(dependencies Dependencies) (*SessionService, error) {
	if dependencies.Repository == nil {
		return nil, fmt.Errorf("%w: session repository is required", session.ErrInvalidInput)
	}
	if dependencies.Now == nil {
		dependencies.Now = time.Now
	}
	if dependencies.NewSessionID == nil {
		dependencies.NewSessionID = newUUID
	}
	return &SessionService{
		repository: dependencies.Repository, now: dependencies.Now, newSessionID: dependencies.NewSessionID,
	}, nil
}

func (s *SessionService) ApplyEvent(ctx context.Context, event Event) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var sessionID string
	var err error
	if event.Type == EventLogin {
		sessionID, err = s.newSessionID()
		if err != nil {
			return fmt.Errorf("create session ID: %w", err)
		}
		normalized, normalizeError := normalizeUUID(sessionID)
		if normalizeError != nil {
			return normalizeError
		}
		sessionID = normalized
	}

	snapshot, err := s.repository.LoadCurrent(ctx, event.Key)
	if err != nil {
		return err
	}
	var mutation session.Mutation
	switch event.Type {
	case EventLogin:
		mutation, err = session.DecideLogin(snapshot, session.LoginCommand{
			EventID: event.EventID, SessionID: sessionID, Key: event.Key, Tags: event.Tags, Timestamp: event.Timestamp,
		})
	case EventUpdate:
		mutation, err = session.DecideUpdate(snapshot, session.UpdateCommand{
			EventID: event.EventID, Key: event.Key, Tags: event.Tags, Timestamp: event.Timestamp,
		})
	case EventLogout:
		mutation, err = session.DecideLogout(snapshot, session.LogoutCommand{
			EventID: event.EventID, Key: event.Key, Timestamp: event.Timestamp,
		})
	default:
		err = fmt.Errorf("%w: trusted event has unsupported type %q", session.ErrInvalidInput, event.Type)
	}
	if err != nil {
		return err
	}
	return s.repository.ApplyMutation(ctx, event.Key, mutation)
}

func (s *SessionService) Query(ctx context.Context, request QueryRequest) (session.QueryResult, error) {
	if ctx == nil {
		return session.QueryResult{}, fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return session.QueryResult{}, err
	}
	spec := session.QuerySpec{
		Filters:     append([]session.Filter(nil), request.Filters...),
		Page:        request.Page,
		EvaluatedAt: s.now().UTC(),
	}
	return s.repository.Query(ctx, spec)
}

func normalizeEventType(value EventType) (EventType, error) {
	normalized := EventType(strings.ToUpper(strings.TrimSpace(string(value))))
	switch normalized {
	case EventLogin, EventUpdate, EventLogout:
		return normalized, nil
	default:
		return "", fmt.Errorf("%w: unsupported event type %q", session.ErrInvalidInput, value)
	}
}

func normalizeUUID(value string) (string, error) {
	compact := strings.ReplaceAll(value, "-", "")
	if len(value) != 36 || len(compact) != 32 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return "", fmt.Errorf("%w: generated session ID must be a UUID", session.ErrInvalidInput)
	}
	if _, err := hex.DecodeString(compact); err != nil {
		return "", fmt.Errorf("%w: generated session ID must be a UUID", session.ErrInvalidInput)
	}
	return strings.ToLower(value), nil
}

func newUUID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	encoded := make([]byte, 36)
	hex.Encode(encoded[0:8], raw[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], raw[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], raw[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], raw[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], raw[10:16])
	return string(encoded), nil
}
