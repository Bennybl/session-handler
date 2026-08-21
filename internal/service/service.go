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
	TenantID  string
	Username  string
	IP        string
	Tags      []string
	Timestamp time.Time
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
	eventType, err := normalizeEventType(event.Type)
	if err != nil {
		return err
	}
	key, err := session.NewSessionKey(event.TenantID, event.Username, event.IP)
	if err != nil {
		return err
	}
	eventID, err := session.NormalizeEventID(event.EventID)
	if err != nil {
		return err
	}
	if event.Timestamp.IsZero() {
		return fmt.Errorf("%w: timestamp is required", session.ErrInvalidInput)
	}
	timestamp := event.Timestamp.UTC()

	var sessionID string
	if eventType == EventLogin {
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

	return s.repository.Mutate(ctx, key, func(snapshot session.CurrentSessionSnapshot) (session.Mutation, error) {
		switch eventType {
		case EventLogin:
			return session.DecideLogin(snapshot, session.LoginCommand{
				EventID: eventID, SessionID: sessionID, Key: key, Tags: event.Tags, Timestamp: timestamp,
			})
		case EventUpdate:
			return session.DecideUpdate(snapshot, session.UpdateCommand{
				EventID: eventID, Key: key, Tags: event.Tags, Timestamp: timestamp,
			})
		case EventLogout:
			return session.DecideLogout(snapshot, session.LogoutCommand{
				EventID: eventID, Key: key, Timestamp: timestamp,
			})
		default:
			panic("normalized event type is unsupported")
		}
	})
}

func (s *SessionService) Query(ctx context.Context, request QueryRequest) (session.QueryResult, error) {
	if ctx == nil {
		return session.QueryResult{}, fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return session.QueryResult{}, err
	}
	if err := validateQueryStructure(request); err != nil {
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

func validateQueryStructure(request QueryRequest) error {
	if request.Page.Limit < 0 {
		return fmt.Errorf("%w: page limit cannot be negative", repository.ErrInvalidQuery)
	}
	for _, filter := range request.Filters {
		if strings.TrimSpace(string(filter.Field)) == "" {
			return fmt.Errorf("%w: filter field is required", repository.ErrInvalidQuery)
		}
		if strings.TrimSpace(string(filter.Operator)) == "" {
			return fmt.Errorf("%w: filter operator is required", repository.ErrInvalidQuery)
		}
		if filter.Value == nil {
			return fmt.Errorf("%w: filter value is required", repository.ErrInvalidQuery)
		}
	}
	return nil
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
