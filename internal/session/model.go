package session

import (
	"fmt"
	"net"
	"sort"
	"strings"
	"time"
)

type UserKey struct {
	TenantID string
	Username string
}

type SessionKey struct {
	TenantID string
	Username string
	IP       string
}

func NewSessionKey(tenantID, username, ip string) (SessionKey, error) {
	if strings.TrimSpace(tenantID) == "" {
		return SessionKey{}, fmt.Errorf("%w: tenant ID is required", ErrInvalidInput)
	}
	if strings.TrimSpace(username) == "" {
		return SessionKey{}, fmt.Errorf("%w: username is required", ErrInvalidInput)
	}
	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return SessionKey{}, fmt.Errorf("%w: IP address is invalid", ErrInvalidInput)
	}

	return SessionKey{TenantID: tenantID, Username: username, IP: parsedIP.String()}, nil
}

func (k SessionKey) User() UserKey {
	return UserKey{TenantID: k.TenantID, Username: k.Username}
}

func (k SessionKey) validate() error {
	normalized, err := NewSessionKey(k.TenantID, k.Username, k.IP)
	if err != nil {
		return err
	}
	if normalized != k {
		return fmt.Errorf("%w: session key must contain a canonical IP address", ErrInvalidInput)
	}
	return nil
}

type Session struct {
	ID          string
	Key         SessionKey
	LoginAt     time.Time
	LogoutAt    *time.Time
	LastEventID string
	States      []SessionState
}

type SessionState struct {
	Tags      []string
	ValidFrom time.Time
	ValidTo   *time.Time
}

func NormalizeTags(tags []string) ([]string, error) {
	unique := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		if strings.TrimSpace(tag) == "" {
			return nil, fmt.Errorf("%w: tags cannot contain an empty value", ErrInvalidInput)
		}
		unique[tag] = struct{}{}
	}

	normalized := make([]string, 0, len(unique))
	for tag := range unique {
		normalized = append(normalized, tag)
	}
	sort.Strings(normalized)
	return normalized, nil
}

type LoginCommand struct {
	EventID   string
	SessionID string
	Key       SessionKey
	Tags      []string
	Timestamp time.Time
}

type UpdateCommand struct {
	EventID   string
	Key       SessionKey
	Tags      []string
	Timestamp time.Time
}

type LogoutCommand struct {
	EventID   string
	Key       SessionKey
	Timestamp time.Time
}

type CurrentSessionSnapshot struct {
	LastEventAt *time.Time
	LastEventID string
	Active      *Session
}
