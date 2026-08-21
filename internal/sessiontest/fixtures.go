// Package sessiontest provides the fixtures shared by the session-handler test
// suites: a fixed clock, canonical session keys, stable identifiers, and query
// filters.
//
// Value constructors panic instead of reporting through *testing.T because they
// are always called with literals. An unparsable fixture is a bug in the test
// itself, not a condition the test is examining.
package sessiontest

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Bennybl/session-handler/internal/session"
)

// Day is the UTC date every fixture timestamp falls on unless it names another.
const Day = "2026-08-21"

// At builds a fixture timestamp from a clock time on Day ("10:00", "10:30:15")
// or from an explicit date and clock time ("2026-08-22 12:00").
func At(value string) time.Time {
	date, clock := Day, value
	if fields := strings.Fields(value); len(fields) == 2 {
		date, clock = fields[0], fields[1]
	}
	if strings.Count(clock, ":") == 1 {
		clock += ":00"
	}
	parsed, err := time.Parse(time.RFC3339Nano, date+"T"+clock+"Z")
	if err != nil {
		panic(fmt.Sprintf("sessiontest: unparsable fixture time %q: %v", value, err))
	}
	return parsed
}

// Key builds a canonical session key.
func Key(tenantID, username, ip string) session.SessionKey {
	key, err := session.NewSessionKey(tenantID, username, ip)
	if err != nil {
		panic(fmt.Sprintf("sessiontest: invalid fixture key %q/%q/%q: %v", tenantID, username, ip, err))
	}
	return key
}

// SessionID returns the stable fixture session UUID with the given number.
func SessionID(number int) string { return fixtureUUID('5', number) }

// EventID returns the stable fixture event UUID with the given number. Use it
// when a test asserts on a specific event identity; prefer NextEventID for
// events that only need to be distinct.
func EventID(number int) string { return fixtureUUID('e', number) }

var eventSequence atomic.Int64

// NextEventID returns a fixture event UUID that no earlier call has returned,
// so seeded events never collide with the duplicate-event check.
func NextEventID() string { return fixtureUUID('f', int(eventSequence.Add(1))) }

func fixtureUUID(namespace rune, number int) string {
	return fmt.Sprintf("%c0000000-0000-4000-8000-%012d", namespace, number)
}

// Filter builds a query filter.
func Filter(field, operator string, value any) session.Filter {
	return session.Filter{Field: session.Field(field), Operator: session.Operator(operator), Value: value}
}

// Spec builds a query specification evaluated at the given time.
func Spec(evaluatedAt time.Time, filters ...session.Filter) session.QuerySpec {
	return session.QuerySpec{Filters: filters, Page: session.PageRequest{Limit: 50}, EvaluatedAt: evaluatedAt}
}

// ActiveSession builds a snapshot holding one active session with a single open
// state, as a repository would report it after a login.
func ActiveSession(key session.SessionKey, sessionID, eventID string, at time.Time, tags ...string) session.CurrentSessionSnapshot {
	return session.CurrentSessionSnapshot{
		LastEventAt: Ptr(at),
		LastEventID: eventID,
		Active: &session.Session{
			ID: sessionID, Key: key, LoginAt: at, LastEventID: eventID,
			States: []session.SessionState{{Tags: tags, ValidFrom: at}},
		},
	}
}

// Ptr returns a pointer to value, for the optional times the domain uses to
// mean "still open".
func Ptr[Value any](value Value) *Value { return &value }
