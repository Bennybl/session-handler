package memory

import (
	"time"

	sharedquery "github.com/Bennybl/session-handler/internal/query"
	"github.com/Bennybl/session-handler/internal/session"
)

type predicate func(session.Session, *session.SessionState, any) bool

var predicateRegistry = mustPredicateRegistry()

func mustPredicateRegistry() *sharedquery.Registry[predicate] {
	entries := []sharedquery.Entry[predicate]{
		sessionEntry("sessionId", "eq", sharedquery.ValueUUID, stringEquals(func(value session.Session) string { return value.ID })),
		sessionEntry("sessionId", "in", sharedquery.ValueUUIDList, stringIn(func(value session.Session) string { return value.ID })),
		sessionEntry("tenantId", "eq", sharedquery.ValueString, stringEquals(func(value session.Session) string { return value.Key.TenantID })),
		sessionEntry("tenantId", "in", sharedquery.ValueStringList, stringIn(func(value session.Session) string { return value.Key.TenantID })),
		sessionEntry("username", "eq", sharedquery.ValueString, stringEquals(func(value session.Session) string { return value.Key.Username })),
		sessionEntry("username", "in", sharedquery.ValueStringList, stringIn(func(value session.Session) string { return value.Key.Username })),
		sessionEntry("ip", "eq", sharedquery.ValueIP, stringEquals(func(value session.Session) string { return value.Key.IP })),
		sessionEntry("ip", "in", sharedquery.ValueIPList, stringIn(func(value session.Session) string { return value.Key.IP })),
		stateEntry("tags", "containsAll", sharedquery.ValueStringList, tagsContainAll),
		stateEntry("tags", "containsAny", sharedquery.ValueStringList, tagsContainAny),
		stateEntry("activity", "at", sharedquery.ValueTimestamp, activityAt),
		stateEntry("activity", "overlaps", sharedquery.ValueInterval, activityOverlaps),
		sessionEntry("loginTime", "eq", sharedquery.ValueTimestamp, compareLogin(func(comparison int) bool { return comparison == 0 })),
		sessionEntry("loginTime", "gt", sharedquery.ValueTimestamp, compareLogin(func(comparison int) bool { return comparison > 0 })),
		sessionEntry("loginTime", "gte", sharedquery.ValueTimestamp, compareLogin(func(comparison int) bool { return comparison >= 0 })),
		sessionEntry("loginTime", "lt", sharedquery.ValueTimestamp, compareLogin(func(comparison int) bool { return comparison < 0 })),
		sessionEntry("loginTime", "lte", sharedquery.ValueTimestamp, compareLogin(func(comparison int) bool { return comparison <= 0 })),
		sessionEntry("loginTime", "between", sharedquery.ValueInterval, loginBetween),
	}
	registry, err := sharedquery.NewRegistry(entries)
	if err != nil {
		panic(err)
	}
	return registry
}

func sessionEntry(field, operator string, kind sharedquery.ValueKind, handler predicate) sharedquery.Entry[predicate] {
	return sharedquery.Entry[predicate]{
		Field: session.Field(field), Operator: session.Operator(operator), Scope: sharedquery.ScopeSession, ValueKind: kind, Handler: handler,
	}
}

func stateEntry(field, operator string, kind sharedquery.ValueKind, handler predicate) sharedquery.Entry[predicate] {
	return sharedquery.Entry[predicate]{
		Field: session.Field(field), Operator: session.Operator(operator), Scope: sharedquery.ScopeState, ValueKind: kind, Handler: handler,
	}
}

func matches(value session.Session, prepared sharedquery.PreparedQuery[predicate]) bool {
	stateFilters := make([]sharedquery.ResolvedFilter[predicate], 0)
	hasActivity := false
	for _, filter := range prepared.Filters {
		if filter.Scope == sharedquery.ScopeSession {
			if !filter.Handler(value, nil, filter.Value) {
				return false
			}
			continue
		}
		stateFilters = append(stateFilters, filter)
		if filter.Field == session.Field("activity") {
			hasActivity = true
		}
	}

	for index := range value.States {
		state := &value.States[index]
		if !hasActivity && !stateInterval(*state).Contains(prepared.EvaluatedAt) {
			continue
		}
		matched := true
		for _, filter := range stateFilters {
			if !filter.Handler(value, state, filter.Value) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

type stringField func(session.Session) string

func stringEquals(field stringField) predicate {
	return func(value session.Session, _ *session.SessionState, raw any) bool {
		return field(value) == raw.(string)
	}
}

func stringIn(field stringField) predicate {
	return func(value session.Session, _ *session.SessionState, raw any) bool {
		candidate := field(value)
		for _, expected := range raw.([]string) {
			if candidate == expected {
				return true
			}
		}
		return false
	}
}

func tagsContainAll(_ session.Session, state *session.SessionState, raw any) bool {
	tags := make(map[string]struct{}, len(state.Tags))
	for _, tag := range state.Tags {
		tags[tag] = struct{}{}
	}
	for _, expected := range raw.([]string) {
		if _, exists := tags[expected]; !exists {
			return false
		}
	}
	return true
}

func tagsContainAny(_ session.Session, state *session.SessionState, raw any) bool {
	for _, expected := range raw.([]string) {
		for _, tag := range state.Tags {
			if tag == expected {
				return true
			}
		}
	}
	return false
}

func activityAt(_ session.Session, state *session.SessionState, raw any) bool {
	return stateInterval(*state).Contains(raw.(time.Time))
}

func activityOverlaps(_ session.Session, state *session.SessionState, raw any) bool {
	return stateInterval(*state).Overlaps(asTimeRange(raw.(sharedquery.IntervalValue)))
}

func compareLogin(accept func(int) bool) predicate {
	return func(value session.Session, _ *session.SessionState, raw any) bool {
		return accept(value.LoginAt.Compare(raw.(time.Time)))
	}
}

func loginBetween(value session.Session, _ *session.SessionState, raw any) bool {
	interval := asTimeRange(raw.(sharedquery.IntervalValue))
	if interval.From != nil && value.LoginAt.Before(*interval.From) {
		return false
	}
	return interval.To == nil || value.LoginAt.Before(*interval.To)
}

func stateInterval(value session.SessionState) session.Interval {
	return session.Interval{From: value.ValidFrom, To: value.ValidTo}
}

func asTimeRange(value sharedquery.IntervalValue) session.TimeRange {
	return session.TimeRange{From: value.From, To: value.To}
}
