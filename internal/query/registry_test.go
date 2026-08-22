package query

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

// A registered filter is resolved to its handler and scope, and its value is
// normalized: IPs canonicalized, tag lists deduplicated and sorted, timestamps
// and intervals converted to UTC. Interval values are validated the same way
// whether they arrive as a typed value or as decoded JSON.
func TestRegistryResolvesAndNormalizesFilters(t *testing.T) {
	t.Parallel()

	registry := mustRegistry(t, []Entry[string]{
		{Field: "tenantId", Operator: "eq", Scope: ScopeSession, ValueKind: ValueString, Handler: "tenant-eq"},
		{Field: "ip", Operator: "eq", Scope: ScopeSession, ValueKind: ValueIP, Handler: "ip-eq"},
		{Field: "tags", Operator: "containsAll", Scope: ScopeState, ValueKind: ValueStringList, Handler: "tags-all"},
		{Field: "activity", Operator: "at", Scope: ScopeState, ValueKind: ValueTimestamp, Handler: "activity-at"},
		{Field: "activity", Operator: "overlaps", Scope: ScopeState, ValueKind: ValueInterval, Handler: "activity-overlaps"},
	})

	resolved, err := registry.Resolve([]session.Filter{
		sessiontest.Filter("tenantId", "eq", "tenant-a"),
		sessiontest.Filter("ip", "eq", "2001:0db8:0:0:0:0:0:1"),
		sessiontest.Filter("tags", "containsAll", []any{"user", "admin", "user"}),
		sessiontest.Filter("activity", "at", "2026-08-21T10:00:00+03:00"),
		sessiontest.Filter("activity", "overlaps", map[string]any{"from": "2026-08-21T10:00:00Z", "to": "2026-08-21T12:00:00Z"}),
	})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	if resolved[0].Handler != "tenant-eq" || resolved[0].Scope != ScopeSession || resolved[0].Value != "tenant-a" {
		t.Errorf("tenant filter = %+v, want handler tenant-eq in session scope with value tenant-a", resolved[0])
	}
	if resolved[1].Value != "2001:db8::1" {
		t.Errorf("normalized IP = %v, want canonical 2001:db8::1", resolved[1].Value)
	}
	if want := []string{"admin", "user"}; !reflect.DeepEqual(resolved[2].Value, want) {
		t.Errorf("normalized tags = %v, want deduplicated and sorted %v", resolved[2].Value, want)
	}

	// A zoned timestamp is converted to UTC: 10:00+03:00 is 07:00Z.
	point, ok := resolved[3].Value.(time.Time)
	if !ok || !point.Equal(sessiontest.At("07:00")) || point.Location() != time.UTC {
		t.Errorf("normalized timestamp = %#v, want 07:00Z", resolved[3].Value)
	}
	interval, ok := resolved[4].Value.(IntervalValue)
	if !ok {
		t.Fatalf("normalized interval = %#v, want an IntervalValue", resolved[4].Value)
	}
	if !equalOptionalTime(interval.From, sessiontest.Ptr(sessiontest.At("10:00"))) ||
		!equalOptionalTime(interval.To, sessiontest.Ptr(sessiontest.At("12:00"))) {
		t.Errorf("normalized interval = %+v, want 10:00Z to 12:00Z", interval)
	}

	// The same rules apply to an interval supplied as a typed value. Its bounds
	// carry a zone offset, so this also covers conversion to UTC.
	zone := time.FixedZone("UTC+3", 3*60*60)
	from, to := sessiontest.At("07:00").In(zone), sessiontest.At("09:00").In(zone)
	zero := time.Time{}
	intervals := []struct {
		name      string
		value     IntervalValue
		wantError bool
		wantFrom  *time.Time
		wantTo    *time.Time
	}{
		{name: "zero from", value: IntervalValue{From: &zero}, wantError: true},
		{name: "zero to", value: IntervalValue{To: &zero}, wantError: true},
		{name: "equal bounds", value: IntervalValue{From: &from, To: &from}, wantError: true},
		{name: "reversed bounds", value: IntervalValue{From: &to, To: &from}, wantError: true},
		{name: "open ended from", value: IntervalValue{From: &from}, wantFrom: sessiontest.Ptr(sessiontest.At("07:00"))},
		{name: "open ended to", value: IntervalValue{To: &to}, wantTo: sessiontest.Ptr(sessiontest.At("09:00"))},
	}
	for _, test := range intervals {
		got, err := registry.Resolve([]session.Filter{sessiontest.Filter("activity", "overlaps", test.value)})
		if test.wantError {
			if !errors.Is(err, repository.ErrInvalidQuery) {
				t.Errorf("%s: Resolve() error = %v, want ErrInvalidQuery", test.name, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: Resolve() error = %v", test.name, err)
			continue
		}
		value := got[0].Value.(IntervalValue)
		if !equalOptionalTime(value.From, test.wantFrom) || !equalOptionalTime(value.To, test.wantTo) {
			t.Errorf("%s: normalized interval = %+v, want from %v to %v", test.name, value, test.wantFrom, test.wantTo)
		}
	}
}

// A registry refuses definitions it cannot serve, and refuses filters naming a
// field or operator it does not carry.
func TestRegistryRejectsInvalidDefinitionsAndFilters(t *testing.T) {
	t.Parallel()

	definitions := []struct {
		name    string
		entries []Entry[string]
	}{
		{name: "empty field", entries: []Entry[string]{{Operator: "eq", Scope: ScopeSession, ValueKind: ValueString}}},
		{name: "empty operator", entries: []Entry[string]{{Field: "tenantId", Scope: ScopeSession, ValueKind: ValueString}}},
		{name: "invalid scope", entries: []Entry[string]{{Field: "tenantId", Operator: "eq", Scope: Scope("other"), ValueKind: ValueString}}},
		{name: "invalid value kind", entries: []Entry[string]{{Field: "tenantId", Operator: "eq", Scope: ScopeSession, ValueKind: ValueKind("other")}}},
		{name: "duplicate entry", entries: []Entry[string]{
			{Field: "tenantId", Operator: "eq", Scope: ScopeSession, ValueKind: ValueString},
			{Field: "tenantId", Operator: "eq", Scope: ScopeSession, ValueKind: ValueString},
		}},
	}
	for _, test := range definitions {
		if _, err := NewRegistry(test.entries); !errors.Is(err, repository.ErrInvalidQuery) {
			t.Errorf("%s: NewRegistry() error = %v, want ErrInvalidQuery", test.name, err)
		}
	}

	registry := mustRegistry(t, []Entry[string]{
		{Field: "tenantId", Operator: "eq", Scope: ScopeSession, ValueKind: ValueString},
	})
	filters := []struct {
		name   string
		filter session.Filter
	}{
		{name: "unregistered field", filter: sessiontest.Filter("unknown", "eq", "value")},
		{name: "unregistered operator", filter: sessiontest.Filter("tenantId", "in", []string{"tenant-a"})},
		{name: "empty value", filter: sessiontest.Filter("tenantId", "eq", "")},
	}
	for _, test := range filters {
		if _, err := registry.Resolve([]session.Filter{test.filter}); !errors.Is(err, repository.ErrInvalidQuery) {
			t.Errorf("%s: Resolve() error = %v, want ErrInvalidQuery", test.name, err)
		}
	}
}

// Preparing a query applies the default and maximum page limits, fingerprints
// equivalent queries identically, and restores the evaluation time a cursor
// carries so paging cannot drift as the clock advances.
func TestPrepareLimitsFingerprintAndCursor(t *testing.T) {
	t.Parallel()

	registry := mustRegistry(t, []Entry[string]{
		{Field: "tenantId", Operator: "eq", Scope: ScopeSession, ValueKind: ValueString, Handler: "tenant"},
		{Field: "tags", Operator: "containsAll", Scope: ScopeState, ValueKind: ValueStringList, Handler: "tags"},
	})
	evaluatedAt := sessiontest.At("10:00")
	tenant := sessiontest.Filter("tenantId", "eq", "tenant-a")

	// The same query written in a different order, with tags listed in a
	// different order, must fingerprint identically.
	first, err := Prepare(session.QuerySpec{
		Filters:     []session.Filter{tenant, sessiontest.Filter("tags", "containsAll", []string{"user", "admin"})},
		EvaluatedAt: evaluatedAt,
	}, "memory", registry)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	second, err := Prepare(session.QuerySpec{
		Filters:     []session.Filter{sessiontest.Filter("tags", "containsAll", []string{"admin", "user"}), tenant},
		Page:        session.PageRequest{Limit: MaxLimit},
		EvaluatedAt: evaluatedAt,
	}, "memory", registry)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if first.Limit != DefaultLimit {
		t.Errorf("limit for an unset page = %d, want the default %d", first.Limit, DefaultLimit)
	}
	if second.Limit != MaxLimit {
		t.Errorf("limit = %d, want the requested maximum %d", second.Limit, MaxLimit)
	}
	if first.Fingerprint != second.Fingerprint {
		t.Errorf("fingerprints differ for equivalent queries: %q and %q", first.Fingerprint, second.Fingerprint)
	}

	for _, limit := range []int{-1, MaxLimit + 1} {
		spec := session.QuerySpec{Page: session.PageRequest{Limit: limit}, EvaluatedAt: evaluatedAt}
		if _, err := Prepare(spec, "memory", registry); !errors.Is(err, repository.ErrInvalidQuery) {
			t.Errorf("Prepare(limit=%d) error = %v, want ErrInvalidQuery", limit, err)
		}
	}

	after := SortKey{
		TenantID: "tenant-a", Username: "alice", IP: "192.0.2.10",
		LoginAt: evaluatedAt, SessionID: sessiontest.SessionID(1),
	}
	cursor, err := EncodeCursor(Cursor{
		Storage: "memory", Fingerprint: first.Fingerprint, EvaluatedAt: evaluatedAt, After: after,
	})
	if err != nil {
		t.Fatalf("EncodeCursor() error = %v", err)
	}
	next, err := Prepare(session.QuerySpec{
		Filters:     []session.Filter{tenant, sessiontest.Filter("tags", "containsAll", []string{"user", "admin"})},
		Page:        session.PageRequest{Cursor: cursor},
		EvaluatedAt: evaluatedAt.Add(time.Hour),
	}, "memory", registry)
	if err != nil {
		t.Fatalf("Prepare() with a cursor error = %v", err)
	}
	if !next.EvaluatedAt.Equal(evaluatedAt) {
		t.Errorf("EvaluatedAt = %v, want the cursor's %v", next.EvaluatedAt, evaluatedAt)
	}
	if next.After == nil || *next.After != after {
		t.Errorf("After = %+v, want %+v", next.After, after)
	}
}

func mustRegistry[Handler any](t *testing.T, entries []Entry[Handler]) *Registry[Handler] {
	t.Helper()
	registry, err := NewRegistry(entries)
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	return registry
}

func equalOptionalTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Equal(*right) && left.Location() == right.Location()
}
