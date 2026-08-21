package query

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/session"
)

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
		{Field: "tenantId", Operator: "eq", Value: "tenant-a"},
		{Field: "ip", Operator: "eq", Value: "2001:0db8:0:0:0:0:0:1"},
		{Field: "tags", Operator: "containsAll", Value: []any{"user", "admin", "user"}},
		{Field: "activity", Operator: "at", Value: "2026-08-21T10:00:00+03:00"},
		{Field: "activity", Operator: "overlaps", Value: map[string]any{"from": "2026-08-21T10:00:00Z", "to": "2026-08-21T12:00:00Z"}},
	})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	if resolved[0].Handler != "tenant-eq" || resolved[0].Scope != ScopeSession || resolved[0].Value != "tenant-a" {
		t.Fatalf("tenant filter = %+v", resolved[0])
	}
	if resolved[1].Value != "2001:db8::1" {
		t.Fatalf("normalized IP = %v", resolved[1].Value)
	}
	if !reflect.DeepEqual(resolved[2].Value, []string{"admin", "user"}) {
		t.Fatalf("normalized tags = %v", resolved[2].Value)
	}
	wantPoint := time.Date(2026, 8, 21, 7, 0, 0, 0, time.UTC)
	if got := resolved[3].Value.(time.Time); !got.Equal(wantPoint) || got.Location() != time.UTC {
		t.Fatalf("normalized timestamp = %v, want %v in UTC", got, wantPoint)
	}
	interval := resolved[4].Value.(IntervalValue)
	if interval.From == nil || interval.To == nil || !interval.From.Equal(mustTime(t, "2026-08-21T10:00:00Z")) || !interval.To.Equal(mustTime(t, "2026-08-21T12:00:00Z")) {
		t.Fatalf("normalized interval = %+v", interval)
	}
}

func TestRegistryRejectsInvalidDefinitionsAndFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		entries []Entry[string]
	}{
		{name: "empty field", entries: []Entry[string]{{Operator: "eq", Scope: ScopeSession, ValueKind: ValueString}}},
		{name: "empty operator", entries: []Entry[string]{{Field: "tenantId", Scope: ScopeSession, ValueKind: ValueString}}},
		{name: "invalid scope", entries: []Entry[string]{{Field: "tenantId", Operator: "eq", Scope: Scope("other"), ValueKind: ValueString}}},
		{name: "invalid value kind", entries: []Entry[string]{{Field: "tenantId", Operator: "eq", Scope: ScopeSession, ValueKind: ValueKind("other")}}},
		{name: "duplicate", entries: []Entry[string]{
			{Field: "tenantId", Operator: "eq", Scope: ScopeSession, ValueKind: ValueString},
			{Field: "tenantId", Operator: "eq", Scope: ScopeSession, ValueKind: ValueString},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewRegistry(tt.entries); !errors.Is(err, repository.ErrInvalidQuery) {
				t.Fatalf("NewRegistry() error = %v, want ErrInvalidQuery", err)
			}
		})
	}

	registry := mustRegistry(t, []Entry[string]{{Field: "tenantId", Operator: "eq", Scope: ScopeSession, ValueKind: ValueString}})
	filters := []session.Filter{
		{Field: "unknown", Operator: "eq", Value: "value"},
		{Field: "tenantId", Operator: "in", Value: []string{"tenant-a"}},
		{Field: "tenantId", Operator: "eq", Value: ""},
	}
	for _, filter := range filters {
		if _, err := registry.Resolve([]session.Filter{filter}); !errors.Is(err, repository.ErrInvalidQuery) {
			t.Fatalf("Resolve(%+v) error = %v, want ErrInvalidQuery", filter, err)
		}
	}
}

func TestIntervalValueUsesTheSameTimestampValidationAsMapInput(t *testing.T) {
	t.Parallel()

	registry := mustRegistry(t, []Entry[string]{
		{Field: "activity", Operator: "overlaps", Scope: ScopeState, ValueKind: ValueInterval},
	})
	zero := time.Time{}
	from := mustTime(t, "2026-08-21T10:00:00+03:00")
	to := mustTime(t, "2026-08-21T12:00:00+03:00")

	tests := []struct {
		name      string
		value     IntervalValue
		wantError bool
		wantFrom  *time.Time
		wantTo    *time.Time
	}{
		{name: "zero from", value: IntervalValue{From: &zero}, wantError: true},
		{name: "zero to", value: IntervalValue{To: &zero}, wantError: true},
		{name: "open ended from", value: IntervalValue{From: &from}, wantFrom: timePointer(from.UTC())},
		{name: "open ended to", value: IntervalValue{To: &to}, wantTo: timePointer(to.UTC())},
		{name: "equal", value: IntervalValue{From: &from, To: &from}, wantError: true},
		{name: "reversed", value: IntervalValue{From: &to, To: &from}, wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resolved, err := registry.Resolve([]session.Filter{{
				Field: "activity", Operator: "overlaps", Value: tt.value,
			}})
			if tt.wantError {
				if !errors.Is(err, repository.ErrInvalidQuery) {
					t.Fatalf("Resolve() error = %v, want ErrInvalidQuery", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			got := resolved[0].Value.(IntervalValue)
			if !equalOptionalTime(got.From, tt.wantFrom) || !equalOptionalTime(got.To, tt.wantTo) {
				t.Fatalf("normalized interval = %+v, want from=%v to=%v", got, tt.wantFrom, tt.wantTo)
			}
		})
	}
}

func TestPrepareAppliesLimitsAndStableFingerprint(t *testing.T) {
	t.Parallel()

	registry := mustRegistry(t, []Entry[string]{
		{Field: "tenantId", Operator: "eq", Scope: ScopeSession, ValueKind: ValueString, Handler: "tenant"},
		{Field: "tags", Operator: "containsAll", Scope: ScopeState, ValueKind: ValueStringList, Handler: "tags"},
	})
	evaluatedAt := mustTime(t, "2026-08-21T10:00:00Z")
	firstSpec := session.QuerySpec{
		Filters: []session.Filter{
			{Field: "tenantId", Operator: "eq", Value: "tenant-a"},
			{Field: "tags", Operator: "containsAll", Value: []string{"user", "admin"}},
		},
		EvaluatedAt: evaluatedAt,
	}
	secondSpec := session.QuerySpec{
		Filters: []session.Filter{
			{Field: "tags", Operator: "containsAll", Value: []string{"admin", "user"}},
			{Field: "tenantId", Operator: "eq", Value: "tenant-a"},
		},
		Page:        session.PageRequest{Limit: MaxLimit},
		EvaluatedAt: evaluatedAt,
	}

	first, err := Prepare(firstSpec, "memory", registry)
	if err != nil {
		t.Fatalf("first Prepare() error = %v", err)
	}
	second, err := Prepare(secondSpec, "memory", registry)
	if err != nil {
		t.Fatalf("second Prepare() error = %v", err)
	}
	if first.Limit != DefaultLimit {
		t.Fatalf("default limit = %d, want %d", first.Limit, DefaultLimit)
	}
	if second.Limit != MaxLimit {
		t.Fatalf("maximum limit = %d, want %d", second.Limit, MaxLimit)
	}
	if first.Fingerprint != second.Fingerprint {
		t.Fatalf("fingerprints differ for equivalent filters: %q != %q", first.Fingerprint, second.Fingerprint)
	}

	for _, limit := range []int{-1, MaxLimit + 1} {
		_, err := Prepare(session.QuerySpec{Page: session.PageRequest{Limit: limit}, EvaluatedAt: evaluatedAt}, "memory", registry)
		if !errors.Is(err, repository.ErrInvalidQuery) {
			t.Fatalf("Prepare(limit=%d) error = %v, want ErrInvalidQuery", limit, err)
		}
	}
}

func TestPrepareRestoresCursorEvaluationTime(t *testing.T) {
	t.Parallel()

	registry := mustRegistry(t, []Entry[string]{{Field: "tenantId", Operator: "eq", Scope: ScopeSession, ValueKind: ValueString}})
	filters := []session.Filter{{Field: "tenantId", Operator: "eq", Value: "tenant-a"}}
	firstEvaluation := mustTime(t, "2026-08-21T10:00:00Z")
	first, err := Prepare(session.QuerySpec{Filters: filters, EvaluatedAt: firstEvaluation}, "memory", registry)
	if err != nil {
		t.Fatalf("first Prepare() error = %v", err)
	}
	cursor, err := EncodeCursor(Cursor{
		Storage:     "memory",
		Fingerprint: first.Fingerprint,
		EvaluatedAt: firstEvaluation,
		After: SortKey{
			TenantID:  "tenant-a",
			Username:  "alice",
			IP:        "192.0.2.10",
			LoginAt:   firstEvaluation,
			SessionID: "11111111-1111-4111-8111-111111111111",
		},
	})
	if err != nil {
		t.Fatalf("EncodeCursor() error = %v", err)
	}

	next, err := Prepare(session.QuerySpec{
		Filters:     filters,
		Page:        session.PageRequest{Cursor: cursor},
		EvaluatedAt: firstEvaluation.Add(time.Hour),
	}, "memory", registry)
	if err != nil {
		t.Fatalf("next Prepare() error = %v", err)
	}
	if !next.EvaluatedAt.Equal(firstEvaluation) {
		t.Fatalf("next EvaluatedAt = %v, want cursor value %v", next.EvaluatedAt, firstEvaluation)
	}
	if next.After == nil || next.After.SessionID != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf("next After = %+v", next.After)
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

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("time.Parse(%q) error = %v", value, err)
	}
	return parsed
}

func timePointer(value time.Time) *time.Time {
	return &value
}

func equalOptionalTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Equal(*right) && left.Location() == right.Location()
}
