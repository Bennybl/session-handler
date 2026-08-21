package session

import (
	"testing"
	"time"
)

func TestIntervalContainsUsesHalfOpenBounds(t *testing.T) {
	t.Parallel()

	from := mustTime(t, "2026-08-21T10:00:00Z")
	to := mustTime(t, "2026-08-21T11:00:00Z")
	interval := Interval{From: from, To: &to}

	if interval.Contains(from.Add(-time.Nanosecond)) {
		t.Fatal("interval contains a value before its lower bound")
	}
	if !interval.Contains(from) {
		t.Fatal("interval must include its lower bound")
	}
	if !interval.Contains(to.Add(-time.Nanosecond)) {
		t.Fatal("interval must contain a value before its upper bound")
	}
	if interval.Contains(to) {
		t.Fatal("interval must exclude its upper bound")
	}
}

func TestIntervalOverlapUsesHalfOpenBoundsAndSupportsOpenRanges(t *testing.T) {
	t.Parallel()

	ten := mustTime(t, "2026-08-21T10:00:00Z")
	eleven := mustTime(t, "2026-08-21T11:00:00Z")
	twelve := mustTime(t, "2026-08-21T12:00:00Z")
	interval := Interval{From: ten, To: &eleven}

	tests := []struct {
		name       string
		rangeValue TimeRange
		want       bool
	}{
		{name: "overlap", rangeValue: TimeRange{From: timePointer(ten.Add(30 * time.Minute)), To: &twelve}, want: true},
		{name: "touching upper bound", rangeValue: TimeRange{From: &eleven, To: &twelve}, want: false},
		{name: "open before", rangeValue: TimeRange{To: timePointer(ten.Add(time.Minute))}, want: true},
		{name: "open after", rangeValue: TimeRange{From: timePointer(ten.Add(time.Minute))}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := interval.Overlaps(tt.rangeValue); got != tt.want {
				t.Fatalf("Overlaps() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGenericQueryAndPaginationTypes(t *testing.T) {
	t.Parallel()

	evaluatedAt := mustTime(t, "2026-08-21T10:00:00Z")
	spec := QuerySpec{
		Filters: []Filter{
			{Field: Field("tenantId"), Operator: Operator("eq"), Value: "tenant-a"},
			{Field: Field("tags"), Operator: Operator("containsAll"), Value: []string{"admin"}},
		},
		Page:        PageRequest{Limit: 25, Cursor: "cursor"},
		EvaluatedAt: evaluatedAt,
	}

	if len(spec.Filters) != 2 || spec.Page.Limit != 25 || spec.Page.Cursor != "cursor" {
		t.Fatalf("unexpected query spec: %+v", spec)
	}
	if !spec.EvaluatedAt.Equal(evaluatedAt) {
		t.Fatalf("EvaluatedAt = %v, want %v", spec.EvaluatedAt, evaluatedAt)
	}

	result := QueryResult{Sessions: []Session{{ID: "session-1"}}, NextCursor: "next"}
	if len(result.Sessions) != 1 || result.NextCursor != "next" {
		t.Fatalf("unexpected query result: %+v", result)
	}
}
