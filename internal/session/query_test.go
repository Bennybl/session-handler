package session_test

import (
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

// Intervals are half-open: they include their lower bound and exclude their
// upper one, both when testing a point and when overlapping an open-ended range.
func TestIntervalHalfOpenBoundsAndOverlap(t *testing.T) {
	t.Parallel()

	ten, eleven, twelve := sessiontest.At("10:00"), sessiontest.At("11:00"), sessiontest.At("12:00")
	interval := session.Interval{From: ten, To: &eleven}

	contains := []struct {
		name string
		at   time.Time
		want bool
	}{
		{name: "before the lower bound", at: ten.Add(-time.Nanosecond), want: false},
		{name: "at the lower bound", at: ten, want: true},
		{name: "before the upper bound", at: eleven.Add(-time.Nanosecond), want: true},
		{name: "at the upper bound", at: eleven, want: false},
	}
	for _, test := range contains {
		if got := interval.Contains(test.at); got != test.want {
			t.Errorf("%s: Contains(%v) = %v, want %v", test.name, test.at, got, test.want)
		}
	}

	overlaps := []struct {
		name  string
		query session.TimeRange
		want  bool
	}{
		{name: "overlapping range", query: timeRange(ten.Add(30*time.Minute), twelve), want: true},
		{name: "range starting at the upper bound", query: timeRange(eleven, twelve), want: false},
		{name: "range open before", query: session.TimeRange{To: sessiontest.Ptr(ten.Add(time.Minute))}, want: true},
		{name: "range open after", query: session.TimeRange{From: sessiontest.Ptr(ten.Add(time.Minute))}, want: true},
	}
	for _, test := range overlaps {
		if got := interval.Overlaps(test.query); got != test.want {
			t.Errorf("%s: Overlaps(%+v) = %v, want %v", test.name, test.query, got, test.want)
		}
	}
}

// A query is filters plus a page, carries the instant it is evaluated at, and
// answers with sessions and an optional cursor.
func TestGenericQueryAndPaginationTypes(t *testing.T) {
	t.Parallel()

	evaluatedAt := sessiontest.At("10:00")
	page := session.PageRequest{Limit: 25, Cursor: "cursor"}
	spec := session.QuerySpec{
		Filters: []session.Filter{
			sessiontest.Filter("tenantId", "eq", "tenant-a"),
			sessiontest.Filter("tags", "containsAll", []string{"admin"}),
		},
		Page:        page,
		EvaluatedAt: evaluatedAt,
	}

	if len(spec.Filters) != 2 {
		t.Errorf("filters = %d, want 2", len(spec.Filters))
	}
	if spec.Page != page {
		t.Errorf("page = %+v, want %+v", spec.Page, page)
	}
	if !spec.EvaluatedAt.Equal(evaluatedAt) {
		t.Errorf("EvaluatedAt = %v, want %v", spec.EvaluatedAt, evaluatedAt)
	}

	result := session.QueryResult{
		Sessions:   []session.Session{{ID: sessiontest.SessionID(1)}},
		NextCursor: "next",
	}
	if len(result.Sessions) != 1 || result.NextCursor != "next" {
		t.Errorf("result = %+v, want one session and cursor %q", result, "next")
	}
}

func timeRange(from, to time.Time) session.TimeRange {
	return session.TimeRange{From: &from, To: &to}
}
