package session_test

import (
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/session"
	"github.com/Bennybl/session-handler/internal/sessiontest"
)

func TestIntervalContainsUsesHalfOpenBounds(t *testing.T) {
	t.Parallel()

	from, to := sessiontest.At("10:00"), sessiontest.At("11:00")
	interval := session.Interval{From: from, To: &to}

	tests := []struct {
		name string
		at   time.Time
		want bool
	}{
		{name: "before lower bound", at: from.Add(-time.Nanosecond), want: false},
		{name: "at lower bound", at: from, want: true},
		{name: "before upper bound", at: to.Add(-time.Nanosecond), want: true},
		{name: "at upper bound", at: to, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := interval.Contains(test.at); got != test.want {
				t.Fatalf("Contains(%v) = %v, want %v", test.at, got, test.want)
			}
		})
	}
}

func TestIntervalOverlapUsesHalfOpenBoundsAndSupportsOpenRanges(t *testing.T) {
	t.Parallel()

	ten, eleven, twelve := sessiontest.At("10:00"), sessiontest.At("11:00"), sessiontest.At("12:00")
	interval := session.Interval{From: ten, To: &eleven}

	tests := []struct {
		name  string
		query session.TimeRange
		want  bool
	}{
		{name: "overlapping range", query: timeRange(ten.Add(30*time.Minute), twelve), want: true},
		{name: "starting at the upper bound", query: timeRange(eleven, twelve), want: false},
		{name: "open before", query: session.TimeRange{To: sessiontest.Ptr(ten.Add(time.Minute))}, want: true},
		{name: "open after", query: session.TimeRange{From: sessiontest.Ptr(ten.Add(time.Minute))}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := interval.Overlaps(test.query); got != test.want {
				t.Fatalf("Overlaps(%+v) = %v, want %v", test.query, got, test.want)
			}
		})
	}
}

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
