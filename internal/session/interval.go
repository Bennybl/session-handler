package session

import "time"

type Interval struct {
	From time.Time
	To   *time.Time
}

func (i Interval) Contains(at time.Time) bool {
	if at.Before(i.From) {
		return false
	}
	return i.To == nil || at.Before(*i.To)
}

func (i Interval) Overlaps(query TimeRange) bool {
	if i.To != nil && !i.From.Before(*i.To) {
		return false
	}
	if query.From != nil && query.To != nil && !query.From.Before(*query.To) {
		return false
	}
	if i.To != nil && query.From != nil && !query.From.Before(*i.To) {
		return false
	}
	if query.To != nil && !i.From.Before(*query.To) {
		return false
	}
	return true
}

type TimeRange struct {
	From *time.Time
	To   *time.Time
}
