package session

import "time"

type Field string

type Operator string

type Filter struct {
	Field    Field
	Operator Operator
	Value    any
}

type PageRequest struct {
	Limit  int
	Cursor string
}

type QuerySpec struct {
	Filters     []Filter
	Page        PageRequest
	EvaluatedAt time.Time
}

type QueryResult struct {
	Sessions   []Session
	NextCursor string
}
