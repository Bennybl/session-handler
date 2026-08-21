package session

import "errors"

var (
	ErrInvalidInput      = errors.New("invalid input")
	ErrInvalidTransition = errors.New("invalid transition")
	ErrStaleEvent        = errors.New("stale event")
)
