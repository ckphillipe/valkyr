package valkyr

import (
	"errors"
	"fmt"
)

var (
	ErrProtocol   = errors.New("valkyr protocol error")
	ErrConnection = errors.New("valkyr connection error")
	ErrTimeout    = errors.New("valkyr timeout")
	ErrAuth       = errors.New("valkyr authentication error")
	ErrOverload   = errors.New("valkyr adapter overloaded")
	ErrRoute      = errors.New("valkyr route error")
	ErrServer     = errors.New("valkyr server error")
)

type Error struct {
	Category   error
	Message    string
	RetryAfter timeDuration
}

// timeDuration keeps errors.go independent of the public time-based API while
// still allowing callers to inspect retry metadata through RetryDelay.
type timeDuration int64

func (e *Error) Error() string     { return e.Message }
func (e *Error) Unwrap() error     { return e.Category }
func (e *Error) RetryDelay() int64 { return int64(e.RetryAfter) }

func newError(category error, message string) error {
	return &Error{Category: category, Message: message}
}

type ServerError struct{ Message string }

func (e *ServerError) Error() string { return e.Message }
func (e *ServerError) Unwrap() error { return ErrServer }

type AuthPendingError struct{ RetryAfterMS uint64 }

func (e *AuthPendingError) Error() string {
	return fmt.Sprintf("authentication pending (retry after %dms)", e.RetryAfterMS)
}
func (e *AuthPendingError) Unwrap() error { return ErrAuth }

type AuthRejectedError struct{ Message string }

func (e *AuthRejectedError) Error() string { return e.Message }
func (e *AuthRejectedError) Unwrap() error { return ErrAuth }

type OverloadError struct{ Message string }

func (e *OverloadError) Error() string { return e.Message }
func (e *OverloadError) Unwrap() error { return ErrOverload }
