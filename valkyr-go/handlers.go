package valkyr

import (
	"context"
	"encoding/json"
	"time"
)

// ProviderValue is a provider result with an optional whole-second cache TTL.
// A nil Value is a provider miss and does not carry a TTL.
type ProviderValue struct {
	Value any
	TTL   *time.Duration
}

type Provider interface {
	Get(context.Context, string, string) (any, error)
}
type Store interface {
	Set(context.Context, string, string, json.RawMessage, *time.Duration) error
	SetMany(context.Context, string, []Entry, *time.Duration) error
	Delete(context.Context, string, *string) error
	Move(context.Context, string, string) error
}
type Entry struct {
	Key   string
	Value json.RawMessage
}
