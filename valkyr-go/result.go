package valkyr

import (
	"encoding/json"
	"time"
)

// Result is a read outcome. The unexported method intentionally limits the
// set of implementations to this package while retaining idiomatic type
// switches for callers.
type Result interface{ result() }

type Value struct {
	Raw json.RawMessage
	TTL *time.Duration
}

func (Value) result()                       {}
func (v Value) Decode(target any) error     { return json.Unmarshal(v.Raw, target) }
func (v Value) Bytes() []byte               { return append([]byte(nil), v.Raw...) }
func (v Value) TTLDuration() *time.Duration { return v.TTL }

type Miss struct{ RetryAfter time.Duration }

func (Miss) result() {}

type Unknown struct{}

func (Unknown) result() {}
