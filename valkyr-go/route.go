package valkyr

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Namespace struct {
	client *Client
	name   string
}
type Route struct {
	client         *Client
	namespace, key string
}

func (n *Namespace) Key(key string) *Route {
	return &Route{client: n.client, namespace: n.name, key: key}
}
func (n *Namespace) SetMany(ctx context.Context, values map[string]any, ttl ...time.Duration) error {
	if n.name == "" {
		return fmt.Errorf("%w: namespace is required", ErrRoute)
	}
	entries := make([]wireEntry, 0, len(values))
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := values[key]
		if key == "" {
			return fmt.Errorf("%w: key is required", ErrRoute)
		}
		raw, e := encodeValue(value)
		if e != nil {
			return e
		}
		entries = append(entries, wireEntry{key, raw})
	}
	seconds, e := ttlValue(ttl)
	if e != nil {
		return e
	}
	r, e := n.client.do(ctx, wireCommand{Type: "set_batch", Namespace: n.name, Entries: entries, TTLSec: seconds})
	if e != nil {
		return e
	}
	return expectOK(r)
}
func (n *Namespace) Delete(ctx context.Context, pattern *string) error {
	if n.name == "" {
		return fmt.Errorf("%w: namespace is required", ErrRoute)
	}
	r, e := n.client.do(ctx, wireCommand{Type: "delete", Namespace: n.name, KeyPattern: pattern})
	if e != nil {
		return e
	}
	return expectOK(r)
}
func (n *Namespace) Ping(ctx context.Context) error           { return n.client.Ping(ctx) }
func (n *Namespace) Stats(ctx context.Context) (Stats, error) { return n.client.Stats(ctx) }
func (r *Route) Get(ctx context.Context) (Result, error) {
	if r.namespace == "" || r.key == "" {
		return nil, fmt.Errorf("%w: namespace and key are required", ErrRoute)
	}
	resp, e := r.client.do(ctx, wireCommand{Type: "get", Namespace: r.namespace, Key: r.key})
	if e != nil {
		return nil, e
	}
	return resultFrom(resp)
}
func (r *Route) GetWithRetry(ctx context.Context) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	first, e := r.Get(ctx)
	if e != nil {
		return nil, e
	}
	m, ok := first.(Miss)
	if !ok {
		return first, nil
	}
	if m.RetryAfter > 0 {
		timer := time.NewTimer(m.RetryAfter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return r.Get(ctx)
}
func (r *Route) Set(ctx context.Context, value any, ttl ...time.Duration) error {
	if r.namespace == "" || r.key == "" {
		return fmt.Errorf("%w: namespace and key are required", ErrRoute)
	}
	raw, e := encodeValue(value)
	if e != nil {
		return e
	}
	seconds, e := ttlValue(ttl)
	if e != nil {
		return e
	}
	resp, e := r.client.do(ctx, wireCommand{Type: "set", Namespace: r.namespace, Key: r.key, Value: raw, TTLSec: seconds})
	if e != nil {
		return e
	}
	return expectOK(resp)
}
func (r *Route) Delete(ctx context.Context) error {
	if r.key == "" {
		return fmt.Errorf("%w: key is required", ErrRoute)
	}
	return (&Namespace{client: r.client, name: r.namespace}).Delete(ctx, &r.key)
}
func (r *Route) Move(ctx context.Context, toContext string) error {
	base, sourceContext, ok := strings.Cut(r.namespace, "::")
	if !ok || base == "" || sourceContext == "" || toContext == "" || strings.Contains(toContext, "::") {
		return fmt.Errorf("%w: move requires a namespace::context route", ErrRoute)
	}
	destination := base + "::" + toContext
	if b, _, ok := strings.Cut(destination, "::"); !ok || b != base {
		return fmt.Errorf("%w: move must preserve base namespace", ErrRoute)
	}
	resp, e := r.client.do(ctx, wireCommand{Type: "move", Source: r.namespace, Destination: destination})
	if e != nil {
		return e
	}
	return expectOK(resp)
}
func ttlValue(ttl []time.Duration) (*uint64, error) {
	if len(ttl) > 1 {
		return nil, fmt.Errorf("%w: only one TTL is allowed", ErrRoute)
	}
	if len(ttl) == 0 {
		return nil, nil
	}
	return durationSeconds(&ttl[0])
}
func expectOK(r wireResponse) error {
	if r.Type == "ok" {
		return nil
	}
	if r.Type == "error" {
		return &ServerError{Message: r.Message}
	}
	return unexpected("ok", r.Type)
}
func resultFrom(r wireResponse) (Result, error) {
	switch r.Type {
	case "value":
		ttl := durationPtr(r.TTL)
		return Value{Raw: append(json.RawMessage(nil), r.Value...), TTL: ttl}, nil
	case "miss":
		return Miss{RetryAfter: durationMillis(*r.RetryAfter)}, nil
	case "unknown":
		return Unknown{}, nil
	case "error":
		return nil, &ServerError{Message: r.Message}
	default:
		return nil, unexpected("value/miss/unknown", r.Type)
	}
}
func durationPtr(s *uint64) *time.Duration {
	if s == nil {
		return nil
	}
	maxSeconds := uint64((time.Duration(1<<63 - 1)) / time.Second)
	seconds := *s
	if seconds > maxSeconds {
		seconds = maxSeconds
	}
	d := time.Duration(seconds) * time.Second
	return &d
}
