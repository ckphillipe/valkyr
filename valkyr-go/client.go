package valkyr

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Client struct {
	transport      *transport
	apiKey         string
	requestTimeout time.Duration
	closeOnce      bool
}

func Dial(ctx context.Context, address string, options ...Option) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	o := defaultOptions()
	for _, option := range options {
		if err := option(&o); err != nil {
			return nil, err
		}
	}
	t, err := openTransport(ctx, address, o)
	if err != nil {
		return nil, err
	}
	c := &Client{transport: t, apiKey: o.APIKey, requestTimeout: o.RequestTimeout}
	if o.APIKey != "" {
		if _, err = authenticate(ctx, t, o.APIKey, nil, o.AuthTimeout); err != nil {
			t.close()
			return nil, err
		}
	}
	return c, nil
}
func (c *Client) Close() error {
	if c == nil || c.transport == nil {
		return nil
	}
	c.transport.close()
	return nil
}
func (c *Client) Namespace(name string) *Namespace { return &Namespace{client: c, name: name} }
func (c *Client) Ping(ctx context.Context) error {
	r, e := c.do(ctx, wireCommand{Type: "ping"})
	if e != nil {
		return e
	}
	if r.Type != "pong" {
		return unexpected("pong", r.Type)
	}
	return nil
}

type Stats struct {
	Requests uint64
	Hits     uint64
	Misses   uint64
	Values   uint64
}

func (c *Client) Stats(ctx context.Context) (Stats, error) {
	r, e := c.do(ctx, wireCommand{Type: "stats"})
	if e != nil {
		return Stats{}, e
	}
	if r.Type != "stats" || r.Requests == nil || r.Hits == nil || r.Misses == nil || r.Values == nil {
		return Stats{}, unexpected("stats", r.Type)
	}
	return Stats{*r.Requests, *r.Hits, *r.Misses, *r.Values}, nil
}
func (c *Client) do(ctx context.Context, cmd wireCommand) (wireResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	callCtx, cancel := withTimeout(ctx, c.requestTimeout)
	defer cancel()
	return c.transport.request(callCtx, cmd)
}
func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return ctx, func() {}
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= d {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}
func unexpected(want, got string) error {
	return fmt.Errorf("%w: expected %s response, got %s", ErrProtocol, want, got)
}
func encodeValue(v any) (json.RawMessage, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return nil, e
	}
	return b, nil
}
func (c *Client) AuthenticateOnce(ctx context.Context, apiKey string) (AuthOutcome, error) {
	return authenticateOnce(ctx, c.transport, apiKey, nil, c.requestTimeout)
}

type AuthOutcome struct {
	ClientID   string
	SessionTTL time.Duration
	Pending    bool
	RetryAfter time.Duration
}
