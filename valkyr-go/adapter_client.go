package valkyr

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type adapterOptions struct {
	tls                                          *TLSConfig
	connectTimeout, authTimeout, callbackTimeout time.Duration
	maxConcurrency                               int
	backoffMin, backoffMax                       time.Duration
}
type AdapterOption func(*adapterOptions) error

func AdapterWithTLS(c TLSConfig) AdapterOption {
	return func(o *adapterOptions) error { o.tls = &c; return nil }
}
func AdapterWithCallbackTimeout(d time.Duration) AdapterOption {
	return func(o *adapterOptions) error {
		if d <= 0 {
			return protocol("callback timeout must be positive")
		}
		o.callbackTimeout = d
		return nil
	}
}
func AdapterWithMaxConcurrency(n int) AdapterOption {
	return func(o *adapterOptions) error {
		if n <= 0 {
			return protocol("max concurrency must be positive")
		}
		o.maxConcurrency = n
		return nil
	}
}
func AdapterWithReconnectBackoff(min, max time.Duration) AdapterOption {
	return func(o *adapterOptions) error {
		if min <= 0 || max < min {
			return protocol("invalid reconnect backoff")
		}
		o.backoffMin = min
		o.backoffMax = max
		return nil
	}
}

type AdapterClient struct {
	endpoints    []string
	apiKey       string
	providers    []ProvideRoute
	stores       []StoreRoute
	instance     string
	opts         adapterOptions
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	closeOnce    sync.Once
	semaphore    chan struct{}
	transportsMu sync.Mutex
	transports   map[*transport]struct{}
}

func NewAdapterClient(endpoints []string, apiKey string, adapter *Adapter, options ...AdapterOption) (*AdapterClient, error) {
	if len(endpoints) == 0 || apiKey == "" || adapter == nil {
		return nil, fmt.Errorf("%w: adapter endpoints, API key, and adapter are required", ErrRoute)
	}
	o := adapterOptions{connectTimeout: 10 * time.Second, authTimeout: 5 * time.Second, callbackTimeout: 30 * time.Second, maxConcurrency: 32, backoffMin: 500 * time.Millisecond, backoffMax: 30 * time.Second}
	for _, f := range options {
		if e := f(&o); e != nil {
			return nil, e
		}
	}
	for _, endpoint := range endpoints {
		if _, _, e := parseAddress(endpoint); e != nil {
			return nil, e
		}
	}
	providers, stores, instance := adapter.snapshot()
	return &AdapterClient{
		endpoints:  append([]string(nil), endpoints...),
		apiKey:     apiKey,
		providers:  providers,
		stores:     stores,
		instance:   instance,
		opts:       o,
		semaphore:  make(chan struct{}, o.maxConcurrency),
		transports: make(map[*transport]struct{}),
	}, nil
}
func (a *AdapterClient) Serve(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	a.ctx, a.cancel = context.WithCancel(ctx)
	for i := range a.endpoints {
		a.wg.Add(1)
		go a.supervise(i)
	}
	done := make(chan struct{})
	go func() { a.wg.Wait(); close(done) }()
	select {
	case <-ctx.Done():
		a.Close()
		return ctx.Err()
	case <-done:
		return nil
	}
}
func (a *AdapterClient) Close() {
	a.closeOnce.Do(func() {
		if a.cancel != nil {
			a.cancel()
		}
		a.transportsMu.Lock()
		for t := range a.transports {
			t.close()
		}
		a.transports = map[*transport]struct{}{}
		a.transportsMu.Unlock()
	})
}
func (a *AdapterClient) supervise(index int) {
	defer a.wg.Done()
	delay := a.opts.backoffMin
	for {
		if a.ctx.Err() != nil {
			return
		}
		t, e := a.connect(index)
		if e == nil {
			a.addTransport(t)
			var valid bool
			e, valid = a.runConnection(t)
			a.removeTransport(t)
			t.close()
			if valid {
				delay = a.opts.backoffMin
			}
		}
		if a.ctx.Err() != nil {
			return
		}
		if _, ok := e.(*AuthRejectedError); ok {
			return
		}
		wait := delay
		if delay < a.opts.backoffMax {
			delay *= 2
			if delay > a.opts.backoffMax {
				delay = a.opts.backoffMax
			}
		}
		wait += time.Duration(rand.Int63n(int64(wait/2) + 1))
		if wait > a.opts.backoffMax {
			wait = a.opts.backoffMax
		}
		timer := time.NewTimer(wait)
		select {
		case <-a.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
func (a *AdapterClient) connect(index int) (*transport, error) {
	o := defaultOptions()
	o.ConnectTimeout = a.opts.connectTimeout
	o.AuthTimeout = a.opts.authTimeout
	o.TLS = a.opts.tls
	t, e := openTransport(a.ctx, a.endpoints[index], o)
	if e != nil {
		return nil, e
	}
	if _, e = authenticate(a.ctx, t, a.apiKey, &a.instance, a.opts.authTimeout); e != nil {
		t.close()
		return nil, e
	}
	if e = a.register(t); e != nil {
		t.close()
		return nil, e
	}
	return t, nil
}
func (a *AdapterClient) register(t *transport) error {
	for _, r := range a.providers {
		timeout, e := protocolDurationMillis(r.Options.Timeout)
		if e != nil {
			return e
		}
		missTTL, e := protocolDurationSeconds(r.Options.MissTTL)
		if e != nil {
			return e
		}
		cmd := wireCommand{Type: "provide", NamespacePattern: r.NamespacePattern, KeyPattern: stringPtr(r.KeyPattern), MaxRate: r.MaxRate, Timeout: timeout, MissTTL: missTTL}
		if resp, e := t.request(a.ctx, cmd); e != nil {
			return e
		} else if e = expectOK(resp); e != nil {
			return e
		}
	}
	for _, r := range a.stores {
		cmd := wireCommand{Type: "store", NamespacePattern: r.NamespacePattern, KeyPattern: stringPtr(r.KeyPattern)}
		if resp, e := t.request(a.ctx, cmd); e != nil {
			return e
		} else if e = expectOK(resp); e != nil {
			return e
		}
	}
	return nil
}
func (a *AdapterClient) runConnection(t *transport) (error, bool) {
	validCallback := false
	for {
		cmd, e := t.readCommand(a.ctx)
		if e != nil {
			return e, validCallback
		}
		validCallback = true
		select {
		case a.semaphore <- struct{}{}:
			go func() { defer func() { <-a.semaphore }(); a.dispatch(t, cmd) }()
		default:
			go a.overload(t, cmd)
		}
	}
}
func (a *AdapterClient) dispatch(t *transport, cmd wireServerCommand) {
	ctx, cancel := context.WithTimeout(a.ctx, a.opts.callbackTimeout)
	defer cancel()
	var result wireServerResult
	done := make(chan wireServerResult, 1)
	go func() { result = a.safeHandle(ctx, cmd); done <- result }()
	select {
	case result = <-done:
	case <-ctx.Done():
		result = errorResult(cmd, ctx.Err().Error())
	}
	if encoded, err := textServerResult(cmd, result); err == nil {
		_ = t.writeRaw(a.ctx, encoded)
	}
}
func (a *AdapterClient) safeHandle(ctx context.Context, command wireServerCommand) (result wireServerResult) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = errorResult(command, fmt.Sprintf("adapter callback panic: %v", recovered))
		}
	}()
	return a.handle(ctx, command)
}
func (a *AdapterClient) overload(t *transport, cmd wireServerCommand) {
	if encoded, err := textServerResult(cmd, errorResult(cmd, "adapter overloaded")); err == nil {
		_ = t.writeRaw(a.ctx, encoded)
	}
}
func (a *AdapterClient) handle(ctx context.Context, c wireServerCommand) wireServerResult {
	if c.Type == "query" {
		for _, r := range a.providers {
			if namespaceMatch(r.NamespacePattern, c.Namespace) && match(r.KeyPattern, c.Key) {
				v, e := r.Provider.Get(ctx, c.Namespace, c.Key)
				if e != nil {
					return queryResult(c, e.Error(), nil, nil)
				}
				var ttl *uint64
				if providerValue, ok := v.(ProviderValue); ok {
					ttl, e = durationSeconds(providerValue.TTL)
					if e != nil {
						return queryResult(c, e.Error(), nil, nil)
					}
					v = providerValue.Value
				}
				if v == nil {
					return queryResult(c, "", []byte("null"), nil)
				}
				raw, e := encodeValue(v)
				if e != nil {
					return queryResult(c, e.Error(), nil, nil)
				}
				return queryResult(c, "", raw, ttl)
			}
		}
		return queryResult(c, "", []byte("null"), nil)
	}
	namespace, key := callbackRoute(c)
	for i := len(a.stores) - 1; i >= 0; i-- {
		r := a.stores[i]
		if !namespaceMatch(r.NamespacePattern, namespace) {
			continue
		}
		if c.Type == "persist_set_batch" {
			ok := true
			for _, entry := range c.Entries {
				if !overlap(r.KeyPattern, entry.Key) {
					ok = false
					break
				}
			}
			if !ok {
				continue
			}
		} else if !overlap(r.KeyPattern, key) {
			continue
		}
		var e error
		switch c.Type {
		case "persist_set":
			e = r.Store.Set(ctx, c.Namespace, c.Key, c.Value, durationPtr(c.TTL))
		case "persist_set_batch":
			entries := make([]Entry, len(c.Entries))
			for i, x := range c.Entries {
				entries[i] = Entry{x.Key, append(json.RawMessage(nil), x.Value...)}
			}
			e = r.Store.SetMany(ctx, c.Namespace, entries, durationPtr(c.TTL))
		case "persist_delete":
			e = r.Store.Delete(ctx, c.Namespace, c.KeyPattern)
		case "persist_move":
			e = r.Store.Move(ctx, c.Source, c.Destination)
		default:
			return errorResult(c, "unsupported callback")
		}
		if e != nil {
			return operationResult(c, e.Error())
		}
		return operationResult(c, "")
	}
	return errorResult(c, "no store handler registered")
}
func callbackRoute(c wireServerCommand) (string, string) {
	switch c.Type {
	case "persist_move":
		return c.Source, "*"
	case "persist_set":
		return c.Namespace, c.Key
	case "persist_delete":
		if c.KeyPattern != nil {
			return c.Namespace, *c.KeyPattern
		}
		return c.Namespace, "*"
	default:
		return c.Namespace, "*"
	}
}
func queryResult(c wireServerCommand, msg string, value json.RawMessage, ttl *uint64) wireServerResult {
	var e *string
	if msg != "" {
		e = &msg
	}
	return wireServerResult{Type: "query", RequestID: c.RequestID, Value: value, Error: e, TTL: ttl}
}
func operationResult(c wireServerCommand, msg string) wireServerResult {
	var e *string
	if msg != "" {
		e = &msg
	}
	return wireServerResult{Type: "operation", RequestID: c.RequestID, Error: e}
}
func errorResult(c wireServerCommand, msg string) wireServerResult {
	if c.Type == "query" {
		return queryResult(c, msg, []byte("null"), nil)
	}
	return operationResult(c, msg)
}
func stringPtr(s string) *string { return &s }
func (a *AdapterClient) addTransport(t *transport) {
	a.transportsMu.Lock()
	a.transports[t] = struct{}{}
	a.transportsMu.Unlock()
}
func (a *AdapterClient) removeTransport(t *transport) {
	a.transportsMu.Lock()
	delete(a.transports, t)
	a.transportsMu.Unlock()
}
