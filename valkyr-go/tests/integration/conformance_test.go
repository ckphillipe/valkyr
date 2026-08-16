//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	valkyr "github.com/ckphillipe/valkyr/valkyr-go"
)

type liveProvider struct {
	values map[string]any
}

func (p liveProvider) Get(_ context.Context, namespace, key string) (any, error) {
	if namespace == "/__auth" && key == "cold-reader-key" {
		return map[string]any{"client_id": "cold-reader", "name": "Cold reader", "permissions": []any{map[string]any{"namespace": "/", "operations": []string{"read"}}}}, nil
	}
	return p.values[key], nil
}

type expiringProvider struct {
	mu    sync.Mutex
	calls int
}

func (p *expiringProvider) Get(context.Context, string, string) (any, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	ttl := time.Second
	return valkyr.ProviderValue{Value: map[string]int{"calls": p.calls}, TTL: &ttl}, nil
}

type blockingProvider struct {
	mu        sync.Mutex
	calls     int
	started   chan struct{}
	startOnce sync.Once
	release   chan struct{}
}

func newBlockingProvider() *blockingProvider {
	return &blockingProvider{started: make(chan struct{}), release: make(chan struct{})}
}

func (p *blockingProvider) Get(ctx context.Context, _ string, key string) (any, error) {
	p.mu.Lock()
	p.calls++
	p.startOnce.Do(func() { close(p.started) })
	p.mu.Unlock()
	select {
	case <-p.release:
		return map[string]string{"key": key}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *blockingProvider) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

type liveStore struct {
	mu      sync.Mutex
	values  map[string]json.RawMessage
	sets    int
	batches int
	deletes int
	moves   int
}

func newLiveStore() *liveStore { return &liveStore{values: make(map[string]json.RawMessage)} }

func (s *liveStore) Set(_ context.Context, _ string, key string, value json.RawMessage, _ *time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = append(json.RawMessage(nil), value...)
	s.sets++
	return nil
}
func (s *liveStore) SetMany(_ context.Context, _ string, entries []valkyr.Entry, _ *time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range entries {
		s.values[entry.Key] = append(json.RawMessage(nil), entry.Value...)
	}
	s.batches++
	return nil
}
func (s *liveStore) Delete(_ context.Context, _ string, keyPattern *string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if keyPattern == nil {
		s.values = make(map[string]json.RawMessage)
	} else {
		delete(s.values, *keyPattern)
	}
	s.deletes++
	return nil
}
func (s *liveStore) Move(context.Context, string, string) error {
	s.mu.Lock()
	s.moves++
	s.mu.Unlock()
	return nil
}
func (s *liveStore) count() (int, int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sets, s.batches, s.deletes, s.moves
}

func TestLiveApplicationAndAdapterConformance(t *testing.T) {
	server := startServer(t, false)
	adapter, err := valkyr.NewAdapter()
	if err != nil {
		t.Fatal(err)
	}
	provider := liveProvider{values: map[string]any{"ada": map[string]any{"name": "Ada"}}}
	store := newLiveStore()
	for _, registration := range []func() error{
		func() error { return adapter.Provide("/__auth", "*", provider) },
		func() error { return adapter.Provide("/warm", "*", provider) },
		func() error { return adapter.Store("/durable", "*", store) },
	} {
		if err := registration(); err != nil {
			t.Fatal(err)
		}
	}
	adapterClient, err := valkyr.NewAdapterClient([]string{server.nativeAddress}, server.bootstrapKey, adapter, valkyr.AdapterWithReconnectBackoff(25*time.Millisecond, 250*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- adapterClient.Serve(context.Background()) }()
	defer closeAdapter(t, adapterClient, serveDone)

	ctx, cancel := contextWithTimeout(t)
	defer cancel()
	client, err := valkyr.Dial(ctx, server.nativeAddress, valkyr.WithAPIKey(server.bootstrapKey))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err = client.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	warmReady := client.Namespace("/warm").Key("ready")
	waitUntil(t, "provider registration", func() bool {
		result, err := warmReady.Get(ctx)
		_, isMiss := result.(valkyr.Miss)
		return err == nil && isMiss
	})

	// A cold non-bootstrap key must warm through the registered /__auth provider.
	authRecord := client.Namespace("/__auth").Key("cold-reader-key")
	waitUntil(t, "auth provider registration", func() bool {
		result, err := authRecord.Get(ctx)
		_, isValue := result.(valkyr.Value)
		return err == nil && isValue
	})
	coldClient, err := valkyr.Dial(ctx, server.nativeAddress, valkyr.WithAPIKey("cold-reader-key"), valkyr.WithAuthTimeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err = coldClient.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	coldClient.Close()

	route := client.Namespace("/application").Key("user")
	if err = route.Set(ctx, map[string]any{"name": "Ada"}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	result, err := route.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := result.(valkyr.Value)
	if !ok {
		t.Fatalf("expected value, got %T", result)
	}
	var user map[string]string
	if err = value.Decode(&user); err != nil || user["name"] != "Ada" {
		t.Fatalf("decoded value: %v %#v", err, user)
	}
	if err = client.Namespace("/batch").SetMany(ctx, map[string]any{"a": 1, "b": 2}); err != nil {
		t.Fatal(err)
	}
	if err = client.Namespace("/batch").Key("a").Delete(ctx); err != nil {
		t.Fatal(err)
	}
	moved := client.Namespace("/contexts::draft").Key("key")
	if err = moved.Set(ctx, "value"); err != nil {
		t.Fatal(err)
	}
	if err = moved.Move(ctx, "published"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Stats(ctx); err != nil {
		t.Fatal(err)
	}

	unknown, err := client.Namespace("/missing").Key("key").Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok = unknown.(valkyr.Unknown); !ok {
		t.Fatalf("expected unknown, got %T", unknown)
	}

	warm := client.Namespace("/warm").Key("ada")
	first, err := warm.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok = first.(valkyr.Miss); !ok {
		t.Fatalf("expected provider miss, got %T", first)
	}
	waitUntil(t, "provider refresh", func() bool {
		result, err := warm.Get(ctx)
		if err != nil {
			return false
		}
		value, ok := result.(valkyr.Value)
		return ok && string(value.Bytes()) == `{"name":"Ada"}`
	})

	durable := client.Namespace("/durable").Key("key")
	for i := 0; i < 20; i++ {
		key := client.Namespace("/durable").Key("key-" + string(rune('a'+i)))
		if err = key.Set(ctx, i); err != nil {
			t.Fatal(err)
		}
		sets, _, _, _ := store.count()
		if sets > 0 {
			break
		}
	}
	waitUntil(t, "durable set callback", func() bool { sets, _, _, _ := store.count(); return sets > 0 })
	if err = durable.Set(ctx, "stored"); err != nil {
		t.Fatal(err)
	}
	if err = client.Namespace("/durable").SetMany(ctx, map[string]any{"a": 1, "b": 2}); err != nil {
		t.Fatal(err)
	}
	if err = client.Namespace("/durable").Delete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err = client.Namespace("/durable::draft").Key("moved").Move(ctx, "published"); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "durable callback coverage", func() bool {
		_, batches, deletes, moves := store.count()
		return batches > 0 && deletes > 0 && moves > 0
	})
}

func TestLiveProviderValueTTLExpiresAndRefreshes(t *testing.T) {
	server := startServer(t, false)
	provider := &expiringProvider{}
	adapter, err := valkyr.NewAdapter()
	if err != nil {
		t.Fatal(err)
	}
	if err = adapter.Provide("/expiring", "*", provider); err != nil {
		t.Fatal(err)
	}
	adapterClient, err := valkyr.NewAdapterClient([]string{server.nativeAddress}, server.bootstrapKey, adapter)
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- adapterClient.Serve(context.Background()) }()
	defer closeAdapter(t, adapterClient, serveDone)

	ctx, cancel := contextWithTimeout(t)
	defer cancel()
	client, err := valkyr.Dial(ctx, server.nativeAddress, valkyr.WithAPIKey(server.bootstrapKey))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	route := client.Namespace("/expiring").Key("temperature")
	waitUntil(t, "provider value with TTL", func() bool {
		result, getErr := route.Get(ctx)
		if getErr != nil {
			return false
		}
		value, ok := result.(valkyr.Value)
		return ok && value.TTLDuration() != nil && *value.TTLDuration() == time.Second && string(value.Bytes()) == `{"calls":1}`
	})

	time.Sleep(1100 * time.Millisecond)
	waitUntil(t, "expired provider value refresh", func() bool {
		result, getErr := route.Get(ctx)
		if getErr != nil {
			return false
		}
		value, ok := result.(valkyr.Value)
		return ok && string(value.Bytes()) == `{"calls":2}`
	})
}

func TestLiveTLSAndServerNameVerification(t *testing.T) {
	server := startServer(t, true)
	ca, err := os.ReadFile(server.tlsCA)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := contextWithTimeout(t)
	defer cancel()
	client, err := valkyr.Dial(ctx, server.tlsAddress, valkyr.WithAPIKey(server.bootstrapKey), valkyr.WithTLS(valkyr.TLSConfig{CAPEM: ca, ServerName: "localhost"}))
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	client.Close()

	if _, err = valkyr.Dial(ctx, server.tlsAddress, valkyr.WithTLS(valkyr.TLSConfig{CAPEM: ca, ServerName: "wrong.localhost"})); err == nil {
		t.Fatal("wrong TLS server name was accepted")
	}
}

func TestLiveAdapterRestoresRegistrationAfterServerRestart(t *testing.T) {
	server := startServer(t, false)
	adapter, err := valkyr.NewAdapter()
	if err != nil {
		t.Fatal(err)
	}
	if err = adapter.Provide("/reconnect", "*", liveProvider{values: map[string]any{"key": "restored"}}); err != nil {
		t.Fatal(err)
	}
	adapterClient, err := valkyr.NewAdapterClient([]string{server.nativeAddress}, server.bootstrapKey, adapter, valkyr.AdapterWithReconnectBackoff(25*time.Millisecond, 250*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- adapterClient.Serve(context.Background()) }()
	defer closeAdapter(t, adapterClient, serveDone)

	ctx, cancel := contextWithTimeout(t)
	defer cancel()
	client, err := valkyr.Dial(ctx, server.nativeAddress, valkyr.WithAPIKey(server.bootstrapKey))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	route := client.Namespace("/reconnect").Key("key")
	waitUntil(t, "initial provider callback", func() bool {
		result, err := route.Get(ctx)
		if err != nil {
			return false
		}
		value, ok := result.(valkyr.Value)
		return ok && string(value.Bytes()) == `"restored"`
	})
	server.restart(t)
	client.Close()
	client, err = valkyr.Dial(ctx, server.nativeAddress, valkyr.WithAPIKey(server.bootstrapKey))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	route = client.Namespace("/reconnect").Key("key")
	waitUntil(t, "restored provider registration", func() bool {
		result, err := route.Get(ctx)
		if err != nil {
			return false
		}
		value, ok := result.(valkyr.Value)
		return ok && string(value.Bytes()) == `"restored"`
	})
}

func TestLiveDurableCallbackFailureDoesNotCommit(t *testing.T) {
	server := startServer(t, false)
	adapter, err := valkyr.NewAdapter()
	if err != nil {
		t.Fatal(err)
	}
	if err = adapter.Store("/rejected", "*", failingStore{}); err != nil {
		t.Fatal(err)
	}
	adapterClient, err := valkyr.NewAdapterClient([]string{server.nativeAddress}, server.bootstrapKey, adapter)
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- adapterClient.Serve(context.Background()) }()
	defer closeAdapter(t, adapterClient, serveDone)
	ctx, cancel := contextWithTimeout(t)
	defer cancel()
	client, err := valkyr.Dial(ctx, server.nativeAddress, valkyr.WithAPIKey(server.bootstrapKey))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var setErr error
	for i := 0; i < 20; i++ {
		setErr = client.Namespace("/rejected").Key("key-"+string(rune('a'+i))).Set(ctx, i)
		if errors.Is(setErr, valkyr.ErrServer) {
			break
		}
	}
	if !errors.Is(setErr, valkyr.ErrServer) {
		t.Fatalf("expected durable callback error, got %v", setErr)
	}
}

func TestLiveAdapterOverloadDoesNotQueueCallbacks(t *testing.T) {
	server := startServer(t, false)
	provider := newBlockingProvider()
	adapter, err := valkyr.NewAdapter()
	if err != nil {
		t.Fatal(err)
	}
	if err = adapter.Provide("/overload", "*", provider); err != nil {
		t.Fatal(err)
	}
	adapterClient, err := valkyr.NewAdapterClient([]string{server.nativeAddress}, server.bootstrapKey, adapter, valkyr.AdapterWithMaxConcurrency(1), valkyr.AdapterWithCallbackTimeout(500*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- adapterClient.Serve(context.Background()) }()
	defer closeAdapter(t, adapterClient, serveDone)
	ctx, cancel := contextWithTimeout(t)
	defer cancel()
	client, err := valkyr.Dial(ctx, server.nativeAddress, valkyr.WithAPIKey(server.bootstrapKey))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	first := client.Namespace("/overload").Key("first")
	second := client.Namespace("/overload").Key("second")
	waitUntil(t, "overload provider registration", func() bool {
		result, err := first.Get(ctx)
		_, isMiss := result.(valkyr.Miss)
		return err == nil && isMiss
	})
	select {
	case <-provider.started:
	case <-time.After(5 * time.Second):
		t.Fatal("first callback did not start")
	}
	if result, err := second.Get(ctx); err != nil {
		t.Fatal(err)
	} else if _, ok := result.(valkyr.Miss); !ok {
		t.Fatalf("expected second request miss, got %T", result)
	}
	time.Sleep(100 * time.Millisecond)
	if calls := provider.callCount(); calls != 1 {
		t.Fatalf("overloaded callback was queued or executed: %d provider calls", calls)
	}
	close(provider.release)
	waitUntil(t, "first callback completion", func() bool {
		result, err := first.Get(ctx)
		if err != nil {
			return false
		}
		value, ok := result.(valkyr.Value)
		return ok && string(value.Bytes()) == `{"key":"first"}`
	})
}

type failingStore struct{}

func (failingStore) Set(context.Context, string, string, json.RawMessage, *time.Duration) error {
	return errors.New("durable store rejected the value")
}
func (failingStore) SetMany(context.Context, string, []valkyr.Entry, *time.Duration) error {
	return errors.New("durable store rejected the batch")
}
func (failingStore) Delete(context.Context, string, *string) error {
	return errors.New("durable store rejected delete")
}
func (failingStore) Move(context.Context, string, string) error {
	return errors.New("durable store rejected move")
}
