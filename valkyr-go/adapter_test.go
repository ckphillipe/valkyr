package valkyr

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type recordingProvider struct {
	value any
	err   error
}

func (p recordingProvider) Get(context.Context, string, string) (any, error) {
	return p.value, p.err
}

type recordingStore struct {
	setValue json.RawMessage
	setTTL   *time.Duration
	setErr   error
}

func (s *recordingStore) Set(_ context.Context, _ string, _ string, value json.RawMessage, ttl *time.Duration) error {
	s.setValue = append(s.setValue[:0], value...)
	s.setTTL = ttl
	return s.setErr
}
func (*recordingStore) SetMany(context.Context, string, []Entry, *time.Duration) error { return nil }
func (*recordingStore) Delete(context.Context, string, *string) error                  { return nil }
func (*recordingStore) Move(context.Context, string, string) error                     { return nil }

func TestAdapterProviderCallbackOutcomes(t *testing.T) {
	a, err := NewAdapter()
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Provide("/values", "*", recordingProvider{value: map[string]any{"ok": true}}); err != nil {
		t.Fatal(err)
	}
	client, err := NewAdapterClient([]string{"127.0.0.1"}, "key", a)
	if err != nil {
		t.Fatal(err)
	}
	_, _, instance := a.snapshot()
	if _, err = uuidValue(instance, true); err != nil {
		t.Fatal(err)
	}

	value := client.handle(context.Background(), wireServerCommand{Type: "query", RequestID: instance, Namespace: "/values", Key: "one"})
	if value.Type != "query" || string(value.Value) != `{"ok":true}` || value.Error != nil || value.TTL != nil {
		t.Fatalf("unexpected provider value: %#v", value)
	}

	ttl := 5 * time.Minute
	ttlAdapter, err := NewAdapter()
	if err != nil {
		t.Fatal(err)
	}
	if err = ttlAdapter.Provide("/values", "*", recordingProvider{value: ProviderValue{
		Value: map[string]any{"ok": true},
		TTL:   &ttl,
	}}); err != nil {
		t.Fatal(err)
	}
	ttlClient, err := NewAdapterClient([]string{"127.0.0.1"}, "key", ttlAdapter)
	if err != nil {
		t.Fatal(err)
	}
	ttlResult := ttlClient.handle(context.Background(), wireServerCommand{Type: "query", RequestID: instance, Namespace: "/values", Key: "one"})
	if ttlResult.Error != nil || ttlResult.TTL == nil || *ttlResult.TTL != 300 {
		t.Fatalf("unexpected provider TTL: %#v", ttlResult)
	}

	missAdapter, _ := NewAdapter()
	missClient, err := NewAdapterClient([]string{"127.0.0.1"}, "key", missAdapter)
	if err != nil {
		t.Fatal(err)
	}
	miss := missClient.handle(context.Background(), wireServerCommand{Type: "query", RequestID: instance, Namespace: "/none", Key: "one"})
	if string(miss.Value) != "null" || miss.Error != nil || miss.TTL != nil {
		t.Fatalf("unexpected provider miss: %#v", miss)
	}

	badAdapter, err := NewAdapter()
	if err != nil {
		t.Fatal(err)
	}
	badTTL := time.Millisecond
	if err = badAdapter.Provide("/values", "*", recordingProvider{value: ProviderValue{
		Value: map[string]any{"ok": true},
		TTL:   &badTTL,
	}}); err != nil {
		t.Fatal(err)
	}
	badClient, err := NewAdapterClient([]string{"127.0.0.1"}, "key", badAdapter)
	if err != nil {
		t.Fatal(err)
	}
	badResult := badClient.handle(context.Background(), wireServerCommand{Type: "query", RequestID: instance, Namespace: "/values", Key: "one"})
	if badResult.Error == nil || badResult.TTL != nil {
		t.Fatalf("invalid provider TTL was not rejected: %#v", badResult)
	}
}

func TestAdapterStoreCallbackAcknowledgesOnlySuccessfulWork(t *testing.T) {
	store := &recordingStore{}
	a, err := NewAdapter()
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Store("/durable", "*", store); err != nil {
		t.Fatal(err)
	}
	client, err := NewAdapterClient([]string{"127.0.0.1"}, "key", a)
	if err != nil {
		t.Fatal(err)
	}
	callback := wireServerCommand{Type: "persist_set", RequestID: "00000000-0000-0000-0000-000000000001", Namespace: "/durable", Key: "key", Value: json.RawMessage(`{"v":1}`)}
	result := client.handle(context.Background(), callback)
	if result.Type != "operation" || result.Error != nil || string(store.setValue) != `{"v":1}` {
		t.Fatalf("successful store callback: %#v", result)
	}

	store.setErr = errors.New("rejected")
	result = client.handle(context.Background(), callback)
	if result.Type != "operation" || result.Error == nil || *result.Error != "rejected" {
		t.Fatalf("failed store callback: %#v", result)
	}
	store.setErr = nil
	storeTTL := uint64(120)
	storeCallback := wireServerCommand{Type: "persist_set", RequestID: callback.RequestID, Namespace: "/durable", Key: "key", Value: json.RawMessage(`{"v":1}`), TTL: &storeTTL}
	if result = client.handle(context.Background(), storeCallback); result.Error != nil || store.setTTL == nil || *store.setTTL != 2*time.Minute {
		t.Fatalf("store TTL was not forwarded: %#v", result)
	}
}

func TestAdapterCallbackTimeoutAndPanicAreCorrelatedErrors(t *testing.T) {
	a, err := NewAdapter()
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Provide("/panic", "*", recordingProvider{err: errors.New("provider failed")}); err != nil {
		t.Fatal(err)
	}
	client, err := NewAdapterClient([]string{"127.0.0.1"}, "key", a)
	if err != nil {
		t.Fatal(err)
	}
	callback := wireServerCommand{Type: "query", RequestID: "00000000-0000-0000-0000-000000000001", Namespace: "/panic", Key: "key"}
	result := client.safeHandle(context.Background(), callback)
	if result.Error == nil || *result.Error != "provider failed" {
		t.Fatalf("provider failure was not correlated: %#v", result)
	}
}

type panicProvider struct{}

func (panicProvider) Get(context.Context, string, string) (any, error) { panic("boom") }

func TestAdapterSafeHandleContainsPanics(t *testing.T) {
	a, err := NewAdapter()
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Provide("/panic", "*", panicProvider{}); err != nil {
		t.Fatal(err)
	}
	client, err := NewAdapterClient([]string{"127.0.0.1"}, "key", a)
	if err != nil {
		t.Fatal(err)
	}
	result := client.safeHandle(context.Background(), wireServerCommand{Type: "query", RequestID: "00000000-0000-0000-0000-000000000001", Namespace: "/panic", Key: "key"})
	if result.Error == nil || *result.Error != "adapter callback panic: boom" {
		t.Fatalf("panic was not contained: %#v", result)
	}
}
