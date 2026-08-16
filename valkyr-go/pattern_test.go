package valkyr

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"
)

type testProvider struct{ value any }

func (p testProvider) Get(context.Context, string, string) (any, error) { return p.value, nil }

type testStore struct{}

func (testStore) Set(context.Context, string, string, json.RawMessage, *time.Duration) error {
	return nil
}
func (testStore) SetMany(context.Context, string, []Entry, *time.Duration) error { return nil }
func (testStore) Delete(context.Context, string, *string) error                  { return nil }
func (testStore) Move(context.Context, string, string) error                     { return nil }
func TestPatternGrammar(t *testing.T) {
	if !match("/users/{id}/*", "/users/42/profile") {
		t.Fatal("capture/wildcard did not match")
	}
	if !match("/services/${service}/config", "/services/api/config") {
		t.Fatal("dollar capture did not match")
	}
	if match("/users/*", "/groups/42") {
		t.Fatal("wildcard matched wrong prefix")
	}
	if !namespaceMatch("/users", "/users::draft") {
		t.Fatal("context namespace did not match")
	}
}
func TestAdapterRegistrationSnapshotAndOverlap(t *testing.T) {
	a, e := NewAdapter()
	if e != nil {
		t.Fatal(e)
	}
	if e = a.Provide("/users", "*", testProvider{}); e != nil {
		t.Fatal(e)
	}
	if e = a.Provide("/users", "42", testProvider{}); e == nil {
		t.Fatal("overlap accepted")
	}
	if e = a.Store("/users", "*", testStore{}); e != nil {
		t.Fatal(e)
	}
	p, s, id := a.snapshot()
	if len(p) != 1 || len(s) != 1 || id == "" {
		t.Fatalf("bad snapshot: %d %d %q", len(p), len(s), id)
	}
}

func TestProviderOptionsSnapshotValidatesUnitsAndPreservesReconnectState(t *testing.T) {
	a, err := NewAdapter()
	if err != nil {
		t.Fatal(err)
	}
	maxRate := uint64(2)
	options := ProvideOptions{MaxRate: &maxRate, Timeout: 250 * time.Millisecond, MissTTL: 30 * time.Second}
	if err = a.ProvideWithOptions("/values", "*", testProvider{}, options); err != nil {
		t.Fatal(err)
	}
	providers, _, _ := a.snapshot()
	maxRate = 99
	if len(providers) != 1 || providers[0].MaxRate == nil || *providers[0].MaxRate != 2 || providers[0].Options.MaxRate == nil || *providers[0].Options.MaxRate != 2 || providers[0].Options.Timeout != options.Timeout || providers[0].Options.MissTTL != options.MissTTL {
		t.Fatalf("provider options were not preserved: %#v", providers)
	}
	if err = a.ProvideWithOptions("/other", "*", testProvider{}, ProvideOptions{Timeout: time.Microsecond}); err == nil {
		t.Fatal("fractional provider timeout accepted")
	}
	if err = a.ProvideWithOptions("/other", "*", testProvider{}, ProvideOptions{MissTTL: -time.Second}); err == nil {
		t.Fatal("negative provider miss TTL accepted")
	}
	zero := uint64(0)
	if err = a.ProvideWithOptions("/other", "*", testProvider{}, ProvideOptions{MaxRate: &zero}); err == nil {
		t.Fatal("zero provider max rate accepted")
	}
	tooLarge := uint64(^uint32(0)) + 1
	if err = a.ProvideWithOptions("/other", "*", testProvider{}, ProvideOptions{MaxRate: &tooLarge}); err == nil {
		t.Fatal("out-of-range provider max rate accepted")
	}
}

func TestAdapterRegistrationRestoresProviderOptionsOnReconnect(t *testing.T) {
	a, err := NewAdapter()
	if err != nil {
		t.Fatal(err)
	}
	maxRate := uint64(2)
	options := ProvideOptions{MaxRate: &maxRate, Timeout: 250 * time.Millisecond, MissTTL: 30 * time.Second}
	if err = a.ProvideWithOptions("/values", "*", testProvider{}, options); err != nil {
		t.Fatal(err)
	}
	maxRate = 99
	client, err := NewAdapterClient([]string{"127.0.0.1"}, "key", a)
	if err != nil {
		t.Fatal(err)
	}
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	frames := make(chan wireCommand, 2)
	go func() {
		reader := bufio.NewReader(server)
		for range 2 {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				return
			}
			command, parseErr := parseTextCommand(line)
			if parseErr == nil {
				frames <- command
			}
			_, _ = server.Write([]byte("OK\n"))
		}
	}()
	transport := &transport{conn: peer, reader: bufio.NewReader(peer), closed: make(chan struct{})}
	if err = client.register(transport); err != nil {
		t.Fatal(err)
	}
	if err = client.register(transport); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		command := <-frames
		if command.MaxRate == nil || *command.MaxRate != 2 || command.Timeout == nil || *command.Timeout != 250 || command.MissTTL == nil || *command.MissTTL != 30 {
			t.Fatalf("provider options were not restored: %#v", command)
		}
	}
}

func TestAdapterClientSnapshotSurvivesSourceMutationAndReconnect(t *testing.T) {
	a, err := NewAdapter()
	if err != nil {
		t.Fatal(err)
	}
	provider := testProvider{value: map[string]any{"source": "original"}}
	store := &recordingStore{}
	maxRate := uint64(2)
	options := ProvideOptions{MaxRate: &maxRate, Timeout: 250 * time.Millisecond, MissTTL: 30 * time.Second}
	if err = a.ProvideWithOptions("/stable", "*", provider, options); err != nil {
		t.Fatal(err)
	}
	if err = a.Store("/stable", "*", store); err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client, err := NewAdapterClient([]string{listener.Addr().String()}, "key", a)
	if err != nil {
		t.Fatal(err)
	}
	instance := a.AdapterInstance()
	if err = a.Provide("/added", "*", testProvider{value: "added"}); err != nil {
		t.Fatal(err)
	}
	addedStore := &recordingStore{}
	if err = a.Store("/added", "*", addedStore); err != nil {
		t.Fatal(err)
	}
	if err = a.SetAdapterInstance("00000000-0000-0000-0000-000000000002"); err != nil {
		t.Fatal(err)
	}

	stableQuery := client.handle(context.Background(), wireServerCommand{Type: "query", Namespace: "/stable", Key: "key"})
	if stableQuery.Error != nil || string(stableQuery.Value) != `{"source":"original"}` {
		t.Fatalf("original provider route changed: %#v", stableQuery)
	}
	addedQuery := client.handle(context.Background(), wireServerCommand{Type: "query", Namespace: "/added", Key: "key"})
	if addedQuery.Error != nil || string(addedQuery.Value) != "null" {
		t.Fatalf("added provider route was served: %#v", addedQuery)
	}
	stableStore := client.handle(context.Background(), wireServerCommand{Type: "persist_set", Namespace: "/stable", Key: "key", Value: json.RawMessage(`{"v":1}`)})
	if stableStore.Error != nil || string(store.setValue) != `{"v":1}` {
		t.Fatalf("original store route changed: %#v", stableStore)
	}
	addedStoreResult := client.handle(context.Background(), wireServerCommand{Type: "persist_set", Namespace: "/added", Key: "key", Value: json.RawMessage(`{"v":2}`)})
	if addedStoreResult.Error == nil || len(addedStore.setValue) != 0 {
		t.Fatalf("added store route was served: %#v", addedStoreResult)
	}

	serverErrors := make(chan error, 1)
	go serveAdapterSnapshotCycles(listener, instance, options, serverErrors)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.ctx = ctx
	for cycle := 0; cycle < 2; cycle++ {
		transport, connectErr := client.connect(0)
		if connectErr != nil {
			t.Fatalf("connection cycle %d failed: %v", cycle+1, connectErr)
		}
		transport.close()
	}
	if err = <-serverErrors; err != nil {
		t.Fatal(err)
	}
}

func serveAdapterSnapshotCycles(listener net.Listener, instance string, options ProvideOptions, errors chan<- error) {
	for cycle := 0; cycle < 2; cycle++ {
		conn, err := listener.Accept()
		if err != nil {
			errors <- err
			return
		}
		reader := bufio.NewReader(conn)
		for commandIndex := 0; commandIndex < 3; commandIndex++ {
			line, readErr := reader.ReadBytes('\n')
			if readErr != nil {
				conn.Close()
				errors <- readErr
				return
			}
			command, parseErr := parseTextCommand(line)
			if parseErr != nil {
				conn.Close()
				errors <- parseErr
				return
			}
			if err = validateSnapshotCommand(command, instance, options, commandIndex); err != nil {
				conn.Close()
				errors <- fmt.Errorf("cycle %d: %w", cycle+1, err)
				return
			}
			response := "OK"
			if commandIndex == 0 {
				response = "OK client TTL 60"
			}
			if _, err = conn.Write([]byte(response + "\n")); err != nil {
				conn.Close()
				errors <- err
				return
			}
		}
		conn.Close()
	}
	errors <- nil
}

func validateSnapshotCommand(command wireCommand, instance string, options ProvideOptions, index int) error {
	switch index {
	case 0:
		if command.Type != "auth" || command.APIKey != "key" || command.AdapterInstance == nil || *command.AdapterInstance != instance {
			return fmt.Errorf("unexpected auth command: %#v", command)
		}
	case 1:
		if command.Type != "provide" || command.NamespacePattern != "/stable" || command.KeyPattern == nil || *command.KeyPattern != "*" || command.MaxRate == nil || *command.MaxRate != *options.MaxRate || command.Timeout == nil || *command.Timeout != 250 || command.MissTTL == nil || *command.MissTTL != 30 {
			return fmt.Errorf("unexpected provider command: %#v", command)
		}
	case 2:
		if command.Type != "store" || command.NamespacePattern != "/stable" || command.KeyPattern == nil || *command.KeyPattern != "*" {
			return fmt.Errorf("unexpected store command: %#v", command)
		}
	}
	return nil
}
