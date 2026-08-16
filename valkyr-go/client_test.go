package valkyr

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"
)

func scriptedClient(t *testing.T, handler func(map[string]any) wireResponse) *Client {
	t.Helper()
	a, b := net.Pipe()
	tr := &transport{conn: a, reader: bufio.NewReader(a), closed: make(chan struct{})}
	go func() {
		r := bufio.NewReader(b)
		for {
			line, e := r.ReadBytes('\n')
			if e != nil {
				return
			}
			command, err := parseTextCommand(line)
			if err != nil {
				return
			}
			req := map[string]any{"type": command.Type, "key": command.Key, "namespace": command.Namespace}
			resp := handler(req)
			response := resp
			encoded, encodeErr := textResponse(command, response)
			if encodeErr != nil {
				return
			}
			if _, e = b.Write(append(encoded, '\n')); e != nil {
				return
			}
		}
	}()
	return &Client{transport: tr, requestTimeout: time.Second}
}
func TestClientReadOutcomesAndRetry(t *testing.T) {
	calls := 0
	c := scriptedClient(t, func(req map[string]any) wireResponse {
		calls++
		if calls == 1 {
			return wireResponse{Type: "miss", RetryAfter: uintPtr(0)}
		}
		return wireResponse{Type: "value", Value: json.RawMessage(`{"name":"Ada"}`), TTL: uintPtr(10)}
	})
	defer c.Close()
	r, e := c.Namespace("/users").Key("42").GetWithRetry(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	v, ok := r.(Value)
	if !ok {
		t.Fatalf("result type %T", r)
	}
	var got map[string]string
	if e = v.Decode(&got); e != nil || got["name"] != "Ada" {
		t.Fatalf("decode: %v %#v", e, got)
	}
	if calls != 2 {
		t.Fatalf("retry count %d", calls)
	}
}
func TestClientValidatesBeforeWriting(t *testing.T) {
	written := make(chan struct{}, 1)
	c := scriptedClient(t, func(req map[string]any) wireResponse { written <- struct{}{}; return wireResponse{Type: "ok"} })
	defer c.Close()
	if e := c.Namespace("").Key("k").Set(context.Background(), 1); e == nil {
		t.Fatal("empty namespace accepted")
	}
	if e := c.Namespace("/x").Key("k").Set(context.Background(), 1, time.Millisecond); e == nil {
		t.Fatal("fractional TTL accepted")
	}
	select {
	case <-written:
		t.Fatal("invalid request was written")
	default:
	}
}
func TestClientWritesJSONAndStats(t *testing.T) {
	c := scriptedClient(t, func(req map[string]any) wireResponse {
		switch req["type"] {
		case "stats":
			return wireResponse{Type: "stats", Requests: uintPtr(1), Hits: uintPtr(2), Misses: uintPtr(3), Values: uintPtr(4)}
		default:
			return wireResponse{Type: "ok"}
		}
	})
	defer c.Close()
	if e := c.Namespace("/x").Key("k").Set(context.Background(), map[string]string{"x": "y"}); e != nil {
		t.Fatal(e)
	}
	s, e := c.Stats(context.Background())
	if e != nil || s.Values != 4 {
		t.Fatalf("stats: %#v %v", s, e)
	}
}
