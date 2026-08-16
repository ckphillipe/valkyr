package valkyr

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func pipeTransport() (*transport, net.Conn) {
	a, b := net.Pipe()
	return &transport{conn: a, reader: bufio.NewReader(a), closed: make(chan struct{})}, b
}
func TestTransportSerializesConcurrentRequests(t *testing.T) {
	client, server := pipeTransport()
	defer client.close()
	defer server.Close()
	go func() {
		r := bufio.NewReader(server)
		for i := 0; i < 20; i++ {
			line, _ := r.ReadBytes('\n')
			c, err := parseTextCommand(line)
			if err != nil {
				return
			}
			response := wireResponse{Type: "value", Value: json.RawMessage(fmt.Sprintf("%q", c.Key))}
			encoded, _ := textResponse(c, response)
			server.Write(append(encoded, '\n'))
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, e := client.request(context.Background(), wireCommand{Type: "get", Namespace: "/x", Key: fmt.Sprint(i)})
			if e != nil || r.Type != "value" {
				t.Errorf("request failed: %v", e)
			}
		}(i)
	}
	wg.Wait()
}
func TestTransportRequestErrorLeavesConnectionUsable(t *testing.T) {
	client, server := pipeTransport()
	defer client.close()
	defer server.Close()
	go func() {
		r := bufio.NewReader(server)
		r.ReadBytes('\n')
		server.Write([]byte("KO temporary\n"))
		r.ReadBytes('\n')
		server.Write([]byte("PONG\n"))
	}()
	r, e := client.request(context.Background(), wireCommand{Type: "ping"})
	if e != nil || r.Type != "error" {
		t.Fatalf("error response: %v %#v", e, r)
	}
	r, e = client.request(context.Background(), wireCommand{Type: "ping"})
	if e != nil || r.Type != "pong" {
		t.Fatalf("connection not reusable: %v %#v", e, r)
	}
}
func TestTransportPoisoning(t *testing.T) {
	client, server := pipeTransport()
	defer server.Close()
	go func() {
		_, _ = bufio.NewReader(server).ReadBytes('\n')
		buf := make([]byte, 2*maxFrameBytes)
		strings.Repeat("x", len(buf))
		server.Write(append([]byte(`{"type":"`), buf...))
	}()
	if _, e := client.request(context.Background(), wireCommand{Type: "ping"}); e == nil || !errors.Is(e, ErrProtocol) {
		t.Fatalf("oversized frame error: %v", e)
	}
	if !client.isClosed() {
		t.Fatal("transport was not poisoned")
	}
}
func TestAddressParsing(t *testing.T) {
	tests := map[string][2]string{"127.0.0.1": {"127.0.0.1", "8081"}, "127.0.0.1:9000": {"127.0.0.1", "9000"}, "[::1]": {"::1", "8081"}, "[::1]:9000": {"::1", "9000"}}
	for in, want := range tests {
		h, p, e := parseAddress(in)
		if e != nil || h != want[0] || p != want[1] {
			t.Errorf("%s => %s:%s (%v)", in, h, p, e)
		}
	}
}
func TestTransportContextTimeoutCloses(t *testing.T) {
	client, server := pipeTransport()
	defer server.Close()
	go func() { bufio.NewReader(server).ReadBytes('\n'); time.Sleep(100 * time.Millisecond) }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, e := client.request(ctx, wireCommand{Type: "ping"}); e == nil {
		t.Fatal("timeout not reported")
	}
	if !client.isClosed() {
		t.Fatal("timeout did not close transport")
	}
}
