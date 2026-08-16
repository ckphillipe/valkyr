package valkyr

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

func authTransport(t *testing.T, responses ...map[string]any) *transport {
	t.Helper()
	client, server := pipeTransport()
	go func() {
		defer server.Close()
		reader := bufio.NewReader(server)
		for _, response := range responses {
			if _, err := reader.ReadBytes('\n'); err != nil {
				return
			}
			var line string
			switch response["type"] {
			case "auth_pending":
				line = fmt.Sprintf("MISS %v", response["retry_after_ms"])
			case "auth_success":
				line = fmt.Sprintf("OK %v TTL %v", response["client_id"], response["session_ttl_seconds"])
			case "auth_failure":
				line = fmt.Sprintf("KO %s", quoteText(fmt.Sprint(response["message"])))
			default:
				return
			}
			if _, err := server.Write([]byte(line + "\n")); err != nil {
				return
			}
		}
	}()
	t.Cleanup(client.close)
	return client
}

func TestAuthenticateOnceReportsPending(t *testing.T) {
	tc := authTransport(t, map[string]any{
		"type": "auth_pending", "retry_after_ms": 25,
	})
	outcome, err := authenticateOnce(context.Background(), tc, "key", nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.Pending || outcome.RetryAfter != 25*time.Millisecond {
		t.Fatalf("unexpected pending outcome: %#v", outcome)
	}
}

func TestAuthenticateRetriesPendingUntilSuccess(t *testing.T) {
	tc := authTransport(t,
		map[string]any{"type": "auth_pending", "retry_after_ms": 1},
		map[string]any{"type": "auth_success", "client_id": "client", "session_ttl_seconds": 60},
	)
	outcome, err := authenticate(context.Background(), tc, "key", nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Pending || outcome.ClientID != "client" || outcome.SessionTTL != time.Minute {
		t.Fatalf("unexpected auth outcome: %#v", outcome)
	}
}

func TestAuthenticateRejectsWithoutRetry(t *testing.T) {
	tc := authTransport(t, map[string]any{"type": "auth_failure", "message": "nope"})
	_, err := authenticate(context.Background(), tc, "key", nil, time.Second)
	var rejected *AuthRejectedError
	if !errors.As(err, &rejected) || rejected.Message != "nope" {
		t.Fatalf("unexpected auth error: %v", err)
	}
}

func TestAuthenticateHonorsContextDuringPendingDelay(t *testing.T) {
	tc := authTransport(t, map[string]any{"type": "auth_pending", "retry_after_ms": 500})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := authenticate(ctx, tc, "key", nil, time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline, got %v", err)
	}
}

func TestAuthenticateRequiresAPIKey(t *testing.T) {
	client, server := net.Pipe()
	server.Close()
	tc := &transport{conn: client, reader: bufio.NewReader(client), closed: make(chan struct{})}
	defer tc.close()
	if _, err := authenticateOnce(context.Background(), tc, "", nil, time.Second); !errors.Is(err, ErrAuth) {
		t.Fatalf("expected ErrAuth, got %v", err)
	}
}
