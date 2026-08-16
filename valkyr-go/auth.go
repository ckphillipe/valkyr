package valkyr

import (
	"context"
	"fmt"
	"time"
)

func authenticate(ctx context.Context, t *transport, key string, adapter *string, timeout time.Duration) (AuthOutcome, error) {
	if key == "" {
		return AuthOutcome{}, fmt.Errorf("%w: API key is required", ErrAuth)
	}
	deadline := time.Now().Add(timeout)
	for {
		if ctx.Err() != nil {
			return AuthOutcome{}, ctx.Err()
		}
		callCtx := ctx
		if until := time.Until(deadline); until <= 0 {
			return AuthOutcome{}, fmt.Errorf("%w: authentication timeout", ErrTimeout)
		} else {
			var cancel context.CancelFunc
			callCtx, cancel = context.WithTimeout(ctx, until)
			r, e := t.request(callCtx, wireCommand{Type: "auth", APIKey: key, AdapterInstance: adapter})
			cancel()
			if e != nil {
				return AuthOutcome{}, e
			}
			switch r.Type {
			case "auth_success":
				return authSuccess(r), nil
			case "auth_failure":
				return AuthOutcome{}, &AuthRejectedError{Message: r.Message}
			case "auth_pending":
				delay := durationMillis(*r.RetryAfter)
				if until := time.Until(deadline); delay > until {
					delay = until
				}
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return AuthOutcome{}, ctx.Err()
				case <-timer.C:
				}
			default:
				return AuthOutcome{}, fmt.Errorf("%w: expected auth response, got %s", ErrAuth, r.Type)
			}
		}
	}
}

func authenticateOnce(ctx context.Context, t *transport, key string, adapter *string, timeout time.Duration) (AuthOutcome, error) {
	if key == "" {
		return AuthOutcome{}, fmt.Errorf("%w: API key is required", ErrAuth)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	callCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	r, err := t.request(callCtx, wireCommand{Type: "auth", APIKey: key, AdapterInstance: adapter})
	if err != nil {
		return AuthOutcome{}, err
	}
	switch r.Type {
	case "auth_success":
		return authSuccess(r), nil
	case "auth_pending":
		return AuthOutcome{Pending: true, RetryAfter: durationMillis(*r.RetryAfter)}, nil
	case "auth_failure":
		return AuthOutcome{}, &AuthRejectedError{Message: r.Message}
	default:
		return AuthOutcome{}, fmt.Errorf("%w: expected auth response, got %s", ErrAuth, r.Type)
	}
}

func authSuccess(r wireResponse) AuthOutcome {
	return AuthOutcome{ClientID: r.ClientID, SessionTTL: time.Duration(*r.SessionTTL) * time.Second}
}
