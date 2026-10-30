// Package sink defines where processed webhook messages are delivered.
package sink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Sink delivers one JSON message to its final destination.
//
// Delivery is at-least-once: the same key may be delivered more than once (after
// a crash or a retry), so destinations should deduplicate on it. The key is
// deterministic, "<file name>:<line number>", and is the same on every attempt.
//
// Deliver must return an error wrapped in PermanentError when retrying cannot
// help (the destination rejected the message for good). Any other error is
// considered transient and is retried.
type Sink interface {
	Deliver(ctx context.Context, key string, line []byte) error
}

// PermanentError marks a delivery failure that must not be retried.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return "permanent: " + e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// IsPermanent reports whether err is, or wraps, a PermanentError.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// Noop accepts everything. It is the default when no destination is configured.
type Noop struct{}

// Deliver implements Sink.
func (Noop) Deliver(context.Context, string, []byte) error { return nil }

// HTTP forwards each message with a POST.
type HTTP struct {
	url    string
	client *http.Client
}

// NewHTTP builds a forwarder with a per-request timeout.
func NewHTTP(url string, timeout time.Duration) *HTTP {
	return &HTTP{
		url: url,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 16,
				IdleConnTimeout:     60 * time.Second,
			},
		},
	}
}

// Deliver implements Sink. 2xx is success; 408, 425, 429, 5xx and network errors
// are transient; every other status is permanent.
func (h *HTTP) Deliver(ctx context.Context, key string, line []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(line))
	if err != nil {
		return &PermanentError{Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	switch code := resp.StatusCode; {
	case code >= 200 && code < 300:
		return nil
	case code == http.StatusRequestTimeout, code == http.StatusTooEarly,
		code == http.StatusTooManyRequests, code >= 500:
		return fmt.Errorf("destination returned %d", code)
	default:
		return &PermanentError{Err: fmt.Errorf("destination rejected the message with %d", code)}
	}
}
