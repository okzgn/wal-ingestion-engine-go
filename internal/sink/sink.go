// Package sink defines where processed webhook messages are delivered.
package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
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

// DefaultPermanentStatuses are the responses that mean the destination rejected
// the message itself, so retrying the same bytes cannot help: 400 Bad Request,
// 413 Content Too Large, 415 Unsupported Media Type and 422 Unprocessable
// Content. Every other failure is transient on purpose: a 401/403 after a
// credential rotation, a 404 while a route is being deployed, a 405 or a
// redirect all come from a misconfiguration that an operator fixes, and
// dead-lettering a whole backlog meanwhile would be far costlier than retrying.
var DefaultPermanentStatuses = []int{
	http.StatusBadRequest,
	http.StatusRequestEntityTooLarge,
	http.StatusUnsupportedMediaType,
	http.StatusUnprocessableEntity,
}

// HTTPConfig configures the HTTP forwarder.
type HTTPConfig struct {
	URL     string
	Timeout time.Duration // per delivery; 0 = 10s

	// MaxConns is the number of idle connections kept to the destination. Make
	// it at least the number of workers so every worker keeps a warm one.
	MaxConns int

	// Authorization, when not empty, is sent verbatim as the Authorization
	// header (for example "Bearer <token>").
	Authorization string

	// Headers are added to every delivery. Content-Type defaults to
	// application/json and may be overridden here; Idempotency-Key may not.
	Headers http.Header

	// PermanentStatuses replaces DefaultPermanentStatuses when not nil.
	PermanentStatuses []int

	// KeyPointer, when not empty, is a JSON Pointer (RFC 6901, for example
	// "/id") to the sender's own identifier of the message. Its value becomes
	// the Idempotency-Key, so the destination can also discard a message the
	// sender delivered twice (after a timeout or a 503 it retries, and the
	// engine stores the retry as a new message with a new engine key). A
	// message where the pointer finds no usable value keeps the engine key.
	KeyPointer string
}

// HTTP forwards each message with a POST.
type HTTP struct {
	cfg       HTTPConfig
	client    *http.Client
	permanent []int
	pointer   []string // parsed KeyPointer; nil = use the engine key
}

// DeliveryIDHeader always carries the engine key, "<file>:<line>": it names
// one stored message, and is the same on every attempt to deliver it.
const DeliveryIDHeader = "X-Delivery-Id"

// NewHTTP builds a forwarder. Redirects are never followed: a 301, 302 or 303
// would turn the POST into a body-less GET, and a 200 on that GET would count
// as a delivery the destination never received.
func NewHTTP(cfg HTTPConfig) *HTTP {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 16
	}
	perm := cfg.PermanentStatuses
	if perm == nil {
		perm = DefaultPermanentStatuses
	}
	pointer, _ := ParsePointer(cfg.KeyPointer) // validated by the configuration
	return &HTTP{
		cfg:       cfg,
		pointer:   pointer,
		permanent: slices.Clone(perm),
		client: &http.Client{
			Timeout: cfg.Timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
			Transport: &http.Transport{
				Proxy:               http.ProxyFromEnvironment,
				MaxIdleConns:        cfg.MaxConns,
				MaxIdleConnsPerHost: cfg.MaxConns,
				IdleConnTimeout:     60 * time.Second,
				TLSHandshakeTimeout: 10 * time.Second,
				ForceAttemptHTTP2:   true,
			},
		},
	}
}

// ParsePointer splits a JSON Pointer (RFC 6901) into its reference tokens.
// The empty pointer means "no pointer" and yields nil.
func ParsePointer(p string) ([]string, error) {
	if p == "" {
		return nil, nil
	}
	if !strings.HasPrefix(p, "/") {
		return nil, fmt.Errorf("JSON pointer %q must start with '/'", p)
	}
	tokens := strings.Split(p[1:], "/")
	for i, t := range tokens {
		for j := 0; j < len(t); j++ {
			if t[j] == '~' && (j+1 >= len(t) || (t[j+1] != '0' && t[j+1] != '1')) {
				return nil, fmt.Errorf("JSON pointer %q: '~' must be followed by 0 or 1", p)
			}
		}
		tokens[i] = strings.ReplaceAll(strings.ReplaceAll(t, "~1", "/"), "~0", "~")
	}
	return tokens, nil
}

// maxMessageKey bounds a key taken from the message: it travels in a header.
const maxMessageKey = 200

// MessageKey resolves pointer inside the JSON message and returns the value
// as an idempotency key. It reports false when there is no pointer, when the
// value is missing or is not a string or a number, or when it could not go in
// a header (empty, too long, or with characters a header value cannot hold).
func MessageKey(line []byte, pointer []string) (string, bool) {
	if pointer == nil {
		return "", false
	}
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber() // keep large identifiers exact
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return "", false
	}
	for _, token := range pointer {
		switch node := doc.(type) {
		case map[string]any:
			next, ok := node[token]
			if !ok {
				return "", false
			}
			doc = next
		case []any:
			i, err := strconv.Atoi(token)
			if err != nil || i < 0 || i >= len(node) || (len(token) > 1 && token[0] == '0') {
				return "", false
			}
			doc = node[i]
		default:
			return "", false
		}
	}
	var key string
	switch v := doc.(type) {
	case string:
		key = v
	case json.Number:
		key = v.String()
	default:
		return "", false
	}
	if key == "" || len(key) > maxMessageKey {
		return "", false
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x21 || key[i] > 0x7e { // visible ASCII only
			return "", false
		}
	}
	return key, true
}

// Deliver implements Sink. 2xx is success; a status in PermanentStatuses is
// permanent; everything else, redirects and network errors included, is
// transient.
func (h *HTTP) Deliver(ctx context.Context, key string, line []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.cfg.URL, bytes.NewReader(line))
	if err != nil {
		return &PermanentError{Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	for name, values := range h.cfg.Headers {
		for i, v := range values {
			if i == 0 {
				req.Header.Set(name, v)
			} else {
				req.Header.Add(name, v)
			}
		}
	}
	if h.cfg.Authorization != "" {
		req.Header.Set("Authorization", h.cfg.Authorization)
	}
	req.Header.Set(DeliveryIDHeader, key)
	if own, ok := MessageKey(line, h.pointer); ok {
		key = own
	}
	req.Header.Set("Idempotency-Key", key)

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	code := resp.StatusCode
	switch {
	case code >= 200 && code < 300:
		return nil
	case code >= 300 && code < 400:
		return fmt.Errorf("destination answered %d redirecting to %q; redirects are not followed because the body would be dropped, fix SINK_URL",
			code, resp.Header.Get("Location"))
	case slices.Contains(h.permanent, code):
		return &PermanentError{Err: fmt.Errorf("destination rejected the message with %d", code)}
	default:
		return fmt.Errorf("destination returned %d", code)
	}
}
