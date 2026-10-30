package server

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"

	"github.com/okzgn/wal-ingestion-engine-go/internal/wal"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type fakeAppender struct {
	mu       sync.Mutex
	payloads []string
	err      error
	delay    time.Duration
}

func (f *fakeAppender) Append(ctx context.Context, p []byte) error {
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", wal.ErrAckTimeout, ctx.Err())
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.payloads = append(f.payloads, string(p))
	return nil
}

func (f *fakeAppender) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.payloads...)
}

type fakeBacklog int64

func (b fakeBacklog) BacklogBytes() int64 { return int64(b) }

func newServer(t *testing.T, w Appender, mutate func(*Config)) *Server {
	t.Helper()
	cfg := Config{MaxMessageSize: 1 << 20, AckTimeout: time.Second}
	if mutate != nil {
		mutate(&cfg)
	}
	return New(cfg, w, nil, quiet)
}

func post(t *testing.T, s *Server, body string, hdr map[string]string) (int, string, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, "/webhook", strings.NewReader(body))
	req.Host = "ingest.test" // fasthttp (rightly) refuses HTTP/1.1 requests without Host
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := s.App().Test(req, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func TestHandler_ValidJSONIsAcknowledged(t *testing.T) {
	w := &fakeAppender{}
	s := newServer(t, w, nil)
	code, _, _ := post(t, s, `{"event":"payment","amount":100}`, nil)
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	if got := w.got(); len(got) != 1 || got[0] != `{"event":"payment","amount":100}` {
		t.Fatalf("stored %v", got)
	}
}

// Regression C2: pretty-printed JSON used to be acknowledged and then lost
// because its lines were not valid JSON on their own.
func TestHandler_MultilineStoredAsOneLine(t *testing.T) {
	w := &fakeAppender{}
	s := newServer(t, w, nil)
	code, _, _ := post(t, s, "{\n  \"id\": 2,\n  \"text\": \"multi\\nline\"\n}", nil)
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	got := w.got()
	if len(got) != 1 {
		t.Fatalf("stored %v", got)
	}
	if strings.ContainsAny(got[0], "\n\r") {
		t.Fatalf("stored payload still has line breaks: %q", got[0])
	}
	if got[0] != `{"id":2,"text":"multi\nline"}` {
		t.Fatalf("stored %q", got[0])
	}
}

func TestHandler_InvalidBodyIs400(t *testing.T) {
	for name, body := range map[string]string{
		"not json":   "not json",
		"empty":      "",
		"truncated":  `{"a":`,
		"two values": `{"a":1}{"b":2}`,
	} {
		w := &fakeAppender{}
		s := newServer(t, w, nil)
		code, _, _ := post(t, s, body, nil)
		if code != 400 {
			t.Errorf("%s: status %d, want 400", name, code)
		}
		if len(w.got()) != 0 {
			t.Errorf("%s: invalid body reached the WAL", name)
		}
	}
}

// Regression C3: bodies above the limit used to be accepted with 200 and then
// break the file they were stored in. This one goes through a real listener,
// because app.Test cannot reproduce the server-side body limit response.
func TestHandler_Oversize413(t *testing.T) {
	w := &fakeAppender{}
	s := newServer(t, w, func(c *Config) { c.MaxMessageSize = 1024 })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.App().Listener(ln, fiber.ListenConfig{DisableStartupMessage: true}) }()
	defer s.Shutdown(time.Second)

	big := `{"x":"` + strings.Repeat("a", 3000) + `"}`
	resp, err := http.Post("http://"+ln.Addr().String()+"/webhook", "application/json", strings.NewReader(big))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 413 {
		t.Fatalf("status %d, want 413", resp.StatusCode)
	}
	if len(w.got()) != 0 {
		t.Fatal("oversized body reached the WAL")
	}
}

func TestHandler_BodyExactlyAtLimitIsAccepted(t *testing.T) {
	w := &fakeAppender{}
	s := newServer(t, w, func(c *Config) { c.MaxMessageSize = 100 })
	body := `{"x":"` + strings.Repeat("a", 100-8) + `"}`
	if len(body) != 100 {
		t.Fatalf("test setup: len=%d", len(body))
	}
	if code, _, _ := post(t, s, body, nil); code != 200 {
		t.Fatalf("status %d", code)
	}
}

// Regression C4 at the HTTP level: a failing WAL must never produce a 200.
func TestHandler_WALFailureIs503NeverOK(t *testing.T) {
	for _, werr := range []error{
		wal.ErrUnavailable,
		fmt.Errorf("%w: input/output error", wal.ErrUnavailable),
		wal.ErrOverloaded,
		wal.ErrClosed,
		errors.New("anything else"),
	} {
		s := newServer(t, &fakeAppender{err: werr}, nil)
		code, _, hdr := post(t, s, `{"a":1}`, nil)
		if code != 503 {
			t.Errorf("%v: status %d, want 503", werr, code)
		}
		if hdr.Get("Retry-After") == "" {
			t.Errorf("%v: missing Retry-After", werr)
		}
	}
}

func TestHandler_AckTimeoutIs503(t *testing.T) {
	s := newServer(t, &fakeAppender{delay: time.Second}, func(c *Config) { c.AckTimeout = 20 * time.Millisecond })
	if code, _, _ := post(t, s, `{"a":1}`, nil); code != 503 {
		t.Fatalf("status %d, want 503", code)
	}
}

func TestHandler_BacklogFullIs503(t *testing.T) {
	w := &fakeAppender{}
	s := New(Config{MaxMessageSize: 1 << 20, MaxBacklogBytes: 100}, w, fakeBacklog(101), quiet)
	if code, _, _ := post(t, s, `{"a":1}`, nil); code != 503 {
		t.Fatalf("status %d, want 503", code)
	}
	if len(w.got()) != 0 {
		t.Fatal("message accepted despite a full backlog")
	}
	s = New(Config{MaxMessageSize: 1 << 20, MaxBacklogBytes: 100}, w, fakeBacklog(100), quiet)
	if code, _, _ := post(t, s, `{"a":1}`, nil); code != 200 {
		t.Fatalf("status %d at the limit, want 200", code)
	}
}

func TestHandler_CompressedBodyRejected(t *testing.T) {
	w := &fakeAppender{}
	s := newServer(t, w, nil)
	code, _, _ := post(t, s, `{"a":1}`, map[string]string{"Content-Encoding": "gzip"})
	if code != 415 {
		t.Fatalf("status %d, want 415", code)
	}
}

func TestHandler_OtherMethodsAndPaths(t *testing.T) {
	s := newServer(t, &fakeAppender{}, nil)
	req, _ := http.NewRequest(http.MethodGet, "/webhook", nil)
	req.Host = "ingest.test"
	resp, err := s.App().Test(req, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 405 {
		t.Fatalf("GET: %d, want 405", resp.StatusCode)
	}
	req, _ = http.NewRequest(http.MethodPost, "/nope", strings.NewReader("{}"))
	req.Host = "ingest.test"
	resp, err = s.App().Test(req, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 404 {
		t.Fatalf("unknown path: %d, want 404", resp.StatusCode)
	}
}

func TestHandler_RateLimit(t *testing.T) {
	s := newServer(t, &fakeAppender{}, func(c *Config) { c.RateLimitPerMinute = 3 })
	var codes []int
	for i := 0; i < 5; i++ {
		code, _, _ := post(t, s, `{"a":1}`, nil)
		codes = append(codes, code)
	}
	if fmt.Sprint(codes) != "[200 200 200 429 429]" {
		t.Fatalf("codes %v", codes)
	}
}

// ---- HMAC ----

func sign(secret, ts, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	if ts != "" {
		mac.Write([]byte(ts + "."))
	}
	mac.Write([]byte(body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestHMAC_Handler(t *testing.T) {
	w := &fakeAppender{}
	s := newServer(t, w, func(c *Config) { c.Secret = "s3cret" })
	body := `{"id": 1}` // note the space: the MAC covers the RAW body, not the compacted one

	if code, _, _ := post(t, s, body, nil); code != 401 {
		t.Errorf("missing signature: %d, want 401", code)
	}
	if code, _, _ := post(t, s, body, map[string]string{"X-Signature-256": sign("other", "", body)}); code != 401 {
		t.Errorf("wrong secret: %d, want 401", code)
	}
	if code, _, _ := post(t, s, body+" ", map[string]string{"X-Signature-256": sign("s3cret", "", body)}); code != 401 {
		t.Errorf("tampered body: %d, want 401", code)
	}
	if code, _, _ := post(t, s, body, map[string]string{"X-Signature-256": "sha256=zz"}); code != 401 {
		t.Errorf("malformed signature: %d, want 401", code)
	}
	if len(w.got()) != 0 {
		t.Fatal("unauthenticated request reached the WAL")
	}
	if code, _, _ := post(t, s, body, map[string]string{"X-Signature-256": sign("s3cret", "", body)}); code != 200 {
		t.Errorf("valid signature: %d, want 200", code)
	}
}

func TestVerifier_Timestamp(t *testing.T) {
	v := NewVerifier("k", true, time.Minute)
	now := time.Unix(1_700_000_000, 0)
	v.now = func() time.Time { return now }
	body := []byte(`{"a":1}`)
	ts := strconv.FormatInt(now.Unix(), 10)

	if err := v.Verify(sign("k", ts, string(body)), ts, body); err != nil {
		t.Fatalf("valid: %v", err)
	}
	if err := v.Verify(sign("k", ts, string(body)), "", body); !errors.Is(err, ErrBadTimestamp) {
		t.Errorf("missing timestamp: %v", err)
	}
	old := strconv.FormatInt(now.Add(-2*time.Minute).Unix(), 10)
	if err := v.Verify(sign("k", old, string(body)), old, body); !errors.Is(err, ErrBadTimestamp) {
		t.Errorf("stale timestamp (replay): %v", err)
	}
	future := strconv.FormatInt(now.Add(2*time.Minute).Unix(), 10)
	if err := v.Verify(sign("k", future, string(body)), future, body); !errors.Is(err, ErrBadTimestamp) {
		t.Errorf("future timestamp: %v", err)
	}
	// A valid MAC for another timestamp must not validate this one.
	if err := v.Verify(sign("k", old, string(body)), ts, body); !errors.Is(err, ErrBadSignature) {
		t.Errorf("timestamp not covered by the MAC: %v", err)
	}
}

func TestVerifier_AcceptsBareHex(t *testing.T) {
	v := NewVerifier("k", false, 0)
	body := []byte(`{}`)
	bare := strings.TrimPrefix(sign("k", "", string(body)), "sha256=")
	if err := v.Verify(bare, "", body); err != nil {
		t.Fatal(err)
	}
}

func TestHandler_ManyConcurrentAreAllStored(t *testing.T) {
	w := &fakeAppender{}
	s := newServer(t, w, nil)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buf bytes.Buffer
			fmt.Fprintf(&buf, "{\n \"i\": %d\n}", i)
			if code, _, _ := post(t, s, buf.String(), nil); code != 200 {
				t.Errorf("status %d", code)
			}
		}()
	}
	wg.Wait()
	if len(w.got()) != 50 {
		t.Fatalf("stored %d of 50", len(w.got()))
	}
}
