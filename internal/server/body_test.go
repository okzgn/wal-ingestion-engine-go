package server

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
)

// serve runs the server on a real listener: app.Test hands the whole request
// over at once and cannot show how a body is read off the wire.
func serve(t *testing.T, s *Server) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.App().Listener(ln, fiber.ListenConfig{DisableStartupMessage: true}) }()
	t.Cleanup(func() { _ = s.Shutdown(time.Second) })
	return "http://" + ln.Addr().String() + "/webhook"
}

// chunked hides the length of the body, so the client sends it chunked.
func chunked(body string) io.Reader { return io.NopCloser(strings.NewReader(body)) }

func do(t *testing.T, url string, body io.Reader) (int, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// A body far larger than what the server pre-reads must arrive whole.
func TestBody_LargeBodyIsStoredIntact(t *testing.T) {
	w := &fakeAppender{}
	url := serve(t, newServer(t, w, nil))
	body := `{"pad":"` + strings.Repeat("z", 300_000) + `"}`
	for name, r := range map[string]io.Reader{"content-length": strings.NewReader(body), "chunked": chunked(body)} {
		if code, msg := do(t, url, r); code != 200 {
			t.Fatalf("%s: status %d (%s)", name, code, msg)
		}
	}
	got := w.got()
	if len(got) != 2 || got[0] != body || got[1] != body {
		t.Fatalf("stored %d messages; the body must be stored byte for byte", len(got))
	}
}

// A chunked body does not declare its size: it is cut off one byte past the
// limit instead of being buffered until the client stops sending.
func TestBody_ChunkedOversizeIs413(t *testing.T) {
	w := &fakeAppender{}
	url := serve(t, newServer(t, w, func(c *Config) { c.MaxMessageSize = 1024 }))
	if code, _ := do(t, url, chunked(`{"x":"`+strings.Repeat("a", 50_000)+`"}`)); code != 413 {
		t.Fatalf("status %d, want 413", code)
	}
	if code, _ := do(t, url, chunked(`{"x":"`+strings.Repeat("a", 500)+`"}`)); code != 200 {
		t.Fatalf("a chunked body within the limit: status %d, want 200", code)
	}
	if len(w.got()) != 1 {
		t.Fatalf("stored %d messages, want 1", len(w.got()))
	}
}

// MAX_CONNECTIONS x MAX_MESSAGE_SIZE bytes could be buffered before a single
// signature was checked. Bodies are now admitted against a budget first.
func TestBody_MemoryBudgetSheds503(t *testing.T) {
	w := &fakeAppender{delay: 400 * time.Millisecond}
	s := newServer(t, w, func(c *Config) {
		c.MaxMessageSize = 1024
		c.MaxBodyMemory = 1500
		c.AckTimeout = 2 * time.Second
	})
	url := serve(t, s)
	body := `{"x":"` + strings.Repeat("a", 1000-8) + `"}`

	first := make(chan int, 1)
	go func() {
		resp, err := http.Post(url, "application/json", strings.NewReader(body))
		if err != nil {
			first <- -1
			return
		}
		resp.Body.Close()
		first <- resp.StatusCode
	}()
	deadline := time.Now().Add(2 * time.Second)
	for s.bodyMem.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the first request never reserved its body")
		}
		time.Sleep(time.Millisecond)
	}

	// 1000 bytes are reserved; another 1000 would exceed the budget of 1500.
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	msg, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 503 || !strings.Contains(string(msg), "body_memory_full") {
		t.Fatalf("status %d (%s), want 503 body_memory_full", resp.StatusCode, msg)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("missing Retry-After")
	}
	// A small one still fits.
	if code, _ := do(t, url, strings.NewReader(`{"small":1}`)); code != 200 {
		t.Fatalf("a body that fits the budget: status %d, want 200", code)
	}

	if code := <-first; code != 200 {
		t.Fatalf("first request: status %d", code)
	}
	for s.bodyMem.Load() != 0 {
		if time.Now().After(deadline.Add(3 * time.Second)) {
			t.Fatalf("budget not returned: %d bytes still reserved", s.bodyMem.Load())
		}
		time.Sleep(time.Millisecond)
	}
	if code, _ := do(t, url, strings.NewReader(body)); code != 200 {
		t.Fatalf("after the budget is free: status %d, want 200", code)
	}
}

// A client that declares a body and then stalls holds its reservation only
// until the read timeout, never for ever.
func TestBody_StalledBodyReleasesItsReservation(t *testing.T) {
	s := newServer(t, &fakeAppender{}, func(c *Config) {
		c.ReadTimeout = 200 * time.Millisecond
		c.MaxBodyMemory = 1 << 20
	})
	url := serve(t, s)
	conn, err := net.Dial("tcp", strings.TrimSuffix(strings.TrimPrefix(url, "http://"), "/webhook"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// 20000 bytes declared (more than the server pre-reads), 10 sent.
	if _, err := io.WriteString(conn, "POST /webhook HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 20000\r\n\r\n{\"a\":\"bbbb"); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	start := time.Now()
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode == 200 {
			t.Fatal("a truncated body was acknowledged")
		}
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("the stalled request was held for %s; the read timeout is 200 ms", elapsed)
	}
	deadline := time.Now().Add(2 * time.Second)
	for s.bodyMem.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d bytes still reserved by a dead request", s.bodyMem.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
}
