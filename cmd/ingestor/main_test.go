package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/okzgn/wal-ingestion-engine-go/internal/config"
	"github.com/okzgn/wal-ingestion-engine-go/internal/pipeline"
	"github.com/okzgn/wal-ingestion-engine-go/internal/server"
)

// client keeps its connections alive. The default client keeps two per host:
// under load every other request opens a new socket, and on Windows a few
// thousand of them in TIME_WAIT exhaust the ephemeral ports.
var client = &http.Client{
	Timeout:   30 * time.Second,
	Transport: &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 64, IdleConnTimeout: 30 * time.Second},
}

// lookup mimics os.LookupEnv over a map.
func lookup(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestHealthURL(t *testing.T) {
	for in, want := range map[string]string{
		"127.0.0.1:9090": "http://127.0.0.1:9090/healthz",
		":9090":          "http://127.0.0.1:9090/healthz",
		"0.0.0.0:9090":   "http://127.0.0.1:9090/healthz",
		"[::]:9090":      "http://127.0.0.1:9090/healthz",
		"10.0.0.5:81":    "http://10.0.0.5:81/healthz",
	} {
		if got := healthURL(in); got != want {
			t.Errorf("healthURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestHealthcheckExitCodes(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
	defer srv.Close()
	env := lookup(map[string]string{"ADMIN_ADDR": strings.TrimPrefix(srv.URL, "http://")})

	var buf bytes.Buffer
	if code := run([]string{"healthcheck"}, env, &buf); code != 0 {
		t.Fatalf("healthy: exit %d (%s)", code, buf.String())
	}
	status = http.StatusServiceUnavailable
	if code := run([]string{"healthcheck"}, env, &buf); code != 1 {
		t.Fatalf("unhealthy: exit %d", code)
	}
	srv.Close()
	if code := run([]string{"healthcheck"}, env, &buf); code != 1 {
		t.Fatalf("unreachable: exit %d", code)
	}
	buf.Reset()
	if code := run([]string{"healthcheck"}, lookup(map[string]string{"ADMIN_ADDR": ""}), &buf); code != 1 || !strings.Contains(buf.String(), "disabled") {
		t.Fatalf("disabled admin: exit %d, output %q", code, buf.String())
	}
}

// Regression R9: the HTTP limit and the file reader's limit are two constants
// in two packages; if they drift apart, a message can be acknowledged and then
// be too long to deliver.
func TestMessageLimitNeverExceedsTheReaders(t *testing.T) {
	if config.MaxMessageSizeLimit > pipeline.DefaultMaxLineSize {
		t.Fatalf("MAX_MESSAGE_SIZE may reach %d but the reader delivers lines up to %d",
			config.MaxMessageSizeLimit, pipeline.DefaultMaxLineSize)
	}
	c, err := config.Load(nil, lookup(map[string]string{"MAX_MESSAGE_SIZE": "16MiB", "MAX_BATCH_BYTES": "16MiB"}))
	if err != nil || c.MaxMessageSize != config.MaxMessageSizeLimit {
		t.Fatalf("the limit itself must be accepted: %v", err)
	}
}

// The scheme names are spelled in two packages (config validates them, server
// implements them): every name the configuration accepts must be implemented.
func TestSignatureSchemesAgree(t *testing.T) {
	for _, scheme := range []string{"generic", "github", "stripe", "slack", "GitHub"} {
		c, err := config.Load(nil, lookup(map[string]string{"WEBHOOK_SECRET": "s", "WEBHOOK_SIGNATURE_SCHEME": scheme}))
		if err != nil {
			t.Fatalf("%s: %v", scheme, err)
		}
		if !server.ValidScheme(c.WebhookSignatureScheme) {
			t.Errorf("config accepts %q but the server does not implement it", scheme)
		}
	}
}

func TestRun_ConfigErrorExitsWithTwo(t *testing.T) {
	var buf bytes.Buffer
	if code := run(nil, lookup(map[string]string{"WORKERS": "0"}), &buf); code != 2 || !strings.Contains(buf.String(), "WORKERS") {
		t.Fatalf("exit %d, output %q", code, buf.String())
	}
}

func TestRun_HelpListsSettings(t *testing.T) {
	var buf bytes.Buffer
	if code := run([]string{"-h"}, lookup(nil), &buf); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if !strings.Contains(buf.String(), "WEBHOOK_SECRET") || !strings.Contains(buf.String(), "DATA_DIR") {
		t.Fatalf("help output incomplete: %s", buf.String())
	}
}

// A busy port is reported before the data directory is locked, so a failed
// start never leaves a lock or moves files around.
func TestServe_BusyPortFailsBeforeTouchingData(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	dir := t.TempDir()
	cfg, err := config.Load(nil, lookup(map[string]string{"LISTEN_ADDR": l.Addr().String(), "ADMIN_ADDR": "", "DATA_DIR": dir}))
	if err != nil {
		t.Fatal(err)
	}
	err = serve(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("want a listen error, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "spool.lock")); !os.IsNotExist(err) {
		t.Fatal("the data directory must not be touched when the port is busy")
	}
}

// Graceful shutdown, exercised in-process (Go cannot send SIGTERM to a child on
// Windows): cancelling the context under load must stop cleanly, and a restart
// must find every message that was acknowledged.
func TestServe_GracefulShutdownLosesNothing(t *testing.T) {
	var mu sync.Mutex
	delivered := map[string]int{}
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m struct {
			ID string `json:"id"`
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &m)
		mu.Lock()
		delivered[m.ID]++
		mu.Unlock()
	}))
	defer down.Close()

	dir := t.TempDir()
	addr, admin := freeAddrs(t)
	cfg, err := config.Load(nil, lookup(map[string]string{
		"LISTEN_ADDR": addr, "ADMIN_ADDR": admin, "DATA_DIR": dir, "SINK_URL": down.URL,
		"ROTATION_INTERVAL": "100ms", "SHUTDOWN_TIMEOUT": "10s", "LOG_LEVEL": "error",
	}))
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	start := func() (cancel context.CancelFunc, done chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done = make(chan error, 1)
		go func() { done <- serve(ctx, cfg, log) }()
		deadline := time.Now().Add(10 * time.Second)
		for {
			if resp, err := client.Get("http://" + admin + "/readyz"); err == nil {
				resp.Body.Close()
				if resp.StatusCode == 200 {
					if r2, err := client.Post("http://"+addr+"/webhook", "application/json", strings.NewReader("{}")); err == nil {
						r2.Body.Close()
						return cancel, done
					}
				}
			}
			if time.Now().After(deadline) {
				t.Fatal("server not ready in time")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	cancel, done := start()
	var (
		ackMu sync.Mutex
		acked []string
		wg    sync.WaitGroup
		stop  = make(chan struct{})
	)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("w%d-%d", w, i)
				resp, err := client.Post("http://"+addr+"/webhook", "application/json", strings.NewReader(fmt.Sprintf(`{"id":%q}`, id)))
				if err != nil {
					continue
				}
				resp.Body.Close()
				if resp.StatusCode == 200 {
					ackMu.Lock()
					acked = append(acked, id)
					ackMu.Unlock()
				}
			}
		}()
	}
	time.Sleep(500 * time.Millisecond)

	cancel() // what SIGTERM does
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned %v after a graceful shutdown", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("graceful shutdown did not finish in time")
	}
	close(stop)
	wg.Wait()
	if len(acked) == 0 {
		t.Fatal("nothing was acknowledged before the shutdown")
	}

	// Restart: whatever the first run did not finish is recovered and delivered.
	cancel2, done2 := start()
	defer func() { cancel2(); <-done2 }()
	deadline := time.Now().Add(30 * time.Second)
	for {
		mu.Lock()
		missing := 0
		for _, id := range acked {
			if delivered[id] == 0 {
				missing++
			}
		}
		mu.Unlock()
		if missing == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d acknowledged messages never reached the backend", missing, len(acked))
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("acknowledged before shutdown: %d, all delivered", len(acked))
}

// freeAddrs returns two distinct free addresses. Both listeners are held open
// until both are known: asking for one port, releasing it and asking again can
// return the same port twice.
func freeAddrs(t *testing.T) (string, string) {
	t.Helper()
	a, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	return a.Addr().String(), b.Addr().String()
}
