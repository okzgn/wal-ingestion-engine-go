// Package e2e runs the real binary against a fake downstream and checks the
// guarantees end to end, including hard crashes (the process is killed, not
// asked to stop). Skipped with -short.
package e2e

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

var binPath string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	dir, err := os.MkdirTemp("", "ingestor-e2e")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binPath = filepath.Join(dir, "ingestor")
	if runtime.GOOS == "windows" {
		binPath += ".exe"
	}
	if out, err := exec.Command("go", "build", "-o", binPath, "../../cmd/ingestor").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// ---- fake downstream ----

type downstream struct {
	srv   *httptest.Server
	mu    sync.Mutex
	count map[string]int // id -> times delivered
	keys  map[string]int // idempotency key -> times delivered
	lines []string
	down  atomic.Bool
	delay atomic.Int64 // nanoseconds
}

func newDownstream(t *testing.T) *downstream {
	d := &downstream{count: map[string]int{}, keys: map[string]int{}}
	d.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if d.down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if dl := d.delay.Load(); dl > 0 {
			time.Sleep(time.Duration(dl))
		}
		body, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(body, &m)
		d.mu.Lock()
		d.lines = append(d.lines, string(body))
		if id, ok := m["id"].(string); ok {
			d.count[id]++
		}
		d.keys[r.Header.Get("Idempotency-Key")]++
		d.mu.Unlock()
	}))
	t.Cleanup(d.srv.Close)
	return d
}

func (d *downstream) delivered(id string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.count[id]
}

func (d *downstream) totalLines() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.lines)
}

// ---- process under test ----

type instance struct {
	t      *testing.T
	cmd    *exec.Cmd
	addr   string
	admin  string
	stderr *syncBuffer
	done   chan struct{}
	exit   error
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func start(t *testing.T, dataDir, sinkURL string, extra ...string) *instance {
	t.Helper()
	if testing.Short() {
		t.Skip("e2e skipped with -short")
	}
	in := &instance{t: t, addr: freeAddr(t), admin: freeAddr(t), stderr: &syncBuffer{}, done: make(chan struct{})}
	in.cmd = exec.Command(binPath)
	in.cmd.Env = append(os.Environ(),
		"LISTEN_ADDR="+in.addr,
		"ADMIN_ADDR="+in.admin,
		"DATA_DIR="+dataDir,
		"SINK_URL="+sinkURL,
		"ROTATION_INTERVAL=150ms",
		"MAX_FILE_SIZE=64KiB",
		"SINK_MAX_ATTEMPTS=1",
		"FILE_RETRY_BASE=30ms",
		"FILE_RETRY_MAX=100ms",
		"WORKERS=4",
		"LOG_LEVEL=info",
	)
	in.cmd.Env = append(in.cmd.Env, extra...)
	in.cmd.Stderr = in.stderr
	if err := in.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { in.exit = in.cmd.Wait(); close(in.done) }()
	t.Cleanup(func() { in.kill() })

	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case <-in.done:
			t.Fatalf("process exited during startup: %v\n%s", in.exit, in.stderr.String())
		default:
		}
		if resp, err := http.Get("http://" + in.admin + "/readyz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				// readyz comes up before the HTTP listener on a busy machine: check both.
				if r2, err := http.Post("http://"+in.addr+"/webhook", "application/json", strings.NewReader("{}")); err == nil {
					r2.Body.Close()
					return in
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("not ready in time\n%s", in.stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// kill is a hard crash: no signal handler, no flush.
func (in *instance) kill() {
	_ = in.cmd.Process.Kill()
	<-in.done
}

func (in *instance) post(body string) (int, string) {
	resp, err := http.Post("http://"+in.addr+"/webhook", "application/json", strings.NewReader(body))
	if err != nil {
		return -1, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// sendMany posts n messages with the given concurrency and returns the ids that
// were acknowledged with 200. stop (optional) is closed to abort early.
func (in *instance) sendMany(prefix string, n, conc int, stop <-chan struct{}) []string {
	var (
		mu    sync.Mutex
		acked []string
		next  atomic.Int64
		wg    sync.WaitGroup
	)
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("%s-%d", prefix, i)
				code, _ := in.post(fmt.Sprintf(`{"id":%q}`, id))
				if code == 200 {
					mu.Lock()
					acked = append(acked, id)
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return acked
}

// ---- tests ----

// The four reproductions of the original code, through the real binary.
func TestE2E_OriginalRegressions(t *testing.T) {
	ds := newDownstream(t)
	dir := t.TempDir()
	in := start(t, dir, ds.srv.URL)

	// C2: pretty-printed JSON is stored as ONE line and delivered.
	if code, _ := in.post("{\n  \"id\": \"multi\",\n  \"x\": \"a\\nb\"\n}"); code != 200 {
		t.Fatalf("multiline: %d", code)
	}
	// C2: garbage and empty bodies are refused, not acknowledged.
	if code, _ := in.post("not json"); code != 400 {
		t.Fatalf("invalid json: %d", code)
	}
	if code, _ := in.post(""); code != 400 {
		t.Fatalf("empty: %d", code)
	}
	// C3: a 3 MB body is refused up front.
	big := `{"id":"big","pad":"` + strings.Repeat("x", 3<<20) + `"}`
	if code, _ := in.post(big); code != 413 {
		t.Fatalf("oversize: %d, want 413", code)
	}
	waitFor(t, "multiline message", 10*time.Second, func() bool { return ds.delivered("multi") == 1 })

	// C1: the backend is down while messages are accepted; nothing may be lost.
	ds.down.Store(true)
	var ids []string
	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("m%d", i)
		ids = append(ids, id)
		if code, _ := in.post(fmt.Sprintf(`{"id":%q}`, id)); code != 200 {
			t.Fatalf("post %s: %d", id, code)
		}
	}
	time.Sleep(600 * time.Millisecond) // several rotations and failed rounds
	ds.down.Store(false)
	waitFor(t, "backlog delivery after the backend recovers", 15*time.Second, func() bool {
		for _, id := range ids {
			if ds.delivered(id) == 0 {
				return false
			}
		}
		return true
	})
	for _, id := range ids {
		if n := ds.delivered(id); n != 1 {
			t.Errorf("%s delivered %d times, want exactly 1 (no crash happened)", id, n)
		}
	}

	// Nothing was dead-lettered by mistake.
	if entries, _ := os.ReadDir(filepath.Join(dir, "failed")); len(entries) != 0 {
		t.Errorf("failed/ should be empty, has %v", entries)
	}
}

// Kill -9 in the middle of heavy traffic and while the backlog is being
// delivered: every message that got a 200 must reach the downstream.
func TestE2E_CrashRecovery(t *testing.T) {
	ds := newDownstream(t)
	ds.down.Store(true) // build a backlog first
	dir := t.TempDir()
	in := start(t, dir, ds.srv.URL)

	acked := in.sendMany("a", 1500, 32, nil)
	if len(acked) < 1000 {
		t.Fatalf("only %d messages acknowledged\n%s", len(acked), in.stderr.String())
	}
	in.kill() // crash #1: backlog on disk, nothing delivered yet

	ds.down.Store(false)
	ds.delay.Store(int64(2 * time.Millisecond))
	in2 := start(t, dir, ds.srv.URL)
	waitFor(t, "some deliveries after restart", 15*time.Second, func() bool { return ds.totalLines() > 200 })
	in2.kill() // crash #2: in the middle of delivering a file

	ds.delay.Store(0)
	in3 := start(t, dir, ds.srv.URL)
	waitFor(t, "every acknowledged message delivered", 60*time.Second, func() bool {
		for _, id := range acked {
			if ds.delivered(id) == 0 {
				return false
			}
		}
		return true
	})

	dups, lost := 0, 0
	for _, id := range acked {
		switch n := ds.delivered(id); {
		case n == 0:
			lost++
		case n > 1:
			dups += n - 1
		}
	}
	t.Logf("acked=%d lost=%d duplicates=%d", len(acked), lost, dups)
	if lost != 0 {
		t.Fatalf("%d acknowledged messages were lost", lost)
	}
	// At-least-once: duplicates are allowed but bounded by the checkpoint interval
	// of the files that were in flight at the crash (4 workers x 50 lines).
	if dups > 4*50 {
		t.Errorf("%d duplicates exceed the bound of 200", dups)
	}
	in3.kill()
}

// Two live instances on one data directory would steal each other's files.
func TestE2E_SecondInstanceIsRefused(t *testing.T) {
	ds := newDownstream(t)
	dir := t.TempDir()
	first := start(t, dir, ds.srv.URL)
	_ = first

	cmd := exec.Command(binPath)
	cmd.Env = append(os.Environ(), "LISTEN_ADDR="+freeAddr(t), "ADMIN_ADDR="+freeAddr(t), "DATA_DIR="+dir)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("the second instance must refuse to start")
	}
	if !strings.Contains(string(out), "locked") {
		t.Fatalf("expected a lock error, got:\n%s", out)
	}
}

func TestE2E_InvalidConfigFailsFast(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	cmd := exec.Command(binPath)
	cmd.Env = append(os.Environ(), "WORKERS=zero")
	out, err := cmd.CombinedOutput()
	exitErr, ok := err.(*exec.ExitError)
	if !ok || exitErr.ExitCode() != 2 {
		t.Fatalf("want exit code 2, got %v", err)
	}
	if !strings.Contains(string(out), "WORKERS") {
		t.Fatalf("the message must name the setting:\n%s", out)
	}
}

// SIGTERM under load: clean exit, and a restart finds everything acknowledged.
func TestE2E_GracefulShutdown(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Go cannot deliver SIGTERM to a child on Windows")
	}
	ds := newDownstream(t)
	dir := t.TempDir()
	in := start(t, dir, ds.srv.URL, "SHUTDOWN_TIMEOUT=10s")

	stop := make(chan struct{})
	var acked []string
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); acked = in.sendMany("g", 100000, 16, stop) }()
	time.Sleep(700 * time.Millisecond)

	if err := in.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-in.done:
	case <-time.After(15 * time.Second):
		t.Fatalf("did not stop in time\n%s", in.stderr.String())
	}
	close(stop)
	wg.Wait()
	if in.exit != nil {
		t.Fatalf("unclean exit: %v\n%s", in.exit, in.stderr.String())
	}
	if !strings.Contains(in.stderr.String(), "stopped") {
		t.Fatalf("no final log line:\n%s", in.stderr.String())
	}

	in2 := start(t, dir, ds.srv.URL)
	waitFor(t, "all acknowledged messages", 60*time.Second, func() bool {
		for _, id := range acked {
			if ds.delivered(id) == 0 {
				return false
			}
		}
		return true
	})
	t.Logf("acknowledged before shutdown: %d", len(acked))
	in2.kill()
}
