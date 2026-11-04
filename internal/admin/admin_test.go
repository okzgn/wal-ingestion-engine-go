package admin

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	_ "github.com/okzgn/wal-ingestion-engine-go/internal/metrics" // registers the expvar counters
)

func get(h http.Handler, path string) (int, string) {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	b, _ := io.ReadAll(rec.Result().Body)
	return rec.Code, string(b)
}

func TestHealthz(t *testing.T) {
	code, body := get(Handler(Options{}), "/healthz")
	if code != 200 || !strings.Contains(body, "ok") {
		t.Fatalf("%d %s", code, body)
	}
}

func TestReadyz_ReflectsEngineState(t *testing.T) {
	ready := true
	h := Handler(Options{Ready: func() (bool, string) {
		if ready {
			return true, ""
		}
		return false, "wal degraded"
	}})
	if code, _ := get(h, "/readyz"); code != 200 {
		t.Fatalf("ready: %d", code)
	}
	ready = false
	code, body := get(h, "/readyz")
	if code != 503 || !strings.Contains(body, "wal degraded") {
		t.Fatalf("not ready: %d %s", code, body)
	}
}

func TestMetricsExposeEngineCounters(t *testing.T) {
	code, body := get(Handler(Options{}), "/debug/vars")
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	for _, name := range []string{"accepted", "rejected", "fsync_count", "backlog_bytes", "delivered", "degraded"} {
		if !strings.Contains(body, `"`+name+`"`) {
			t.Errorf("metric %q missing from /debug/vars", name)
		}
	}
}

func TestPprofIsOptIn(t *testing.T) {
	if code, _ := get(Handler(Options{}), "/debug/pprof/"); code != 404 {
		t.Fatalf("pprof must be off by default, got %d", code)
	}
	if code, _ := get(Handler(Options{Pprof: true}), "/debug/pprof/"); code != 200 {
		t.Fatalf("pprof enabled: got %d", code)
	}
}

func TestStart_BindsAndServes(t *testing.T) {
	a, err := Start(Options{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Shutdown(t.Context())
	resp, err := http.Get("http://" + a.Addr() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if _, err := Start(Options{Addr: a.Addr()}); err == nil {
		t.Fatal("a busy port must be reported at start")
	}
}

// The command line can carry SINK_URL or paths; metrics are no place for it.
func TestMetricsDoNotExposeCommandLine(t *testing.T) {
	_, body := get(Handler(Options{}), "/debug/vars")
	if strings.Contains(body, `"cmdline"`) {
		t.Fatal("/debug/vars must not expose the command line")
	}
	var v map[string]any
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatalf("not valid JSON: %v\n%s", err, body)
	}
	if _, ok := v["memstats"]; !ok {
		t.Fatal("the other expvar values must still be served")
	}
}

func TestStart_ServesProvidedListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a, err := Start(Options{Listener: ln})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Shutdown(t.Context())
	if a.Addr() != ln.Addr().String() {
		t.Fatalf("addr %s, want %s", a.Addr(), ln.Addr())
	}
	resp, err := http.Get("http://" + a.Addr() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}
