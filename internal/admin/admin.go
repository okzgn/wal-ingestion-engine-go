// Package admin serves the operational endpoints on a separate listener (by
// default bound to localhost only): liveness, readiness, expvar metrics and,
// optionally, pprof. It uses only net/http so it keeps working when the
// webhook server is saturated.
package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

// Options configures the admin server.
type Options struct {
	Addr string
	// Listener, when set, is served instead of binding Addr. Binding it up
	// front lets a busy port fail before the engine touches its data directory.
	Listener net.Listener
	Pprof    bool
	// Ready reports whether the engine can take traffic, with a reason when not.
	Ready  func() (ok bool, reason string)
	Logger *slog.Logger
}

// Admin is a running admin listener.
type Admin struct {
	srv *http.Server
	ln  net.Listener
}

// Start binds the listener synchronously (so a busy port is reported at once)
// and serves in the background.
func Start(o Options) (*Admin, error) {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	ln := o.Listener
	if ln == nil {
		var err error
		if ln, err = net.Listen("tcp", o.Addr); err != nil {
			return nil, err
		}
	}
	a := &Admin{ln: ln, srv: &http.Server{
		Handler:           Handler(o),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       60 * time.Second,
		// No WriteTimeout: /debug/pprof/profile streams for 30s by design.
	}}
	go func() {
		if err := a.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			o.Logger.Error("admin server stopped", "err", err)
		}
	}()
	return a, nil
}

// Addr is the bound address.
func (a *Admin) Addr() string { return a.ln.Addr().String() }

// Shutdown stops the listener.
func (a *Admin) Shutdown(ctx context.Context) error { return a.srv.Shutdown(ctx) }

// Handler builds the admin routes (exported for tests).
func Handler(o Options) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		ok, reason := true, ""
		if o.Ready != nil {
			ok, reason = o.Ready()
		}
		if !ok {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ready": false, "reason": reason})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ready": true})
	})
	mux.HandleFunc("GET /debug/vars", vars)
	if o.Pprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return mux
}

// vars is expvar.Handler without "cmdline": the command line can carry a
// SINK_URL or a data path that has no business on a metrics endpoint.
func vars(w http.ResponseWriter, _ *http.Request) {
	var b bytes.Buffer
	b.WriteString("{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == "cmdline" {
			return
		}
		if !first {
			b.WriteString(",\n")
		}
		first = false
		fmt.Fprintf(&b, "%q: %s", kv.Key, kv.Value)
	})
	b.WriteString("\n}\n")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(b.Bytes())
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
