// Command ingestor is a crash-resilient webhook ingestion engine: it answers
// 200 only after a message is fsynced to a write-ahead log, then delivers it to
// a downstream sink at its own pace, at-least-once.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/okzgn/wal-ingestion-engine-go/internal/admin"
	"github.com/okzgn/wal-ingestion-engine-go/internal/config"
	"github.com/okzgn/wal-ingestion-engine-go/internal/pipeline"
	"github.com/okzgn/wal-ingestion-engine-go/internal/server"
	"github.com/okzgn/wal-ingestion-engine-go/internal/sink"
	"github.com/okzgn/wal-ingestion-engine-go/internal/spool"
	"github.com/okzgn/wal-ingestion-engine-go/internal/wal"
)

func main() {
	os.Exit(run(os.Args[1:], os.LookupEnv, os.Stderr))
}

// run is the program. lookup follows os.LookupEnv: a variable set to the empty
// string is applied as empty, so "ADMIN_ADDR=" really disables the admin port.
func run(args []string, lookup func(string) (string, bool), stderr io.Writer) int {
	// "ingestor healthcheck" lets container runtimes probe a binary that has no
	// shell or curl (distroless images).
	if len(args) == 1 && args[0] == "healthcheck" {
		return healthcheck(lookup, stderr)
	}
	cfg, err := config.Load(args, lookup)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = fmt.Fprintf(stderr, "Usage: ingestor [flags]\n\nEvery flag has an environment variable; flags win.\n\n%s", config.Usage())
			return 0
		}
		_, _ = fmt.Fprintf(stderr, "configuration error:\n%v\n\nRun with -h to list every setting.\n", err)
		return 2
	}
	log := newLogger(cfg, stderr)
	if err := serve(context.Background(), cfg, log); err != nil {
		log.Error("fatal", "err", err)
		return 1
	}
	return 0
}

// healthcheck probes the local admin /healthz endpoint and returns an exit code.
func healthcheck(lookup func(string) (string, bool), stderr io.Writer) int {
	addr, set := lookup("ADMIN_ADDR")
	if !set {
		addr = "127.0.0.1:9090"
	}
	if addr == "" {
		_, _ = fmt.Fprintln(stderr, "healthcheck: the admin listener is disabled (ADMIN_ADDR is empty)")
		return 1
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(healthURL(addr))
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "healthcheck: %v\n", err)
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		_, _ = fmt.Fprintf(stderr, "healthcheck: status %d\n", resp.StatusCode)
		return 1
	}
	return 0
}

// healthURL turns a listen address into a URL that can be dialled locally.
func healthURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://" + addr + "/healthz"
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/healthz"
}

func newLogger(cfg *config.Config, w io.Writer) *slog.Logger {
	var level slog.Level
	_ = level.UnmarshalText([]byte(cfg.LogLevel))
	opts := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(cfg.LogFormat, "json") {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}

func serve(parent context.Context, cfg *config.Config, log *slog.Logger) error {
	// 1. Listeners first: a busy port is the most common start-up error and
	//    must be reported before the data directory is locked and touched.
	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.ListenAddr, err)
	}
	defer func() { _ = ln.Close() }()
	var adminLn net.Listener
	if cfg.AdminAddr != "" {
		if adminLn, err = net.Listen("tcp", cfg.AdminAddr); err != nil {
			return fmt.Errorf("admin listen on %s: %w", cfg.AdminAddr, err)
		}
		defer func() { _ = adminLn.Close() }()
	}

	// 2. Data directory: exclusive lock, then recover what a previous run left.
	sp, err := spool.Open(cfg.DataDir, log)
	if err != nil {
		return err
	}
	defer func() { _ = sp.Close() }()
	if err := sp.Recover(); err != nil {
		return fmt.Errorf("recover: %w", err)
	}

	// 3. The write-ahead log.
	w, err := wal.New(wal.Config{
		Dir:              sp.Dirs.Ingestions,
		MaxFileSize:      cfg.MaxFileSize,
		RotationInterval: cfg.RotationInterval,
		MaxBatchSize:     cfg.MaxBatchSize,
		MaxBatchBytes:    int(cfg.MaxBatchBytes),
		QueueSize:        cfg.QueueSize,
		MaxInflightBytes: cfg.MaxInflightBytes,
		Linger:           cfg.CommitLinger,
		Handoff:          sp,
		Logger:           log,
	})
	if err != nil {
		return fmt.Errorf("start wal: %w", err)
	}

	// 4. Workers.
	var dst sink.Sink = sink.Noop{}
	if cfg.SinkURL != "" {
		dst = sink.NewHTTP(sink.HTTPConfig{
			URL:               cfg.SinkURL,
			Timeout:           cfg.SinkTimeout,
			MaxConns:          cfg.Workers,
			Authorization:     cfg.SinkAuthorization,
			Headers:           cfg.SinkHeaders,
			PermanentStatuses: cfg.SinkPermanentStatuses,
		})
	} else {
		log.Warn("SINK_URL is not set: messages are stored and then discarded")
	}
	runner := &pipeline.Runner{
		Spool: sp,
		Proc: pipeline.NewProcessor(pipeline.Options{
			Sink:               dst,
			FailedDir:          sp.Dirs.Failed,
			CheckpointInterval: cfg.CheckpointInterval,
			Retry:              pipeline.RetryPolicy{MaxAttempts: cfg.SinkMaxAttempts, BaseDelay: cfg.SinkBackoffBase, MaxDelay: cfg.SinkBackoffMax},
			Logger:             log,
		}),
		Workers:         cfg.Workers,
		MaxFileAttempts: cfg.MaxFileAttempts,
		RetryBase:       cfg.FileRetryBase,
		RetryMax:        cfg.FileRetryMax,
		Logger:          log,
	}
	runCtx, stopRunner := context.WithCancel(context.Background())
	runnerDone := make(chan struct{})
	go func() { runner.Run(runCtx); close(runnerDone) }()
	defer stopRunner() // idempotent; covers the early error returns below

	// 5. HTTP front door and admin endpoints.
	if cfg.WebhookSecret == "" {
		log.Warn("WEBHOOK_SECRET is not set: requests are not authenticated")
	}
	srv := server.New(server.Config{
		Endpoint:           cfg.Endpoint,
		MaxMessageSize:     int(cfg.MaxMessageSize),
		MaxConnections:     cfg.MaxConnections,
		ReadTimeout:        cfg.ReadTimeout,
		WriteTimeout:       cfg.WriteTimeout,
		IdleTimeout:        cfg.IdleTimeout,
		AckTimeout:         cfg.AckTimeout,
		MaxBacklogBytes:    cfg.MaxBacklogBytes,
		RateLimitPerMinute: cfg.RateLimitPerMinute,
		TrustedProxies:     cfg.TrustedProxies,
		ProxyHeader:        cfg.ProxyHeader,
		Secret:             cfg.WebhookSecret,
		SignatureHeader:    cfg.WebhookSignatureHeader,
		TimestampHeader:    cfg.WebhookTimestampHeader,
		Tolerance:          cfg.WebhookTolerance,
	}, w, sp.Disp, log)

	var adm *admin.Admin
	if adminLn != nil {
		adm, err = admin.Start(admin.Options{
			Listener: adminLn,
			Pprof:    cfg.AdminPprof,
			Ready: func() (bool, string) {
				if !w.Healthy() {
					return false, "write-ahead log is not accepting data"
				}
				if cfg.MaxBacklogBytes > 0 && sp.Disp.BacklogBytes() > cfg.MaxBacklogBytes {
					return false, "processing backlog is full"
				}
				return true, ""
			},
			Logger: log,
		})
		if err != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
			_ = w.Close(closeCtx)
			cancel()
			return fmt.Errorf("admin listener: %w", err)
		}
		log.Info("admin listening", "addr", adm.Addr())
	}

	sigCtx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	log.Info("ready", "addr", ln.Addr().String(), "endpoint", cfg.Endpoint, "data_dir", cfg.DataDir, "pid", os.Getpid())

	var runErr error
	select {
	case <-sigCtx.Done():
		log.Info("shutdown requested")
	case runErr = <-serveErr:
		log.Error("http server stopped", "err", runErr)
	}
	stop() // a second Ctrl+C now terminates immediately

	// 6. Shutdown, in the only order that loses nothing that was acknowledged:
	//    stop taking requests -> let in-flight ones finish and get their ACK ->
	//    commit and close the WAL -> stop the workers (they checkpoint).
	//    The whole sequence fits in SHUTDOWN_TIMEOUT: the HTTP drain gets the
	//    budget minus a reserve for the WAL and the workers, which only need an
	//    instant to commit what is queued and save their checkpoints.
	deadline := time.Now().Add(cfg.ShutdownTimeout)
	left := func() time.Duration { return max(time.Until(deadline), 0) }
	const reserve = 500 * time.Millisecond

	if err := srv.Shutdown(max(cfg.ShutdownTimeout-reserve, cfg.ShutdownTimeout/2)); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	walCtx, cancelWal := context.WithTimeout(context.Background(), left())
	if err := w.Close(walCtx); err != nil {
		log.Error("wal did not close cleanly; unsynced data may remain in ingestions/ for recovery", "err", err)
	}
	cancelWal()

	stopRunner()
	select {
	case <-runnerDone:
	case <-time.After(left()):
		select {
		case <-runnerDone:
		default:
			log.Warn("workers did not stop in time; their files resume from the last checkpoint")
		}
	}
	if adm != nil {
		admCtx, cancel := context.WithTimeout(context.Background(), max(left(), 100*time.Millisecond))
		_ = adm.Shutdown(admCtx)
		cancel()
	}

	log.Info("stopped",
		"files_left_for_next_start", sp.Disp.BacklogFiles(),
		"bytes_left_for_next_start", sp.Disp.BacklogBytes())
	return runErr
}
