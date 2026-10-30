// Package server is the HTTP front door. Its contract is strict: a request gets
// 200 only if its JSON is valid and has been fsynced to the WAL. Everything else
// is an explicit 4xx (the sender's problem, do not retry blindly) or 503 (ours,
// retry later) so that nothing is ever acknowledged and then lost.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/limiter"

	"github.com/okzgn/wal-ingestion-engine-go/internal/metrics"
	"github.com/okzgn/wal-ingestion-engine-go/internal/wal"
)

// Appender is what the server needs from the WAL.
type Appender interface {
	Append(ctx context.Context, payload []byte) error
}

// Backlog reports how much unprocessed data is waiting on disk.
type Backlog interface {
	BacklogBytes() int64
}

// Config configures the HTTP server.
type Config struct {
	Endpoint           string
	MaxMessageSize     int
	MaxConnections     int
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	IdleTimeout        time.Duration
	AckTimeout         time.Duration
	MaxBacklogBytes    int64
	RateLimitPerMinute int

	// HMAC verification is enabled when Secret is not empty.
	Secret          string
	SignatureHeader string
	TimestampHeader string // optional: enables the anti-replay check
	Tolerance       time.Duration
}

// Server wraps the Fiber app.
type Server struct {
	cfg     Config
	app     *fiber.App
	w       Appender
	backlog Backlog
	ver     *Verifier
	log     *slog.Logger
	pool    sync.Pool
}

// New builds the server and its routes.
func New(cfg Config, w Appender, backlog Backlog, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = "/webhook"
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = 1 << 20
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 5 * time.Second
	}
	if cfg.SignatureHeader == "" {
		cfg.SignatureHeader = "X-Signature-256"
	}

	s := &Server{cfg: cfg, w: w, backlog: backlog, log: log}
	s.pool.New = func() any { return new(bytes.Buffer) }
	if cfg.Secret != "" {
		s.ver = NewVerifier(cfg.Secret, cfg.TimestampHeader != "", cfg.Tolerance)
	}

	s.app = fiber.New(fiber.Config{
		AppName:      "WAL Ingestion Engine",
		BodyLimit:    cfg.MaxMessageSize,
		Concurrency:  cfg.MaxConnections,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
		ErrorHandler: s.errorHandler,
	})

	handlers := []any{s.webhook}
	if cfg.RateLimitPerMinute > 0 {
		handlers = []any{limiter.New(limiter.Config{
			Max:        cfg.RateLimitPerMinute,
			Expiration: time.Minute,
			LimitReached: func(c fiber.Ctx) error {
				return s.reject(c, fiber.StatusTooManyRequests, "rate_limited", "too many requests")
			},
		}), s.webhook}
	}
	s.app.Post(cfg.Endpoint, handlers[0], handlers[1:]...)
	return s
}

// App exposes the Fiber app (used by tests with app.Test).
func (s *Server) App() *fiber.App { return s.app }

// Listen blocks serving on addr until Shutdown is called.
func (s *Server) Listen(addr string) error {
	return s.app.Listen(addr, fiber.ListenConfig{DisableStartupMessage: true})
}

// Shutdown stops accepting connections and waits for in-flight requests, up to
// timeout, after which the remaining connections are closed.
func (s *Server) Shutdown(timeout time.Duration) error {
	return s.app.ShutdownWithTimeout(timeout)
}

func (s *Server) webhook(c fiber.Ctx) error {
	// Fiber would transparently inflate a compressed body, bypassing BodyLimit
	// (decompression bomb) and invalidating the signature over the raw bytes.
	if c.Get(fiber.HeaderContentEncoding) != "" {
		return s.reject(c, fiber.StatusUnsupportedMediaType, "unsupported_encoding", "Content-Encoding is not supported")
	}

	body := c.Body()
	if len(body) == 0 {
		return s.reject(c, fiber.StatusBadRequest, "empty_body", "empty body")
	}

	if s.ver != nil {
		if err := s.ver.Verify(c.Get(s.cfg.SignatureHeader), c.Get(s.cfg.TimestampHeader), body); err != nil {
			return s.reject(c, fiber.StatusUnauthorized, "bad_signature", err.Error())
		}
	}

	if !json.Valid(body) {
		return s.reject(c, fiber.StatusBadRequest, "invalid_json", "body is not valid JSON")
	}

	if s.backlog != nil && s.cfg.MaxBacklogBytes > 0 && s.backlog.BacklogBytes() > s.cfg.MaxBacklogBytes {
		return s.unavailable(c, "backlog_full", "the processing backlog is full")
	}

	// One message = one line: compact away every insignificant newline.
	buf := s.pool.Get().(*bytes.Buffer)
	buf.Reset()
	if err := json.Compact(buf, body); err != nil {
		s.release(buf)
		return s.reject(c, fiber.StatusBadRequest, "invalid_json", "body is not valid JSON")
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.AckTimeout)
	defer cancel()
	start := time.Now()
	err := s.w.Append(ctx, buf.Bytes())
	metrics.AckLatencyMicros.Add(time.Since(start).Microseconds())
	metrics.AckCount.Add(1)

	switch {
	case err == nil:
		s.release(buf)
		return c.SendStatus(fiber.StatusOK)
	case errors.Is(err, wal.ErrAckTimeout):
		// The writer may still read the buffer: never put it back in the pool.
		return s.unavailable(c, "ack_timeout", "timed out waiting for the disk")
	case errors.Is(err, wal.ErrOverloaded):
		s.release(buf)
		return s.unavailable(c, "overloaded", "the server is overloaded")
	case errors.Is(err, wal.ErrClosed):
		s.release(buf)
		return s.unavailable(c, "shutting_down", "the server is shutting down")
	case errors.Is(err, wal.ErrNewline):
		s.release(buf)
		return s.reject(c, fiber.StatusBadRequest, "invalid_json", "message cannot be framed")
	default:
		s.release(buf)
		s.log.Warn("message not persisted", "err", err)
		return s.unavailable(c, "unavailable", "storage is unavailable")
	}
}

func (s *Server) release(buf *bytes.Buffer) {
	if buf.Cap() > 256<<10 {
		return // do not let rare large messages bloat the pool
	}
	s.pool.Put(buf)
}

// reject answers a client error (4xx).
func (s *Server) reject(c fiber.Ctx, code int, reason, msg string) error {
	metrics.Reject(reason)
	return c.Status(code).JSON(fiber.Map{"error": msg, "reason": reason})
}

// unavailable answers a server-side refusal (503) that is worth retrying.
func (s *Server) unavailable(c fiber.Ctx, reason, msg string) error {
	metrics.Reject(reason)
	c.Set(fiber.HeaderRetryAfter, strconv.Itoa(1))
	return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": msg, "reason": reason})
}

func (s *Server) errorHandler(c fiber.Ctx, err error) error {
	code := fiber.StatusInternalServerError
	var fe *fiber.Error
	if errors.As(err, &fe) {
		code = fe.Code
	}
	reason := "error"
	switch code {
	case fiber.StatusRequestEntityTooLarge:
		reason = "too_large"
	case fiber.StatusMethodNotAllowed:
		reason = "method_not_allowed"
	case fiber.StatusNotFound:
		reason = "not_found"
	default:
		s.log.Warn("request failed", "code", code, "err", err)
	}
	metrics.Reject(reason)
	return c.Status(code).JSON(fiber.Map{"error": fiber.NewError(code).Message, "reason": reason})
}
