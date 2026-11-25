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
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/limiter"

	"github.com/okzgn/wal-ingestion-engine-go/internal/logutil"
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

	// MaxBodyMemory bounds the request bodies held in memory at once, across
	// every connection (0 = no bound). Bodies are read by the handler, after
	// this much has been reserved for them, instead of by the server before
	// the handler runs: without it MaxConnections x MaxMessageSize bytes could
	// be buffered before a single signature is checked.
	MaxBodyMemory int64

	// TrustedProxies lists the reverse proxies (IPs or CIDRs) whose ProxyHeader
	// is believed for the client IP that the rate limit keys on. Without it
	// every request behind a proxy shares the proxy's IP and the per-client
	// limit silently becomes a global one.
	TrustedProxies []string
	ProxyHeader    string // default X-Forwarded-For

	// Signature verification is enabled when Secret is not empty.
	Secret string
	// SignatureScheme is how the sender signs (default SchemeGeneric).
	SignatureScheme Scheme
	// SignatureHeader and TimestampHeader override the headers of the scheme.
	// With the generic scheme a TimestampHeader enables the anti-replay check.
	SignatureHeader string
	TimestampHeader string
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
	bodyMem atomic.Int64 // bytes reserved for request bodies being handled

	// Both lines can repeat thousands of times per second while the disk is
	// failing; they are throttled so the log keeps the signal, not the flood.
	notPersisted *logutil.Throttle
	failed       *logutil.Throttle
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
	if cfg.SignatureScheme == "" {
		cfg.SignatureScheme = SchemeGeneric
	}
	cfg.SignatureScheme = Scheme(strings.ToLower(string(cfg.SignatureScheme)))
	cfg.SignatureHeader, cfg.TimestampHeader = SchemeHeaders(cfg.SignatureScheme, cfg.SignatureHeader, cfg.TimestampHeader)
	if cfg.ProxyHeader == "" {
		cfg.ProxyHeader = fiber.HeaderXForwardedFor
	}

	s := &Server{
		cfg: cfg, w: w, backlog: backlog, log: log,
		notPersisted: logutil.NewThrottle(time.Second),
		failed:       logutil.NewThrottle(time.Second),
	}
	s.pool.New = func() any { return new(bytes.Buffer) }
	if cfg.Secret != "" {
		s.ver = NewSchemeVerifier(cfg.SignatureScheme, cfg.Secret, cfg.TimestampHeader != "", cfg.Tolerance)
	}

	fcfg := fiber.Config{
		AppName:   "WAL Ingestion Engine",
		BodyLimit: cfg.MaxMessageSize,
		// The handler reads the body itself (see readBody): the server only
		// pre-reads the first few KiB, so a connection costs almost nothing
		// until its body has been admitted against MaxBodyMemory.
		StreamRequestBody: true,
		Concurrency:       cfg.MaxConnections,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ErrorHandler:      s.errorHandler,
	}
	if len(cfg.TrustedProxies) > 0 {
		fcfg.TrustProxy = true
		fcfg.TrustProxyConfig = fiber.TrustProxyConfig{Proxies: cfg.TrustedProxies}
		fcfg.ProxyHeader = cfg.ProxyHeader
		// Walk a "client, proxy1, proxy2" chain from the right past the trusted
		// hops instead of believing whatever the client put first.
		fcfg.EnableIPValidation = true
	}
	s.app = fiber.New(fcfg)

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

// Serve blocks serving on ln until Shutdown is called. Binding the listener
// first lets a busy port fail before anything else is started.
func (s *Server) Serve(ln net.Listener) error {
	return s.app.Listener(ln, fiber.ListenConfig{DisableStartupMessage: true})
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

	raw, done, ok := s.readBody(c)
	if !ok {
		return nil // already answered
	}
	defer done()
	body := raw.Bytes()
	if len(body) == 0 {
		return s.reject(c, fiber.StatusBadRequest, "empty_body", "empty body")
	}

	if s.ver != nil {
		if err := s.ver.Verify(c.Get(s.cfg.SignatureHeader), c.Get(s.cfg.TimestampHeader), body); err != nil {
			return s.reject(c, fiber.StatusUnauthorized, "bad_signature", err.Error())
		}
	}

	// One message = one line: compact away every insignificant newline. Compact
	// validates as it goes, so this is the only JSON pass over the body.
	buf := s.pool.Get().(*bytes.Buffer)
	buf.Reset()
	if err := json.Compact(buf, body); err != nil {
		s.release(buf)
		return s.reject(c, fiber.StatusBadRequest, "invalid_json", "body is not valid JSON")
	}

	if s.backlog != nil && s.cfg.MaxBacklogBytes > 0 && s.backlog.BacklogBytes() > s.cfg.MaxBacklogBytes {
		s.release(buf)
		return s.unavailable(c, "backlog_full", "the processing backlog is full")
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
		if ok, suppressed := s.notPersisted.Allow(); ok {
			s.log.Warn("message not persisted", "err", err, "suppressed", suppressed)
		}
		return s.unavailable(c, "unavailable", "storage is unavailable")
	}
}

// readBody admits the request body against the memory budget and reads it. On
// success the caller must call done when it no longer needs the buffer. When
// ok is false the request has already been answered.
//
// The connection is closed after any refusal, since part of the body may still
// be on the wire. A bounded amount of it is read and thrown away first
// (maxDrain, held nowhere): a client that is still sending when the connection
// closes sees a reset instead of the answer.
func (s *Server) readBody(c fiber.Ctx) (body *bytes.Buffer, done func(), ok bool) {
	limit := s.cfg.MaxMessageSize
	refuse := func(error) (*bytes.Buffer, func(), bool) {
		c.Response().SetConnectionClose()
		if stream := c.Request().BodyStream(); stream != nil {
			_, _ = io.CopyN(io.Discard, stream, maxDrain)
		}
		return nil, nil, false
	}

	// -1 is a chunked body of unknown length: it may grow up to the limit.
	declared := c.Request().Header.ContentLength()
	if declared > limit {
		return refuse(s.reject(c, fiber.StatusRequestEntityTooLarge, "too_large", "Request Entity Too Large"))
	}
	reserve := int64(limit)
	if declared >= 0 {
		reserve = int64(declared)
	}
	if s.cfg.MaxBodyMemory > 0 && s.bodyMem.Add(reserve) > s.cfg.MaxBodyMemory {
		s.bodyMem.Add(-reserve)
		return refuse(s.unavailable(c, "body_memory_full", "too many request bodies in memory"))
	}
	buf := s.pool.Get().(*bytes.Buffer)
	buf.Reset()
	done = func() {
		s.release(buf)
		if s.cfg.MaxBodyMemory > 0 {
			s.bodyMem.Add(-reserve)
		}
	}

	if stream := c.Request().BodyStream(); stream != nil {
		if declared > 0 {
			buf.Grow(declared)
		}
		// One byte past the limit is enough to know a chunked body is too big.
		_, rerr := buf.ReadFrom(io.LimitReader(stream, int64(limit)+1))
		if rerr != nil {
			done()
			return refuse(s.reject(c, fiber.StatusBadRequest, "bad_body", "the request body could not be read"))
		}
	} else {
		buf.Write(c.Request().Body()) // not streamed: the server already holds it
	}
	if buf.Len() > limit {
		done()
		return refuse(s.reject(c, fiber.StatusRequestEntityTooLarge, "too_large", "Request Entity Too Large"))
	}
	return buf, done, true
}

// maxDrain is how much of a refused body is read and discarded before the
// connection is closed (a "lingering close"). It covers a body of the largest
// MAX_MESSAGE_SIZE that can be configured several times over, costs no memory,
// and is bounded in time by the read timeout.
const maxDrain = 64 << 20

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
		if ok, suppressed := s.failed.Allow(); ok {
			s.log.Warn("request failed", "code", code, "err", err, "suppressed", suppressed)
		}
	}
	metrics.Reject(reason)
	return c.Status(code).JSON(fiber.Map{"error": fiber.NewError(code).Message, "reason": reason})
}
