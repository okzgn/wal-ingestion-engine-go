// Package config loads and validates the engine settings. Every setting has an
// environment variable and, except secrets, an equivalent command-line flag
// (lower-case, dashes). Precedence: flag > environment > default. An environment
// variable that is set to the empty string counts as set, so "ADMIN_ADDR="
// disables the admin listener instead of silently restoring the default.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config holds every tunable of the engine.
type Config struct {
	ListenAddr string
	DataDir    string
	Endpoint   string

	MaxMessageSize   int64
	MaxFileSize      int64
	RotationInterval time.Duration
	MaxBatchSize     int
	MaxBatchBytes    int64
	CommitLinger     time.Duration
	QueueSize        int
	MaxInflightBytes int64
	MaxBacklogBytes  int64

	Workers               int
	CheckpointInterval    int
	SinkURL               string
	SinkTimeout           time.Duration
	SinkMaxAttempts       int
	SinkBackoffBase       time.Duration
	SinkBackoffMax        time.Duration
	SinkAuthorization     string
	SinkHeaders           http.Header
	SinkPermanentStatuses []int
	FileRetryBase         time.Duration
	FileRetryMax          time.Duration
	MaxFileAttempts       int

	WebhookSecret          string
	WebhookSignatureHeader string
	WebhookTimestampHeader string
	WebhookTolerance       time.Duration
	RateLimitPerMinute     int
	TrustedProxies         []string
	ProxyHeader            string

	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	AckTimeout      time.Duration
	MaxConnections  int
	ShutdownTimeout time.Duration

	AdminAddr  string
	AdminPprof bool
	LogLevel   string
	LogFormat  string
}

type setting struct {
	env, flag, def, usage string
	set                   func(string) error
}

func settings(c *Config) []setting {
	return []setting{
		{"LISTEN_ADDR", "listen-addr", ":8080", "address the webhook server listens on", str(&c.ListenAddr)},
		{"DATA_DIR", "data-dir", "./data", "root of ingestions/, processing/ and failed/", str(&c.DataDir)},
		{"ENDPOINT", "endpoint", "/webhook", "webhook path", str(&c.Endpoint)},

		{"MAX_MESSAGE_SIZE", "max-message-size", "1MiB", "largest accepted request body", size(&c.MaxMessageSize)},
		{"MAX_FILE_SIZE", "max-file-size", "1MiB", "rotate the WAL file at this size", size(&c.MaxFileSize)},
		{"ROTATION_INTERVAL", "rotation-interval", "10s", "rotate a non-empty WAL file after this long", dur(&c.RotationInterval)},
		{"MAX_BATCH_SIZE", "max-batch-size", "256", "max messages per group commit", num(&c.MaxBatchSize)},
		{"MAX_BATCH_BYTES", "max-batch-bytes", "1MiB", "max bytes per group commit", size(&c.MaxBatchBytes)},
		{"COMMIT_LINGER", "commit-linger", "0s", "extra wait to gather a bigger batch (0 = commit immediately)", dur(&c.CommitLinger)},
		{"QUEUE_SIZE", "queue-size", "4096", "capacity of the writer queue", num(&c.QueueSize)},
		{"MAX_INFLIGHT_BYTES", "max-inflight-bytes", "64MiB", "503 above this many bytes waiting for the disk", size(&c.MaxInflightBytes)},
		{"MAX_BACKLOG_BYTES", "max-backlog-bytes", "1GiB", "503 above this much unprocessed data on disk", size(&c.MaxBacklogBytes)},

		{"WORKERS", "workers", "4", "concurrent file workers (1 = in-order delivery)", num(&c.Workers)},
		{"CHECKPOINT_INTERVAL", "checkpoint-interval", "50", "lines between checkpoint saves", num(&c.CheckpointInterval)},
		{"SINK_URL", "sink-url", "", "forward every message to this URL (empty = discard)", str(&c.SinkURL)},
		{"SINK_TIMEOUT", "sink-timeout", "10s", "timeout of one delivery request", dur(&c.SinkTimeout)},
		{"SINK_MAX_ATTEMPTS", "sink-max-attempts", "5", "attempts per message before the file is postponed", num(&c.SinkMaxAttempts)},
		{"SINK_BACKOFF_BASE", "sink-backoff-base", "200ms", "first per-message retry delay", dur(&c.SinkBackoffBase)},
		{"SINK_BACKOFF_MAX", "sink-backoff-max", "30s", "per-message retry delay cap", dur(&c.SinkBackoffMax)},
		{"SINK_AUTHORIZATION", "", "", "Authorization header sent with every delivery, e.g. 'Bearer <token>'", str(&c.SinkAuthorization)},
		{"SINK_HEADERS", "", "", "extra delivery headers: 'Name: value, Name2: value2'", headers(&c.SinkHeaders)},
		{"SINK_PERMANENT_STATUSES", "sink-permanent-statuses", "400,413,415,422", "destination statuses that dead-letter a message instead of retrying it", ints(&c.SinkPermanentStatuses)},
		{"FILE_RETRY_BASE", "file-retry-base", "1s", "first delay before a postponed file is retried", dur(&c.FileRetryBase)},
		{"FILE_RETRY_MAX", "file-retry-max", "1m", "postponed file retry delay cap", dur(&c.FileRetryMax)},
		{"MAX_FILE_ATTEMPTS", "max-file-attempts", "0", "move a file to failed/ after this many postponements (0 = never)", num(&c.MaxFileAttempts)},

		{"WEBHOOK_SECRET", "", "", "HMAC-SHA256 secret; enables signature verification", str(&c.WebhookSecret)},
		{"WEBHOOK_SIGNATURE_HEADER", "webhook-signature-header", "X-Signature-256", "header carrying sha256=<hex>", str(&c.WebhookSignatureHeader)},
		{"WEBHOOK_TIMESTAMP_HEADER", "webhook-timestamp-header", "", "header carrying unix seconds; enables the anti-replay check", str(&c.WebhookTimestampHeader)},
		{"WEBHOOK_TOLERANCE", "webhook-tolerance", "5m", "accepted clock skew for the timestamp", dur(&c.WebhookTolerance)},
		{"RATE_LIMIT_PER_MINUTE", "rate-limit-per-minute", "0", "per-client request limit (0 = off)", num(&c.RateLimitPerMinute)},
		{"TRUSTED_PROXIES", "trusted-proxies", "", "IPs or CIDRs of reverse proxies whose PROXY_HEADER gives the client IP (empty = the TCP peer is the client)", list(&c.TrustedProxies)},
		{"PROXY_HEADER", "proxy-header", "X-Forwarded-For", "header set by TRUSTED_PROXIES with the client IP", str(&c.ProxyHeader)},

		{"READ_TIMEOUT", "read-timeout", "10s", "HTTP read timeout", dur(&c.ReadTimeout)},
		{"WRITE_TIMEOUT", "write-timeout", "15s", "HTTP write timeout (must exceed ACK_TIMEOUT)", dur(&c.WriteTimeout)},
		{"IDLE_TIMEOUT", "idle-timeout", "60s", "HTTP keep-alive idle timeout", dur(&c.IdleTimeout)},
		{"ACK_TIMEOUT", "ack-timeout", "5s", "max wait for the disk before answering 503", dur(&c.AckTimeout)},
		{"MAX_CONNECTIONS", "max-connections", "10000", "max concurrent HTTP connections", num(&c.MaxConnections)},
		{"SHUTDOWN_TIMEOUT", "shutdown-timeout", "15s", "grace period for in-flight work at shutdown", dur(&c.ShutdownTimeout)},

		{"ADMIN_ADDR", "admin-addr", "127.0.0.1:9090", "health/metrics listener (empty = disabled)", str(&c.AdminAddr)},
		{"ADMIN_PPROF", "admin-pprof", "false", "expose /debug/pprof on the admin listener", boolean(&c.AdminPprof)},
		{"LOG_LEVEL", "log-level", "info", "debug, info, warn or error", str(&c.LogLevel)},
		{"LOG_FORMAT", "log-format", "text", "text or json", str(&c.LogFormat)},
	}
}

// Load builds the configuration from args (without the program name) and the
// environment. lookup follows os.LookupEnv: a variable that is present but
// empty is applied as empty, it does not fall back to the default.
func Load(args []string, lookup func(string) (string, bool)) (*Config, error) {
	c := &Config{}
	fs := flag.NewFlagSet("ingestor", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	table := settings(c)
	for _, s := range table {
		v := s.def
		if e, ok := lookup(s.env); ok {
			v = e
		}
		if err := s.set(v); err != nil {
			return nil, fmt.Errorf("%s: %w", s.env, err)
		}
		if s.flag != "" {
			fs.String(s.flag, v, s.usage+" [$"+s.env+"]")
		}
	}
	if err := fs.Parse(args); err != nil {
		return nil, fmt.Errorf("flags: %w", err)
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	var visitErr error
	fs.Visit(func(f *flag.Flag) {
		for _, s := range table {
			if s.flag == f.Name {
				if err := s.set(f.Value.String()); err != nil && visitErr == nil {
					visitErr = fmt.Errorf("-%s: %w", f.Name, err)
				}
			}
		}
	})
	if visitErr != nil {
		return nil, visitErr
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// Usage describes every setting, for -h output and the README.
func Usage() string {
	var b strings.Builder
	for _, s := range settings(&Config{}) {
		name := "-" + s.flag
		if s.flag == "" {
			name = "(env only)"
		}
		fmt.Fprintf(&b, "  %-28s $%-26s default %-16q %s\n", name, s.env, s.def, s.usage)
	}
	return b.String()
}

// Validate rejects inconsistent settings with a message that names the culprit.
func (c *Config) Validate() error {
	var errs []error
	bad := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if c.ListenAddr == "" {
		bad("LISTEN_ADDR must not be empty")
	}
	if c.DataDir == "" {
		bad("DATA_DIR must not be empty")
	}
	if !strings.HasPrefix(c.Endpoint, "/") {
		bad("ENDPOINT must start with '/', got %q", c.Endpoint)
	}
	if c.MaxMessageSize < 1024 {
		bad("MAX_MESSAGE_SIZE must be at least 1KiB")
	}
	if c.MaxFileSize < 1 {
		bad("MAX_FILE_SIZE must be positive")
	}
	if c.RotationInterval <= 0 {
		bad("ROTATION_INTERVAL must be positive")
	}
	if c.MaxBatchSize < 1 {
		bad("MAX_BATCH_SIZE must be at least 1")
	}
	if c.MaxBatchBytes < c.MaxMessageSize {
		bad("MAX_BATCH_BYTES (%d) must be at least MAX_MESSAGE_SIZE (%d)", c.MaxBatchBytes, c.MaxMessageSize)
	}
	if c.CommitLinger < 0 {
		bad("COMMIT_LINGER must not be negative")
	}
	if c.QueueSize < 1 {
		bad("QUEUE_SIZE must be at least 1")
	}
	if c.MaxInflightBytes <= c.MaxMessageSize {
		bad("MAX_INFLIGHT_BYTES must exceed MAX_MESSAGE_SIZE, or the largest message could never be accepted")
	}
	if c.MaxBacklogBytes < 0 {
		bad("MAX_BACKLOG_BYTES must not be negative (0 disables the limit)")
	}
	if c.Workers < 1 {
		bad("WORKERS must be at least 1")
	}
	if c.CheckpointInterval < 1 {
		bad("CHECKPOINT_INTERVAL must be at least 1")
	}
	if c.SinkURL != "" {
		u, err := url.Parse(c.SinkURL)
		switch {
		case err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "":
			bad("SINK_URL must be an http(s) URL, got %q", c.SinkURL)
		case u.User != nil:
			bad("SINK_URL must not carry credentials (they would show up in logs); use SINK_AUTHORIZATION")
		}
	}
	if c.SinkTimeout <= 0 {
		bad("SINK_TIMEOUT must be positive")
	}
	if c.SinkMaxAttempts < 1 {
		bad("SINK_MAX_ATTEMPTS must be at least 1")
	}
	if c.SinkBackoffBase <= 0 || c.SinkBackoffMax < c.SinkBackoffBase {
		bad("SINK_BACKOFF_BASE must be positive and not above SINK_BACKOFF_MAX")
	}
	for _, s := range c.SinkPermanentStatuses {
		if s < 400 || s > 599 {
			bad("SINK_PERMANENT_STATUSES: %d is not a 4xx or 5xx status", s)
		}
	}
	if c.FileRetryBase <= 0 || c.FileRetryMax < c.FileRetryBase {
		bad("FILE_RETRY_BASE must be positive and not above FILE_RETRY_MAX")
	}
	if c.MaxFileAttempts < 0 {
		bad("MAX_FILE_ATTEMPTS must not be negative")
	}
	if c.WebhookSignatureHeader == "" {
		bad("WEBHOOK_SIGNATURE_HEADER must not be empty")
	}
	if c.WebhookTimestampHeader != "" && c.WebhookSecret == "" {
		bad("WEBHOOK_TIMESTAMP_HEADER requires WEBHOOK_SECRET")
	}
	if c.WebhookTolerance <= 0 {
		bad("WEBHOOK_TOLERANCE must be positive")
	}
	if c.RateLimitPerMinute < 0 {
		bad("RATE_LIMIT_PER_MINUTE must not be negative")
	}
	for _, p := range c.TrustedProxies {
		if _, err := netip.ParseAddr(p); err == nil {
			continue
		}
		if _, err := netip.ParsePrefix(p); err == nil {
			continue
		}
		bad("TRUSTED_PROXIES: %q is neither an IP address nor a CIDR range", p)
	}
	if len(c.TrustedProxies) > 0 && c.ProxyHeader == "" {
		bad("PROXY_HEADER must not be empty when TRUSTED_PROXIES is set")
	}
	if c.AckTimeout <= 0 {
		bad("ACK_TIMEOUT must be positive")
	}
	if c.WriteTimeout <= c.AckTimeout {
		bad("WRITE_TIMEOUT (%s) must exceed ACK_TIMEOUT (%s), or the 503 could never be written", c.WriteTimeout, c.AckTimeout)
	}
	if c.ReadTimeout <= 0 || c.IdleTimeout <= 0 {
		bad("READ_TIMEOUT and IDLE_TIMEOUT must be positive: without them idle keep-alive connections hold slots forever and shutdown can hang")
	}
	if c.MaxConnections < 1 {
		bad("MAX_CONNECTIONS must be at least 1")
	}
	if c.ShutdownTimeout <= 0 {
		bad("SHUTDOWN_TIMEOUT must be positive")
	}
	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		bad("LOG_LEVEL must be debug, info, warn or error, got %q", c.LogLevel)
	}
	switch strings.ToLower(c.LogFormat) {
	case "text", "json":
	default:
		bad("LOG_FORMAT must be text or json, got %q", c.LogFormat)
	}
	return errors.Join(errs...)
}

func str(p *string) func(string) error {
	return func(v string) error { *p = v; return nil }
}

func num(p *int) func(string) error {
	return func(v string) error {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return fmt.Errorf("not an integer: %q", v)
		}
		*p = n
		return nil
	}
}

func dur(p *time.Duration) func(string) error {
	return func(v string) error {
		d, err := time.ParseDuration(strings.TrimSpace(v))
		if err != nil {
			return fmt.Errorf("not a duration (e.g. 500ms, 10s, 2m): %q", v)
		}
		*p = d
		return nil
	}
}

func boolean(p *bool) func(string) error {
	return func(v string) error {
		b, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			return fmt.Errorf("not a boolean: %q", v)
		}
		*p = b
		return nil
	}
}

func size(p *int64) func(string) error {
	return func(v string) error {
		n, err := ParseSize(v)
		if err != nil {
			return err
		}
		*p = n
		return nil
	}
}

// list splits a comma-separated value, trimming spaces and dropping empties.
func list(p *[]string) func(string) error {
	return func(v string) error {
		*p = splitList(v)
		return nil
	}
}

// ints parses a comma-separated list of integers.
func ints(p *[]int) func(string) error {
	return func(v string) error {
		var out []int
		for _, item := range splitList(v) {
			n, err := strconv.Atoi(item)
			if err != nil {
				return fmt.Errorf("not an integer list: %q", v)
			}
			out = append(out, n)
		}
		*p = out
		return nil
	}
}

func headers(p *http.Header) func(string) error {
	return func(v string) error {
		h, err := ParseHeaders(v)
		if err != nil {
			return err
		}
		*p = h
		return nil
	}
}

func splitList(v string) []string {
	var out []string
	for _, item := range strings.Split(v, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// ParseHeaders parses "Name: value, Name2: value2". Headers are separated by
// commas or newlines, so a value cannot contain a comma. Idempotency-Key is
// reserved: the engine sets it on every delivery.
func ParseHeaders(s string) (http.Header, error) {
	h := http.Header{}
	for _, item := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == '\n' }) {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		name, value, ok := strings.Cut(item, ":")
		name, value = strings.TrimSpace(name), strings.TrimSpace(value)
		if !ok || !validHeaderName(name) {
			return nil, fmt.Errorf("header %q is not of the form \"Name: value\"", item)
		}
		if strings.EqualFold(name, "Idempotency-Key") {
			return nil, errors.New("header Idempotency-Key is set by the engine and cannot be overridden")
		}
		h.Add(name, value)
	}
	return h, nil
}

// validHeaderName reports whether s is an RFC 9110 token.
func validHeaderName(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case strings.ContainsRune("!#$%&'*+-.^_`|~", r):
		default:
			return false
		}
	}
	return true
}

// ParseSize understands plain bytes ("1048576") and KB/MB/GB (decimal) or
// KiB/MiB/GiB (binary) suffixes, case-insensitive.
func ParseSize(s string) (int64, error) {
	t := strings.ToUpper(strings.TrimSpace(s))
	mult := int64(1)
	for _, u := range []struct {
		suffix string
		mult   int64
	}{
		{"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10},
		{"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}, {"B", 1},
	} {
		if strings.HasSuffix(t, u.suffix) {
			t, mult = strings.TrimSpace(strings.TrimSuffix(t, u.suffix)), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("not a size (e.g. 512KiB, 1MiB, 64MB): %q", s)
	}
	return n * mult, nil
}
