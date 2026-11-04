package config

import (
	"strings"
	"testing"
	"time"
)

// env mimics os.LookupEnv over a map: a key that is present but empty is set.
func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestLoad_Defaults(t *testing.T) {
	c, err := Load(nil, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.ListenAddr != ":8080" || c.DataDir != "./data" || c.MaxMessageSize != 1<<20 ||
		c.Workers != 4 || c.AckTimeout != 5*time.Second || c.AdminAddr != "127.0.0.1:9090" {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	if c.WebhookSecret != "" || c.SinkURL != "" || c.SinkAuthorization != "" || len(c.TrustedProxies) != 0 {
		t.Fatal("secret, sink, sink auth and trusted proxies must be off by default")
	}
	if len(c.SinkPermanentStatuses) != 4 || c.SinkPermanentStatuses[0] != 400 || c.SinkPermanentStatuses[3] != 422 {
		t.Fatalf("default permanent statuses: %v", c.SinkPermanentStatuses)
	}
	if c.ProxyHeader != "X-Forwarded-For" {
		t.Fatalf("default proxy header: %q", c.ProxyHeader)
	}
}

func TestLoad_Precedence_FlagOverEnvOverDefault(t *testing.T) {
	e := env(map[string]string{"LISTEN_ADDR": ":9000", "WORKERS": "2", "MAX_FILE_SIZE": "2MiB"})
	c, err := Load([]string{"-workers", "8"}, e)
	if err != nil {
		t.Fatal(err)
	}
	if c.ListenAddr != ":9000" {
		t.Errorf("env should override default, got %q", c.ListenAddr)
	}
	if c.Workers != 8 {
		t.Errorf("flag should override env, got %d", c.Workers)
	}
	if c.MaxFileSize != 2<<20 {
		t.Errorf("size parsing: %d", c.MaxFileSize)
	}
}

// "ADMIN_ADDR=" used to be indistinguishable from an unset variable and
// silently restored the default, so the documented "empty = disabled" never
// worked through the environment.
func TestLoad_EmptyEnvCountsAsSet(t *testing.T) {
	c, err := Load(nil, env(map[string]string{"ADMIN_ADDR": ""}))
	if err != nil {
		t.Fatal(err)
	}
	if c.AdminAddr != "" {
		t.Fatalf("ADMIN_ADDR= must disable the admin listener, got %q", c.AdminAddr)
	}
	if _, err := Load(nil, env(map[string]string{"LISTEN_ADDR": ""})); err == nil || !strings.Contains(err.Error(), "LISTEN_ADDR") {
		t.Fatalf("an explicitly empty LISTEN_ADDR must be rejected, got %v", err)
	}
	c, err = Load([]string{"-admin-addr", ""}, env(nil))
	if err != nil || c.AdminAddr != "" {
		t.Fatalf("empty flag: %v %q", err, c.AdminAddr)
	}
}

func TestLoad_SecretsAreEnvOnly(t *testing.T) {
	c, err := Load(nil, env(map[string]string{"WEBHOOK_SECRET": "s", "SINK_AUTHORIZATION": "Bearer t", "SINK_HEADERS": "X-Api-Key: k"}))
	if err != nil || c.WebhookSecret != "s" || c.SinkAuthorization != "Bearer t" || c.SinkHeaders.Get("X-Api-Key") != "k" {
		t.Fatalf("secrets via env: %v %+v", err, c)
	}
	for _, flag := range []string{"-webhook-secret", "-sink-authorization", "-sink-headers"} {
		if _, err := Load([]string{flag, "x"}, env(nil)); err == nil {
			t.Errorf("%s would leak in the process list and must not exist", flag)
		}
	}
}

func TestLoad_SinkSettings(t *testing.T) {
	c, err := Load(nil, env(map[string]string{
		"SINK_URL":                "https://api.example.com/events",
		"SINK_HEADERS":            "X-Tenant: acme, Accept: application/json\nX-Empty:",
		"SINK_PERMANENT_STATUSES": "400, 404,422",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.SinkHeaders.Get("X-Tenant") != "acme" || c.SinkHeaders.Get("Accept") != "application/json" || c.SinkHeaders.Get("X-Empty") != "" {
		t.Fatalf("headers: %v", c.SinkHeaders)
	}
	if _, ok := c.SinkHeaders["X-Empty"]; !ok {
		t.Fatal("an empty header value is still a header")
	}
	if len(c.SinkPermanentStatuses) != 3 || c.SinkPermanentStatuses[1] != 404 {
		t.Fatalf("statuses: %v", c.SinkPermanentStatuses)
	}
	c, err = Load([]string{"-sink-permanent-statuses", ""}, env(nil))
	if err != nil || len(c.SinkPermanentStatuses) != 0 {
		t.Fatalf("an empty list means nothing is permanent: %v %v", err, c.SinkPermanentStatuses)
	}
}

func TestLoad_TrustedProxies(t *testing.T) {
	c, err := Load(nil, env(map[string]string{"TRUSTED_PROXIES": "10.0.0.1, 192.168.0.0/16,fd00::/8", "PROXY_HEADER": "X-Real-IP"}))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.TrustedProxies) != 3 || c.ProxyHeader != "X-Real-IP" {
		t.Fatalf("%v %q", c.TrustedProxies, c.ProxyHeader)
	}
}

func TestLoad_Errors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"bad int", nil, map[string]string{"WORKERS": "many"}, "WORKERS"},
		{"bad duration", nil, map[string]string{"ACK_TIMEOUT": "soon"}, "ACK_TIMEOUT"},
		{"bad size", nil, map[string]string{"MAX_FILE_SIZE": "huge"}, "MAX_FILE_SIZE"},
		{"bad bool", nil, map[string]string{"ADMIN_PPROF": "maybe"}, "ADMIN_PPROF"},
		{"zero workers", nil, map[string]string{"WORKERS": "0"}, "WORKERS must be at least 1"},
		{"tiny message limit", nil, map[string]string{"MAX_MESSAGE_SIZE": "10"}, "MAX_MESSAGE_SIZE"},
		{"write <= ack", nil, map[string]string{"WRITE_TIMEOUT": "5s", "ACK_TIMEOUT": "5s"}, "WRITE_TIMEOUT"},
		{"inflight below message", nil, map[string]string{"MAX_INFLIGHT_BYTES": "1KiB"}, "MAX_INFLIGHT_BYTES"},
		{"batch below message", nil, map[string]string{"MAX_BATCH_BYTES": "1KiB"}, "MAX_BATCH_BYTES"},
		{"bad sink url", nil, map[string]string{"SINK_URL": "ftp://x"}, "SINK_URL"},
		{"credentials in sink url", nil, map[string]string{"SINK_URL": "https://user:pass@x.example/e"}, "SINK_AUTHORIZATION"},
		{"bad sink header", nil, map[string]string{"SINK_HEADERS": "no colon"}, "SINK_HEADERS"},
		{"invalid header name", nil, map[string]string{"SINK_HEADERS": "Bad Name: v"}, "SINK_HEADERS"},
		{"reserved header", nil, map[string]string{"SINK_HEADERS": "idempotency-key: v"}, "Idempotency-Key"},
		{"bad permanent status", nil, map[string]string{"SINK_PERMANENT_STATUSES": "200"}, "SINK_PERMANENT_STATUSES"},
		{"non-numeric status", nil, map[string]string{"SINK_PERMANENT_STATUSES": "4xx"}, "SINK_PERMANENT_STATUSES"},
		{"bad trusted proxy", nil, map[string]string{"TRUSTED_PROXIES": "proxy.internal"}, "TRUSTED_PROXIES"},
		{"proxies without header", nil, map[string]string{"TRUSTED_PROXIES": "10.0.0.1", "PROXY_HEADER": ""}, "PROXY_HEADER"},
		{"timestamp without secret", nil, map[string]string{"WEBHOOK_TIMESTAMP_HEADER": "X-T"}, "WEBHOOK_SECRET"},
		{"zero idle timeout", nil, map[string]string{"IDLE_TIMEOUT": "0s"}, "IDLE_TIMEOUT"},
		{"bad log level", nil, map[string]string{"LOG_LEVEL": "loud"}, "LOG_LEVEL"},
		{"unknown flag", []string{"-nope"}, nil, "flags"},
		{"stray argument", []string{"stray"}, nil, "unexpected arguments"},
		{"bad flag value", []string{"-ack-timeout", "x"}, nil, "-ack-timeout"},
	}
	for _, tc := range tests {
		_, err := Load(tc.args, env(tc.env))
		if err == nil {
			t.Errorf("%s: expected an error", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %q should mention %q", tc.name, err, tc.want)
		}
	}
}

func TestValidate_ReportsEveryProblemAtOnce(t *testing.T) {
	_, err := Load(nil, env(map[string]string{"WORKERS": "0", "QUEUE_SIZE": "0"}))
	if err == nil || !strings.Contains(err.Error(), "WORKERS") || !strings.Contains(err.Error(), "QUEUE_SIZE") {
		t.Fatalf("both problems should be reported, got %v", err)
	}
}

func TestParseSize(t *testing.T) {
	ok := map[string]int64{
		"0": 0, "1": 1, "10B": 10, "1KiB": 1024, "1kib": 1024, "2 MiB": 2 << 20,
		"1GiB": 1 << 30, "1KB": 1000, "64MB": 64_000_000, "1048576": 1 << 20,
	}
	for in, want := range ok {
		got, err := ParseSize(in)
		if err != nil || got != want {
			t.Errorf("ParseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "x", "-1", "1.5MiB", "MiB", "1TB"} {
		if _, err := ParseSize(in); err == nil {
			t.Errorf("ParseSize(%q) should fail", in)
		}
	}
}

func TestUsageMentionsEveryEnvVariable(t *testing.T) {
	u := Usage()
	for _, s := range settings(&Config{}) {
		if !strings.Contains(u, s.env) {
			t.Errorf("usage lacks %s", s.env)
		}
	}
}
