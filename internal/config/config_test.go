package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
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
	if c.WebhookSecret != "" || c.SinkURL != "" {
		t.Fatal("secret and sink must be off by default")
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

func TestLoad_SecretIsEnvOnly(t *testing.T) {
	c, err := Load(nil, env(map[string]string{"WEBHOOK_SECRET": "s"}))
	if err != nil || c.WebhookSecret != "s" {
		t.Fatalf("secret via env: %v %q", err, c.WebhookSecret)
	}
	if _, err := Load([]string{"-webhook-secret", "x"}, env(nil)); err == nil {
		t.Fatal("a secret flag would leak in the process list and must not exist")
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
