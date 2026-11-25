package sink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func deliverTo(t *testing.T, handler http.HandlerFunc, mutate func(*HTTPConfig)) error {
	t.Helper()
	srv := httptest.NewServer(handler)
	defer srv.Close()
	cfg := HTTPConfig{URL: srv.URL, Timeout: time.Second}
	if mutate != nil {
		mutate(&cfg)
	}
	return NewHTTP(cfg).Deliver(context.Background(), "f:1", []byte(`{"a":1}`))
}

func TestHTTP_StatusClassification(t *testing.T) {
	tests := []struct {
		code      int
		wantErr   bool
		permanent bool
	}{
		{200, false, false},
		{204, false, false},
		{400, true, true},
		{413, true, true},
		{415, true, true},
		{422, true, true},
		// Misconfigurations an operator fixes: retry, never dead-letter.
		{401, true, false},
		{403, true, false},
		{404, true, false},
		{405, true, false},
		{408, true, false},
		{425, true, false},
		{429, true, false},
		{500, true, false},
		{503, true, false},
	}
	for _, tc := range tests {
		err := deliverTo(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(tc.code) }, nil)
		if (err != nil) != tc.wantErr {
			t.Errorf("%d: err=%v wantErr=%v", tc.code, err, tc.wantErr)
		}
		if err != nil && IsPermanent(err) != tc.permanent {
			t.Errorf("%d: permanent=%v want %v", tc.code, IsPermanent(err), tc.permanent)
		}
	}
}

func TestHTTP_PermanentStatusesAreConfigurable(t *testing.T) {
	custom := func(c *HTTPConfig) { c.PermanentStatuses = []int{404} }
	err := deliverTo(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(404) }, custom)
	if !IsPermanent(err) {
		t.Fatalf("404 configured as permanent, got %v", err)
	}
	err = deliverTo(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(400) }, custom)
	if err == nil || IsPermanent(err) {
		t.Fatalf("400 not in the configured list must be transient, got %v", err)
	}
}

// A redirect used to be followed: Go turns the POST into a GET without a body
// and a 200 on that GET was reported as a successful delivery.
func TestHTTP_RedirectIsNotFollowed(t *testing.T) {
	for _, code := range []int{301, 302, 303, 307, 308} {
		var reached bool
		mux := http.NewServeMux()
		mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/v2/events", code)
		})
		mux.HandleFunc("/v2/events", func(w http.ResponseWriter, _ *http.Request) {
			reached = true
			w.WriteHeader(200)
		})
		srv := httptest.NewServer(mux)
		err := NewHTTP(HTTPConfig{URL: srv.URL + "/events", Timeout: time.Second}).Deliver(context.Background(), "k", []byte(`{"a":1}`))
		srv.Close()
		if err == nil {
			t.Fatalf("%d: a redirect must not count as a delivery", code)
		}
		if IsPermanent(err) {
			t.Fatalf("%d: a redirect is a misconfiguration to retry after, got permanent %v", code, err)
		}
		if reached {
			t.Fatalf("%d: the redirect target must never be requested", code)
		}
	}
}

func TestHTTP_SendsBodyHeadersAndIdempotencyKey(t *testing.T) {
	var got http.Header
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{
		URL:           srv.URL,
		Timeout:       time.Second,
		Authorization: "Bearer t0k3n",
		Headers:       http.Header{"X-Tenant": {"acme"}, "Idempotency-Key": {"must-not-override"}},
	})
	if err := h.Deliver(context.Background(), "ingest_1.list:7", []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if gotBody != `{"a":1}` {
		t.Fatalf("body %q", gotBody)
	}
	for name, want := range map[string]string{
		"Idempotency-Key": "ingest_1.list:7",
		"Content-Type":    "application/json",
		"Authorization":   "Bearer t0k3n",
		"X-Tenant":        "acme",
	} {
		if v := got.Get(name); v != want {
			t.Errorf("%s = %q, want %q", name, v, want)
		}
	}
}

func TestHTTP_NetworkErrorIsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // nobody listens any more

	err := NewHTTP(HTTPConfig{URL: url, Timeout: 200 * time.Millisecond}).Deliver(context.Background(), "k", []byte(`{}`))
	if err == nil || IsPermanent(err) {
		t.Fatalf("connection errors must be transient, got %v", err)
	}
}

func TestIsPermanent_Wrapped(t *testing.T) {
	err := errors.Join(errors.New("ctx"), &PermanentError{Err: errors.New("boom")})
	if !IsPermanent(err) {
		t.Fatal("wrapped PermanentError not detected")
	}
	if IsPermanent(errors.New("plain")) {
		t.Fatal("plain error reported as permanent")
	}
}

func TestNoop(t *testing.T) {
	if err := (Noop{}).Deliver(context.Background(), "k", nil); err != nil {
		t.Fatal(err)
	}
}

func TestParsePointer(t *testing.T) {
	for in, want := range map[string][]string{
		"/id":             {"id"},
		"/data/object/id": {"data", "object", "id"},
		"/a~1b/c~0d":      {"a/b", "c~d"},
		"/":               {""},
		"/items/0":        {"items", "0"},
	} {
		got, err := ParsePointer(in)
		if err != nil || fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("ParsePointer(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	if got, err := ParsePointer(""); got != nil || err != nil {
		t.Errorf("the empty pointer means no pointer, got %v, %v", got, err)
	}
	for _, in := range []string{"id", "/a~", "/a~2"} {
		if _, err := ParsePointer(in); err == nil {
			t.Errorf("ParsePointer(%q) should fail", in)
		}
	}
}

func TestMessageKey(t *testing.T) {
	ptr := func(p string) []string { tokens, _ := ParsePointer(p); return tokens }
	doc := `{"id":"evt_1","n":12345678901234567890,"data":{"object":{"id":"ch_9"}},"items":[{"k":"first"}],"flag":true,"empty":"","sp":"a b","nl":"a\nb"}`
	for pointer, want := range map[string]string{
		"/id":             "evt_1",
		"/n":              "12345678901234567890", // exact: not rounded through a float
		"/data/object/id": "ch_9",
		"/items/0/k":      "first",
	} {
		if got, ok := MessageKey([]byte(doc), ptr(pointer)); !ok || got != want {
			t.Errorf("%s = %q, %v; want %q", pointer, got, ok, want)
		}
	}
	// No usable value: the engine key is kept.
	for _, pointer := range []string{"/missing", "/data", "/flag", "/empty", "/sp", "/nl", "/items/1", "/items/x", "/items/00", "/id/deeper"} {
		if got, ok := MessageKey([]byte(doc), ptr(pointer)); ok {
			t.Errorf("%s resolved to %q, want no key", pointer, got)
		}
	}
	if _, ok := MessageKey([]byte(doc), nil); ok {
		t.Error("no pointer, no key")
	}
	if _, ok := MessageKey([]byte(`{"id":"`+strings.Repeat("x", 300)+`"}`), ptr("/id")); ok {
		t.Error("a key too long for a header must be refused")
	}
	if _, ok := MessageKey([]byte(`[1,2`), ptr("/0")); ok {
		t.Error("invalid JSON has no key")
	}
}

// The sender retries after a timeout and the engine stores both copies: each
// has its own engine key, so only the sender's id lets the backend see that
// they are the same message.
func TestHTTP_KeyPointerDeduplicatesSenderRetries(t *testing.T) {
	var mu sync.Mutex
	var idem, delivery []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		idem = append(idem, r.Header.Get("Idempotency-Key"))
		delivery = append(delivery, r.Header.Get(DeliveryIDHeader))
		mu.Unlock()
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{URL: srv.URL, KeyPointer: "/id", Headers: http.Header{"X-Delivery-Id": {"must-not-override"}}})
	ctx := context.Background()
	for _, d := range []struct{ key, line string }{
		{"ingest_1.list:7", `{"id":"evt_42","n":1}`},
		{"ingest_2.list:3", `{"id":"evt_42","n":1}`}, // the sender's retry
		{"ingest_2.list:4", `{"no_id":true}`},        // nothing to point at
	} {
		if err := h.Deliver(ctx, d.key, []byte(d.line)); err != nil {
			t.Fatal(err)
		}
	}
	if fmt.Sprint(idem) != "[evt_42 evt_42 ingest_2.list:4]" {
		t.Fatalf("Idempotency-Key = %v", idem)
	}
	if fmt.Sprint(delivery) != "[ingest_1.list:7 ingest_2.list:3 ingest_2.list:4]" {
		t.Fatalf("%s = %v; it must always be the engine key", DeliveryIDHeader, delivery)
	}
}
