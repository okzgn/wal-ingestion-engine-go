package sink

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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
