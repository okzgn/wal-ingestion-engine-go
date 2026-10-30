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

func TestHTTP_StatusClassification(t *testing.T) {
	tests := []struct {
		code      int
		wantErr   bool
		permanent bool
	}{
		{200, false, false},
		{204, false, false},
		{400, true, true},
		{401, true, true},
		{404, true, true},
		{408, true, false},
		{425, true, false},
		{429, true, false},
		{500, true, false},
		{503, true, false},
	}
	for _, tc := range tests {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.code)
		}))
		err := NewHTTP(srv.URL, time.Second).Deliver(context.Background(), "f:1", []byte(`{"a":1}`))
		srv.Close()
		if (err != nil) != tc.wantErr {
			t.Errorf("%d: err=%v wantErr=%v", tc.code, err, tc.wantErr)
		}
		if err != nil && IsPermanent(err) != tc.permanent {
			t.Errorf("%d: permanent=%v want %v", tc.code, IsPermanent(err), tc.permanent)
		}
	}
}

func TestHTTP_SendsBodyAndIdempotencyKey(t *testing.T) {
	var gotKey, gotType, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("Idempotency-Key")
		gotType = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
	}))
	defer srv.Close()

	if err := NewHTTP(srv.URL, time.Second).Deliver(context.Background(), "ingest_1.list:7", []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if gotKey != "ingest_1.list:7" || gotType != "application/json" || gotBody != `{"a":1}` {
		t.Fatalf("key=%q type=%q body=%q", gotKey, gotType, gotBody)
	}
}

func TestHTTP_NetworkErrorIsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // nobody listens any more

	err := NewHTTP(url, 200*time.Millisecond).Deliver(context.Background(), "k", []byte(`{}`))
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
