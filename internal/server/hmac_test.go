package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"testing"
	"time"
)

func hexMAC(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// The example of GitHub's "Validating webhook deliveries" documentation.
func TestScheme_GitHubDocumentedExample(t *testing.T) {
	v := NewSchemeVerifier(SchemeGitHub, "It's a Secret to Everybody", false, 0)
	body := []byte("Hello, World!")
	const sig = "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17"
	if err := v.Verify(sig, "", body); err != nil {
		t.Fatalf("documented example rejected: %v", err)
	}
	if err := v.Verify(sig, "", []byte("Hello, World?")); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("tampered body: %v", err)
	}
	// GitHub always sends the prefix: a bare digest is not its format.
	if err := v.Verify(sig[len("sha256="):], "", body); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("digest without prefix: %v", err)
	}
	if err := v.Verify("", "", body); !errors.Is(err, ErrMissingSignature) {
		t.Fatalf("missing: %v", err)
	}
}

// The example of Slack's "Verifying requests from Slack" documentation.
func TestScheme_SlackDocumentedExample(t *testing.T) {
	const (
		secret = "8f742231b10e8888abcd99yyyzzz85a5"
		ts     = "1531420618"
		body   = "token=xyzz0WbapA4vBCDEFasx0q6G&team_id=T1DC2JH3J&team_domain=testteamnow&channel_id=G8PSS9T3V&channel_name=foobar&user_id=U2CERLKJA&user_name=roadrunner&command=%2Fwebhook-collect&text=&response_url=https%3A%2F%2Fhooks.slack.com%2Fcommands%2FT1DC2JH3J%2F397700885554%2F96rGlfmibIGlgcZRskXaIFfN&trigger_id=398738663015.47445629121.803a0bc887a14d10d2c447fce8b6703c"
		sig    = "v0=a2114d57b48eac39b9ad189dd8316235a7b4a8d21a10bd27519666489c69b503"
	)
	v := NewSchemeVerifier(SchemeSlack, secret, false, 5*time.Minute)
	v.now = func() time.Time { return time.Unix(1531420618, 0).Add(30 * time.Second) }
	if err := v.Verify(sig, ts, []byte(body)); err != nil {
		t.Fatalf("documented example rejected: %v", err)
	}
	// The same request replayed an hour later.
	v.now = func() time.Time { return time.Unix(1531420618, 0).Add(time.Hour) }
	if err := v.Verify(sig, ts, []byte(body)); !errors.Is(err, ErrBadTimestamp) {
		t.Fatalf("replay: %v", err)
	}
	v.now = func() time.Time { return time.Unix(1531420618, 0) }
	if err := v.Verify(sig, "1531420619", []byte(body)); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("the timestamp must be covered by the MAC: %v", err)
	}
	if err := v.Verify("sha256="+sig[3:], ts, []byte(body)); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("wrong version prefix: %v", err)
	}
	if err := v.Verify(sig, "", []byte(body)); !errors.Is(err, ErrBadTimestamp) {
		t.Fatalf("missing timestamp: %v", err)
	}
}

// Stripe publishes the algorithm, not a fixed example: the signatures here
// are built as its documentation describes (HMAC-SHA256 of "<t>.<body>").
func TestScheme_Stripe(t *testing.T) {
	const secret = "whsec_test_secret"
	now := time.Unix(1_700_000_000, 0)
	ts := strconv.FormatInt(now.Unix(), 10)
	body := `{"id":"evt_1","type":"charge.succeeded"}`
	good := hexMAC(secret, ts+"."+body)

	v := NewSchemeVerifier(SchemeStripe, secret, false, 5*time.Minute)
	v.now = func() time.Time { return now }

	for name, header := range map[string]string{
		"plain":                 "t=" + ts + ",v1=" + good,
		"with legacy v0":        "t=" + ts + ",v1=" + good + ",v0=deadbeef",
		"rolled secret, second": "t=" + ts + ",v1=" + hexMAC("whsec_old", ts+"."+body) + ",v1=" + good,
		"spaces and unknown":    "t=" + ts + ", v2=zz, v1=" + good,
	} {
		if err := v.Verify(header, "", []byte(body)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, tc := range map[string]struct {
		header string
		want   error
	}{
		"wrong secret":       {"t=" + ts + ",v1=" + hexMAC("other", ts+"."+body), ErrBadSignature},
		"only v0":            {"t=" + ts + ",v0=" + good, ErrBadSignature},
		"no timestamp":       {"v1=" + good, ErrBadTimestamp},
		"timestamp replaced": {"t=" + strconv.FormatInt(now.Unix()+1, 10) + ",v1=" + good, ErrBadSignature},
		"stale":              {"t=1600000000,v1=" + hexMAC(secret, "1600000000."+body), ErrBadTimestamp},
		"garbage":            {"hello", ErrBadTimestamp},
	} {
		if err := v.Verify(tc.header, "", []byte(body)); !errors.Is(err, tc.want) {
			t.Errorf("%s: got %v, want %v", name, err, tc.want)
		}
	}
	if err := v.Verify("t="+ts+",v1="+good, "", []byte(body+" ")); !errors.Is(err, ErrBadSignature) {
		t.Errorf("tampered body: %v", err)
	}
}

func TestSchemeHeaders(t *testing.T) {
	for _, tc := range []struct {
		scheme                  Scheme
		sigIn, tsIn, sig, tsOut string
	}{
		{SchemeGeneric, "", "", "X-Signature-256", ""},
		{SchemeGeneric, "X-Sig", "X-Ts", "X-Sig", "X-Ts"},
		{SchemeGitHub, "", "", "X-Hub-Signature-256", ""},
		{SchemeStripe, "", "", "Stripe-Signature", ""},
		{SchemeSlack, "", "", "X-Slack-Signature", "X-Slack-Request-Timestamp"},
		{SchemeSlack, "", "X-Ts", "X-Slack-Signature", "X-Ts"},
	} {
		sig, ts := SchemeHeaders(tc.scheme, tc.sigIn, tc.tsIn)
		if sig != tc.sig || ts != tc.tsOut {
			t.Errorf("%s(%q,%q) = %q,%q; want %q,%q", tc.scheme, tc.sigIn, tc.tsIn, sig, ts, tc.sig, tc.tsOut)
		}
	}
	if !ValidScheme("Stripe") || ValidScheme("paypal") {
		t.Error("ValidScheme")
	}
}

// End to end through the handler: each scheme reads its own headers.
func TestHandler_SignatureSchemes(t *testing.T) {
	const secret = "s3cret"
	body := `{"event":"ping"}`
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	for _, tc := range []struct {
		scheme Scheme
		good   map[string]string
	}{
		{SchemeGitHub, map[string]string{"X-Hub-Signature-256": "sha256=" + hexMAC(secret, body)}},
		{SchemeStripe, map[string]string{"Stripe-Signature": "t=" + ts + ",v1=" + hexMAC(secret, ts+"."+body)}},
		{SchemeSlack, map[string]string{"X-Slack-Signature": "v0=" + hexMAC(secret, "v0:"+ts+":"+body), "X-Slack-Request-Timestamp": ts}},
	} {
		w := &fakeAppender{}
		s := newServer(t, w, func(c *Config) { c.Secret = secret; c.SignatureScheme = tc.scheme })
		if code, _, _ := post(t, s, body, nil); code != 401 {
			t.Errorf("%s: unsigned request got %d, want 401", tc.scheme, code)
		}
		// A valid generic signature, in the generic header, is not what this scheme reads.
		if code, _, _ := post(t, s, body, map[string]string{"X-Signature-256": "sha256=" + hexMAC(secret, body)}); code != 401 {
			t.Errorf("%s: the generic header was accepted (%d)", tc.scheme, code)
		}
		if code, msg, _ := post(t, s, body, tc.good); code != 200 {
			t.Errorf("%s: signed request got %d (%s), want 200", tc.scheme, code, msg)
		}
		if len(w.got()) != 1 {
			t.Errorf("%s: stored %d messages, want 1", tc.scheme, len(w.got()))
		}
	}
}
