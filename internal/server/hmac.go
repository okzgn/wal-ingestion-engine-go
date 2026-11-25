package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"
)

// Errors returned by Verifier.Verify.
var (
	ErrMissingSignature = errors.New("missing signature")
	ErrBadSignature     = errors.New("invalid signature")
	ErrBadTimestamp     = errors.New("missing, malformed or stale timestamp")
)

// Scheme is the way a sender signs its webhooks. Every scheme is HMAC-SHA256
// with a shared secret; they differ in the headers, in what exactly is signed
// and in how the digest is written.
type Scheme string

const (
	// SchemeGeneric: "<header>: sha256=<hex>" (a bare hex digest is accepted
	// too) over the raw body. With a timestamp header the MAC covers
	// "<timestamp>.<body>" and the timestamp (unix seconds) must be fresh.
	SchemeGeneric Scheme = "generic"
	// SchemeGitHub: "X-Hub-Signature-256: sha256=<hex>" over the raw body.
	// GitHub sends no timestamp, so there is no replay window.
	SchemeGitHub Scheme = "github"
	// SchemeStripe: "Stripe-Signature: t=<unix>,v1=<hex>[,v1=<hex>...]" over
	// "<t>.<body>". Several v1 values appear while a secret is being rolled;
	// one valid is enough. The timestamp must be fresh.
	SchemeStripe Scheme = "stripe"
	// SchemeSlack: "X-Slack-Signature: v0=<hex>" over "v0:<timestamp>:<body>",
	// the timestamp coming in X-Slack-Request-Timestamp and having to be fresh.
	SchemeSlack Scheme = "slack"
)

// schemeDefaults are the headers each scheme uses unless configured otherwise.
// usesTimestampHeader tells whether the scheme reads a separate timestamp
// header at all.
var schemeDefaults = map[Scheme]struct {
	signature, timestamp string
	usesTimestampHeader  bool
}{
	SchemeGeneric: {signature: "X-Signature-256", usesTimestampHeader: true}, // timestamp header optional
	SchemeGitHub:  {signature: "X-Hub-Signature-256"},
	SchemeStripe:  {signature: "Stripe-Signature"},
	SchemeSlack:   {signature: "X-Slack-Signature", timestamp: "X-Slack-Request-Timestamp", usesTimestampHeader: true},
}

// ValidScheme reports whether name is a known signature scheme.
func ValidScheme(name string) bool {
	_, ok := schemeDefaults[Scheme(strings.ToLower(name))]
	return ok
}

// SchemeHeaders returns the signature and timestamp headers a scheme reads,
// after applying the configured overrides (empty = the scheme's own).
func SchemeHeaders(scheme Scheme, signature, timestamp string) (sig, ts string) {
	d := schemeDefaults[scheme]
	if signature == "" {
		signature = d.signature
	}
	if !d.usesTimestampHeader {
		return signature, ""
	}
	if timestamp == "" {
		timestamp = d.timestamp
	}
	return signature, timestamp
}

// Verifier checks webhook signatures.
type Verifier struct {
	scheme    Scheme
	secret    []byte
	tolerance time.Duration
	useTime   bool // generic scheme only: a timestamp header is configured
	now       func() time.Time
}

// NewVerifier returns a Verifier for the generic scheme. useTimestamp enables
// the replay check.
func NewVerifier(secret string, useTimestamp bool, tolerance time.Duration) *Verifier {
	return NewSchemeVerifier(SchemeGeneric, secret, useTimestamp, tolerance)
}

// NewSchemeVerifier returns a Verifier for scheme. useTimestamp only matters
// for the generic scheme, where the timestamp header is optional.
func NewSchemeVerifier(scheme Scheme, secret string, useTimestamp bool, tolerance time.Duration) *Verifier {
	if tolerance <= 0 {
		tolerance = 5 * time.Minute
	}
	if scheme == "" {
		scheme = SchemeGeneric
	}
	return &Verifier{scheme: scheme, secret: []byte(secret), useTime: useTimestamp, tolerance: tolerance, now: time.Now}
}

// Verify validates signature (and timestamp, for the schemes that carry it in
// its own header) against the raw body.
func (v *Verifier) Verify(signature, timestamp string, body []byte) error {
	signature = strings.TrimSpace(signature)
	if signature == "" {
		return ErrMissingSignature
	}
	switch v.scheme {
	case SchemeStripe:
		return v.verifyStripe(signature, body)
	case SchemeSlack:
		ts := strings.TrimSpace(timestamp)
		if err := v.fresh(ts); err != nil {
			return err
		}
		digest, ok := strings.CutPrefix(signature, "v0=")
		if !ok {
			return ErrBadSignature
		}
		return v.match(digest, "v0:"+ts+":", body)
	case SchemeGitHub:
		digest, ok := strings.CutPrefix(signature, "sha256=")
		if !ok {
			return ErrBadSignature
		}
		return v.match(digest, "", body)
	default:
		prefix := ""
		if v.useTime {
			ts := strings.TrimSpace(timestamp)
			if err := v.fresh(ts); err != nil {
				return err
			}
			prefix = ts + "."
		}
		return v.match(strings.TrimPrefix(signature, "sha256="), prefix, body)
	}
}

// verifyStripe parses "t=<unix>,v1=<hex>,v1=<hex>,v0=<hex>". Unknown fields
// are ignored, as Stripe asks, so the header can grow new versions.
func (v *Verifier) verifyStripe(header string, body []byte) error {
	var ts string
	var digests []string
	for _, part := range strings.Split(header, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		switch key {
		case "t":
			ts = value
		case "v1":
			digests = append(digests, value)
		}
	}
	if err := v.fresh(ts); err != nil {
		return err
	}
	if len(digests) == 0 {
		return ErrBadSignature
	}
	want := v.mac(ts+".", body)
	for _, d := range digests {
		if got, err := hex.DecodeString(d); err == nil && hmac.Equal(got, want) {
			return nil
		}
	}
	return ErrBadSignature
}

// fresh checks that ts (unix seconds) is within the tolerance of now, which is
// what stops a captured request from being replayed later.
func (v *Verifier) fresh(ts string) error {
	secs, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return ErrBadTimestamp
	}
	skew := v.now().Sub(time.Unix(secs, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew > v.tolerance {
		return ErrBadTimestamp
	}
	return nil
}

func (v *Verifier) mac(prefix string, body []byte) []byte {
	mac := hmac.New(sha256.New, v.secret)
	mac.Write([]byte(prefix))
	mac.Write(body)
	return mac.Sum(nil)
}

// match compares a hex digest with the MAC of prefix+body in constant time.
func (v *Verifier) match(hexDigest, prefix string, body []byte) error {
	got, err := hex.DecodeString(hexDigest)
	if err != nil {
		return ErrBadSignature
	}
	if !hmac.Equal(got, v.mac(prefix, body)) {
		return ErrBadSignature
	}
	return nil
}
