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

// Verifier checks HMAC-SHA256 webhook signatures.
//
// The signature header carries "sha256=<hex>" (a bare hex digest is accepted
// too). Without a timestamp header the MAC covers the raw body. When a
// timestamp header is configured the MAC covers "<timestamp>.<body>" and the
// timestamp (unix seconds) must be within Tolerance of now, which blocks
// replays.
type Verifier struct {
	secret    []byte
	tolerance time.Duration
	useTime   bool
	now       func() time.Time
}

// NewVerifier returns a Verifier. useTimestamp enables the replay check.
func NewVerifier(secret string, useTimestamp bool, tolerance time.Duration) *Verifier {
	if tolerance <= 0 {
		tolerance = 5 * time.Minute
	}
	return &Verifier{secret: []byte(secret), useTime: useTimestamp, tolerance: tolerance, now: time.Now}
}

// Verify validates signature (and timestamp) against the raw body.
func (v *Verifier) Verify(signature, timestamp string, body []byte) error {
	if signature == "" {
		return ErrMissingSignature
	}
	got, err := hex.DecodeString(strings.TrimPrefix(strings.TrimSpace(signature), "sha256="))
	if err != nil {
		return ErrBadSignature
	}

	mac := hmac.New(sha256.New, v.secret)
	if v.useTime {
		secs, err := strconv.ParseInt(strings.TrimSpace(timestamp), 10, 64)
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
		mac.Write([]byte(strings.TrimSpace(timestamp)))
		mac.Write([]byte("."))
	}
	mac.Write(body)

	if !hmac.Equal(got, mac.Sum(nil)) {
		return ErrBadSignature
	}
	return nil
}
