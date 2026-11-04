package logutil

import (
	"sync"
	"testing"
	"time"
)

func TestThrottle_OnePerIntervalWithSuppressedCount(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	th := NewThrottle(time.Second)
	th.now = func() time.Time { return now }

	if ok, n := th.Allow(); !ok || n != 0 {
		t.Fatalf("first call: ok=%v suppressed=%d", ok, n)
	}
	for i := 0; i < 5; i++ {
		if ok, _ := th.Allow(); ok {
			t.Fatalf("call %d inside the interval must be suppressed", i)
		}
	}
	now = now.Add(999 * time.Millisecond)
	if ok, _ := th.Allow(); ok {
		t.Fatal("still inside the interval")
	}
	now = now.Add(time.Millisecond)
	if ok, n := th.Allow(); !ok || n != 6 {
		t.Fatalf("after the interval: ok=%v suppressed=%d, want true/6", ok, n)
	}
	if ok, n := th.Allow(); ok || n != 0 {
		t.Fatalf("counter must reset: ok=%v suppressed=%d", ok, n)
	}
}

func TestThrottle_ConcurrentCallersLoseNothing(t *testing.T) {
	now := time.Unix(0, 0)
	th := NewThrottle(time.Hour)
	th.now = func() time.Time { return now }
	th.Allow()

	var wg sync.WaitGroup
	const n = 1000
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); th.Allow() }()
	}
	wg.Wait()
	now = now.Add(2 * time.Hour)
	if ok, got := th.Allow(); !ok || got != n {
		t.Fatalf("ok=%v suppressed=%d, want %d", ok, got, n)
	}
}
