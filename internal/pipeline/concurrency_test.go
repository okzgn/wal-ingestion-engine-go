package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/okzgn/wal-ingestion-engine-go/internal/sink"
	"github.com/okzgn/wal-ingestion-engine-go/internal/spool"
)

// slowSink is a destination with latency: unlike fakeSink it does not hold a
// lock while "delivering", so concurrent deliveries really overlap.
type slowSink struct {
	delay  time.Duration
	failFn func(line string) error // nil = success
	gate   func(line string)       // optional: blocks a delivery

	mu        sync.Mutex
	delivered []string
	active    atomic.Int32
	peak      atomic.Int32
}

func (s *slowSink) Deliver(ctx context.Context, _ string, line []byte) error {
	n := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		p := s.peak.Load()
		if n <= p || s.peak.CompareAndSwap(p, n) {
			break
		}
	}
	l := string(line)
	if s.gate != nil {
		s.gate(l)
	}
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.failFn != nil {
		if err := s.failFn(l); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.delivered = append(s.delivered, l)
	s.mu.Unlock()
	return nil
}

func (s *slowSink) got() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.delivered...)
}

func (s *slowSink) counts() map[string]int {
	m := map[string]int{}
	for _, l := range s.got() {
		m[l]++
	}
	return m
}

func msg(i int) string { return fmt.Sprintf(`{"i":%d}`, i) }

func numbered(n int) string {
	var b bytes.Buffer
	for i := 1; i <= n; i++ {
		b.WriteString(msg(i) + "\n")
	}
	return b.String()
}

func concurrentEnv(t *testing.T, s sink.Sink, concurrency int, mutate func(*Options)) *env {
	t.Helper()
	return newEnv(t, func(o *Options) {
		o.Sink = s
		o.Concurrency = concurrency
		o.Retry = RetryPolicy{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}
		if mutate != nil {
			mutate(o)
		}
	})
}

func checkpointOf(t *testing.T, path string) uint64 {
	t.Helper()
	ck, v, err := OpenCheckpoint(path + spool.CheckpointExt)
	if err != nil {
		t.Fatal(err)
	}
	ck.Close()
	return v
}

// The reason SINK_CONCURRENCY exists: a file used to be delivered one message
// at a time, so the drain rate was WORKERS / latency whatever the load.
func TestProcessor_ConcurrentDeliveryIsFaster(t *testing.T) {
	const n, delay = 80, 10 * time.Millisecond
	s := &slowSink{delay: delay}
	e := concurrentEnv(t, s, 8, nil)
	p := e.writeFile(t, "fast.list", numbered(n))

	start := time.Now()
	if err := e.proc.ProcessFile(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)

	if sequential := n * delay; elapsed > sequential/2 {
		t.Fatalf("80 messages at 10 ms took %s; one at a time takes %s", elapsed, sequential)
	}
	if peak := s.peak.Load(); peak < 2 || peak > 8 {
		t.Fatalf("peak concurrency %d, want within 2..8", peak)
	}
	if c := s.counts(); len(c) != n {
		t.Fatalf("delivered %d distinct messages, want %d", len(c), n)
	}
	if exists(p) || exists(p+spool.CheckpointExt) {
		t.Fatal("file or checkpoint left behind")
	}
}

// With concurrency 1 the file is delivered strictly in order, as before.
func TestProcessor_OrderPreservedWithConcurrencyOne(t *testing.T) {
	s := &slowSink{delay: 200 * time.Microsecond}
	e := concurrentEnv(t, s, 1, nil)
	p := e.writeFile(t, "ordered.list", numbered(60))
	if err := e.proc.ProcessFile(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if s.peak.Load() != 1 {
		t.Fatalf("peak concurrency %d, want 1", s.peak.Load())
	}
	for i, l := range s.got() {
		if l != msg(i+1) {
			t.Fatalf("position %d holds %s: order broken", i, l)
		}
	}
}

// Progress is a contiguous prefix: while one line is stuck, only the lines
// within the window after it may start, and the checkpoint cannot pass it.
func TestProcessor_CheckpointIsContiguousPrefix(t *testing.T) {
	release := make(chan struct{})
	s := &slowSink{gate: func(l string) {
		if l == msg(3) {
			<-release
		}
	}}
	e := concurrentEnv(t, s, 4, func(o *Options) { o.CheckpointInterval = 1 })
	p := e.writeFile(t, "prefix.list", numbered(20))

	done := make(chan error, 1)
	go func() { done <- e.proc.ProcessFile(context.Background(), p) }()

	// Lines 1 and 2 complete; 3 is stuck; the window then lets 4, 5 and 6 through.
	deadline := time.Now().Add(3 * time.Second)
	for len(s.got()) < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("delivered only %v", s.got())
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond) // anything beyond the window would show up now
	got := s.got()
	sort.Strings(got)
	want := []string{msg(1), msg(2), msg(4), msg(5), msg(6)}
	sort.Strings(want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("while line 3 is stuck: delivered %v, want %v", got, want)
	}
	if v := checkpointOf(t, p); v != 2 {
		t.Fatalf("checkpoint = %d; it must stop right before the stuck line (2)", v)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if c := s.counts(); len(c) != 20 {
		t.Fatalf("delivered %d distinct messages, want 20", len(c))
	}
}

// A transient failure in the middle of the window: nothing is lost, the
// checkpoint stays before the failed line and, as long as the process lives,
// the neighbours that did get through are not delivered twice.
func TestProcessor_TransientFailureMidWindowLosesNothing(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	s := &slowSink{delay: time.Millisecond, failFn: func(l string) error {
		if l == msg(5) && down.Load() {
			return errors.New("backend hiccup")
		}
		return nil
	}}
	e := concurrentEnv(t, s, 4, nil) // CheckpointInterval = 2
	p := e.writeFile(t, "mid.list", numbered(12))

	err := e.proc.ProcessFile(context.Background(), p)
	if !errors.Is(err, ErrRetryLater) {
		t.Fatalf("want ErrRetryLater, got %v", err)
	}
	if v := checkpointOf(t, p); v != 4 {
		t.Fatalf("checkpoint = %d, want 4 (right before the failed line)", v)
	}
	first := s.counts()
	if first[msg(5)] != 0 {
		t.Fatal("the failed line cannot have been delivered")
	}
	// Whether its neighbours went out in the same window depends on timing;
	// what is guaranteed is that none beyond the window did.
	for l := range first {
		var i int
		fmt.Sscanf(l, `{"i":%d}`, &i)
		if i > 4+4 {
			t.Fatalf("%s was delivered beyond the window of the stuck prefix", l)
		}
	}

	down.Store(false)
	if err := e.proc.ProcessFile(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	c := s.counts()
	for i := 1; i <= 12; i++ {
		if c[msg(i)] != 1 {
			t.Fatalf("%s delivered %d times, want exactly 1 (no crash happened)", msg(i), c[msg(i)])
		}
	}
	if exists(p) {
		t.Fatal("completed file must be deleted")
	}
}

// After a crash the memory of what went out beyond the checkpoint is gone:
// those lines are delivered again, but never more than the documented bound
// of CheckpointInterval-1 + Concurrency per file.
func TestProcessor_RedeliveryAfterCrashIsBounded(t *testing.T) {
	const interval, conc = 5, 4
	var down atomic.Bool
	down.Store(true)
	s := &slowSink{failFn: func(l string) error {
		if l == msg(13) && down.Load() {
			return errors.New("down")
		}
		return nil
	}}
	mutate := func(o *Options) { o.CheckpointInterval = interval }
	e := concurrentEnv(t, s, conc, mutate)
	p := e.writeFile(t, "crash.list", numbered(40))
	if err := e.proc.ProcessFile(context.Background(), p); !errors.Is(err, ErrRetryLater) {
		t.Fatalf("want ErrRetryLater, got %v", err)
	}

	// "Crash": the checkpoint on disk is what the next process starts from.
	// Drop the last save to simulate the worst case, a crash right before it.
	if err := os.Remove(p + spool.CheckpointExt); err != nil {
		t.Fatal(err)
	}
	ck, _, _ := OpenCheckpoint(p + spool.CheckpointExt)
	_ = ck.Save(10) // the last multiple of the interval before line 13
	ck.Close()

	down.Store(false)
	fresh := NewProcessor(Options{Sink: s, FailedDir: e.failed, Concurrency: conc, CheckpointInterval: interval, Logger: quiet})
	if err := fresh.ProcessFile(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	dups := 0
	c := s.counts()
	for i := 1; i <= 40; i++ {
		if c[msg(i)] == 0 {
			t.Fatalf("%s lost", msg(i))
		}
		dups += c[msg(i)] - 1
	}
	if bound := interval - 1 + conc; dups > bound {
		t.Fatalf("%d duplicates after a crash exceed the bound of %d", dups, bound)
	}
}

// Every kind of line, at several concurrency levels: the set of deliveries and
// of dead letters must be the same, only the order may differ.
func TestProcessor_SameOutcomeAtAnyConcurrency(t *testing.T) {
	huge := `{"big":"` + strings.Repeat("x", 5000) + `"}`
	content := msg(1) + "\nnot json\n" + msg(3) + "\n{\"poison\":true}\n" + huge + "\n\n" + msg(7) + "\n" + msg(8) + "\n{\"torn\":"
	for _, c := range []int{1, 2, 8, 64} {
		t.Run(fmt.Sprintf("concurrency=%d", c), func(t *testing.T) {
			s := &slowSink{delay: 500 * time.Microsecond, failFn: func(l string) error {
				if strings.Contains(l, "poison") {
					return &sink.PermanentError{Err: errors.New("400 bad request")}
				}
				return nil
			}}
			e := concurrentEnv(t, s, c, nil)
			p := e.writeFile(t, "mix.list", content)
			if err := e.proc.ProcessFile(context.Background(), p); err != nil {
				t.Fatal(err)
			}
			got := s.got()
			sort.Strings(got)
			if want := []string{msg(1), msg(3), msg(7), msg(8)}; fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("delivered %v, want %v", got, want)
			}
			bad, err := os.ReadFile(filepath.Join(e.failed, "mix.list.bad"))
			if err != nil {
				t.Fatal(err)
			}
			var lines []string
			for _, rec := range strings.Split(strings.TrimSuffix(string(bad), "\n"), "\n") {
				lines = append(lines, strings.SplitN(rec, "\t", 2)[0])
			}
			sort.Strings(lines)
			if fmt.Sprint(lines) != "[2 4 5 6 9]" {
				t.Fatalf("dead-lettered lines %v, want [2 4 5 6 9]", lines)
			}
			if exists(p) || exists(p+spool.CheckpointExt) {
				t.Fatal("file or checkpoint left behind")
			}
		})
	}
}

// Shutdown in the middle of a concurrent file: nothing lost on resume.
func TestProcessor_CancelMidWindowResumes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var seen atomic.Int32
	s := &slowSink{delay: time.Millisecond, gate: func(string) {
		if seen.Add(1) == 20 {
			cancel()
		}
	}}
	e := concurrentEnv(t, s, 8, nil)
	p := e.writeFile(t, "cancel.list", numbered(100))

	if err := e.proc.ProcessFile(ctx, p); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if !exists(p) {
		t.Fatal("file must stay for the next run")
	}
	if v := checkpointOf(t, p); v == 0 || v >= 100 {
		t.Fatalf("checkpoint = %d, want progress short of the end", v)
	}

	fresh := NewProcessor(Options{Sink: s, FailedDir: e.failed, Concurrency: 8, CheckpointInterval: 2, Logger: quiet})
	if err := fresh.ProcessFile(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	c := s.counts()
	dups := 0
	for i := 1; i <= 100; i++ {
		if c[msg(i)] == 0 {
			t.Fatalf("%s lost across the restart", msg(i))
		}
		dups += c[msg(i)] - 1
	}
	if dups > 8 {
		t.Fatalf("%d duplicates after a clean shutdown, want at most the window (8)", dups)
	}
}
