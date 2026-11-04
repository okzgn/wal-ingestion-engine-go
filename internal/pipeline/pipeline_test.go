package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/okzgn/wal-ingestion-engine-go/internal/sink"
	"github.com/okzgn/wal-ingestion-engine-go/internal/spool"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// ---- checkpoint ----

func TestCheckpoint_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f.ckpt")
	c, v, err := OpenCheckpoint(path)
	if err != nil || v != 0 {
		t.Fatalf("fresh checkpoint: v=%d err=%v", v, err)
	}
	for _, n := range []uint64{50, 100, 150} {
		if err := c.Save(n); err != nil {
			t.Fatal(err)
		}
	}
	c.Close()

	c, v, err = OpenCheckpoint(path)
	if err != nil || v != 150 {
		t.Fatalf("reopen: v=%d err=%v", v, err)
	}
	c.Close()
}

// Regression M7: the old text checkpoint could be torn and was parsed with an
// ignored error. A damaged slot must never lose the previous value.
func TestCheckpoint_TornWriteKeepsPreviousValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f.ckpt")
	c, _, _ := OpenCheckpoint(path)
	_ = c.Save(100)
	_ = c.Save(150) // lands in the other slot
	c.Close()

	data, _ := os.ReadFile(path)
	// Corrupt the slot that holds 150 (the second one).
	data[slotSize+2] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	c, v, err := OpenCheckpoint(path)
	if err != nil || v != 100 {
		t.Fatalf("want fallback to 100, got %d (err=%v)", v, err)
	}
	// The next save must not overwrite the surviving slot.
	_ = c.Save(200)
	c.Close()
	c, v, _ = OpenCheckpoint(path)
	c.Close()
	if v != 200 {
		t.Fatalf("want 200, got %d", v)
	}
}

func TestCheckpoint_GarbageMeansZero(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f.ckpt")
	// A legacy text checkpoint ("50" padded with spaces) is not a valid slot.
	if err := os.WriteFile(path, []byte(fmt.Sprintf("%-20d", 50)), 0o644); err != nil {
		t.Fatal(err)
	}
	c, v, err := OpenCheckpoint(path)
	if err != nil || v != 0 {
		t.Fatalf("want 0, got %d (err=%v)", v, err)
	}
	c.Close()
}

// ---- line reader ----

func TestLineReader(t *testing.T) {
	big := strings.Repeat("x", 200_000)
	in := "a\n" + big + "\nb\n\ntail"
	lr := newLineReader(strings.NewReader(in), 1000)

	type rec struct {
		line       string
		tooLong    bool
		terminated bool
	}
	var got []rec
	for {
		l, tl, term, err := lr.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		s := string(l)
		if tl {
			s = "<truncated>"
		}
		got = append(got, rec{s, tl, term})
	}
	want := []rec{
		{"a", false, true},
		{"<truncated>", true, true},
		{"b", false, true},
		{"", false, true},
		{"tail", false, false},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
}

func TestLineReader_ExactlyMaxIsAccepted(t *testing.T) {
	line := strings.Repeat("y", 100)
	lr := newLineReader(strings.NewReader(line+"\n"), 100)
	l, tooLong, term, err := lr.next()
	if err != nil || tooLong || !term || string(l) != line {
		t.Fatalf("line of exactly max bytes rejected: tooLong=%v term=%v err=%v", tooLong, term, err)
	}
}

func FuzzLineReader(f *testing.F) {
	f.Add("a\nb\n", 10)
	f.Add("no newline", 4)
	f.Add("\n\n\n", 1)
	f.Fuzz(func(t *testing.T, in string, max int) {
		if max < 1 || max > 1<<16 {
			t.Skip()
		}
		lr := newLineReader(strings.NewReader(in), max)
		total := 0
		for i := 0; i < len(in)+2; i++ {
			l, tooLong, _, err := lr.next()
			if err != nil {
				break
			}
			if !tooLong && len(l) > max {
				t.Fatalf("line of %d bytes exceeds max %d without tooLong", len(l), max)
			}
			total += len(l)
		}
		if total > len(in) {
			t.Fatal("reader produced more bytes than it was given")
		}
	})
}

// ---- processor ----

// fakeSink records deliveries and fails on demand.
type fakeSink struct {
	mu        sync.Mutex
	delivered []string
	keys      []string
	// failFn decides the result for a message; nil means success.
	failFn func(line string, call int) error
	calls  int
}

func (s *fakeSink) Deliver(ctx context.Context, key string, line []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failFn != nil {
		if err := s.failFn(string(line), s.calls); err != nil {
			return err
		}
	}
	s.delivered = append(s.delivered, string(line))
	s.keys = append(s.keys, key)
	return nil
}

func (s *fakeSink) got() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.delivered...)
}

type env struct {
	dir    string
	proc   *Processor
	sink   *fakeSink
	failed string
}

func newEnv(t *testing.T, mutate func(*Options)) *env {
	t.Helper()
	root := t.TempDir()
	e := &env{dir: root, sink: &fakeSink{}, failed: filepath.Join(root, "failed")}
	if err := os.MkdirAll(e.failed, 0o755); err != nil {
		t.Fatal(err)
	}
	o := Options{
		Sink:               e.sink,
		FailedDir:          e.failed,
		MaxLineSize:        1024,
		CheckpointInterval: 2,
		Retry:              RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond},
		Logger:             quiet,
	}
	if mutate != nil {
		mutate(&o)
	}
	e.proc = NewProcessor(o)
	return e
}

func (e *env) writeFile(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(e.dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func exists(p string) bool { _, err := os.Stat(p); return err == nil }

func TestProcessor_HappyPathDeletesFileAndCheckpoint(t *testing.T) {
	e := newEnv(t, nil)
	p := e.writeFile(t, "a.list", "{\"i\":1}\n{\"i\":2}\n{\"i\":3}\n")
	if err := e.proc.ProcessFile(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if got := e.sink.got(); len(got) != 3 {
		t.Fatalf("delivered %v", got)
	}
	if e.sink.keys[0] != "a.list:1" || e.sink.keys[2] != "a.list:3" {
		t.Fatalf("keys must be <file>:<line>, got %v", e.sink.keys)
	}
	if exists(p) || exists(p+spool.CheckpointExt) {
		t.Fatal("file or checkpoint left behind")
	}
}

// Regression C1: a sink error used to advance the checkpoint past the failed
// line, report "completed" and delete the rest of the file.
func TestWorker_SinkFailureKeepsFileAndCheckpoint(t *testing.T) {
	e := newEnv(t, nil)
	e.sink.failFn = func(line string, _ int) error {
		if strings.Contains(line, "FAILME") {
			return errors.New("backend down")
		}
		return nil
	}
	content := "{\"id\":\"A\"}\n{\"id\":\"B\"}\n{\"id\":\"FAILME\"}\n{\"id\":\"D\"}\n{\"id\":\"E\"}\n{\"id\":\"F\"}\n"
	p := e.writeFile(t, "b.list", content)

	err := e.proc.ProcessFile(context.Background(), p)
	if !errors.Is(err, ErrRetryLater) {
		t.Fatalf("want ErrRetryLater, got %v", err)
	}
	if !exists(p) {
		t.Fatal("data file was deleted although messages are undelivered")
	}
	if got := e.sink.got(); len(got) != 2 {
		t.Fatalf("before the failure only A and B are delivered, got %v", got)
	}
	ck, v, _ := OpenCheckpoint(p + spool.CheckpointExt)
	ck.Close()
	if v != 2 {
		t.Fatalf("checkpoint must stay right before the failed line (2), got %d", v)
	}

	// The backend recovers: everything from the failed line on must arrive.
	e.sink.failFn = nil
	if err := e.proc.ProcessFile(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	got := e.sink.got()
	want := []string{`{"id":"A"}`, `{"id":"B"}`, `{"id":"FAILME"}`, `{"id":"D"}`, `{"id":"E"}`, `{"id":"F"}`}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v\nwant %v (nothing lost, nothing duplicated)", got, want)
	}
	if exists(p) || exists(p+spool.CheckpointExt) {
		t.Fatal("completed file must be cleaned up")
	}
}

func TestProcessor_RetriesTransientErrorsThenSucceeds(t *testing.T) {
	e := newEnv(t, nil)
	e.sink.failFn = func(_ string, call int) error {
		if call <= 2 {
			return errors.New("temporary")
		}
		return nil
	}
	p := e.writeFile(t, "c.list", "{\"i\":1}\n")
	if err := e.proc.ProcessFile(context.Background(), p); err != nil {
		t.Fatalf("2 failures then success must complete within 3 attempts: %v", err)
	}
	if len(e.sink.got()) != 1 {
		t.Fatal("message not delivered")
	}
}

func TestProcessor_PermanentErrorGoesToDeadLetterAndContinues(t *testing.T) {
	e := newEnv(t, nil)
	e.sink.failFn = func(line string, _ int) error {
		if strings.Contains(line, "poison") {
			return &sink.PermanentError{Err: errors.New("400 bad request")}
		}
		return nil
	}
	p := e.writeFile(t, "d.list", "{\"i\":1}\n{\"poison\":true}\n{\"i\":3}\n")
	if err := e.proc.ProcessFile(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if got := e.sink.got(); len(got) != 2 {
		t.Fatalf("delivered %v", got)
	}
	bad, err := os.ReadFile(filepath.Join(e.failed, "d.list.bad"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(bad), "2\t") || !strings.Contains(string(bad), `{"poison":true}`) {
		t.Fatalf("dead letter = %q", bad)
	}
}

// Regression C2 (defence in depth): invalid lines are kept, not silently dropped.
func TestProcessor_InvalidAndTornLinesAreDeadLettered(t *testing.T) {
	e := newEnv(t, nil)
	p := e.writeFile(t, "f.list", "{\"i\":1}\nnot json\n\n{\"i\":4}\n{\"torn\":")
	if err := e.proc.ProcessFile(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if got := e.sink.got(); len(got) != 2 {
		t.Fatalf("delivered %v", got)
	}
	bad, _ := os.ReadFile(filepath.Join(e.failed, "f.list.bad"))
	lines := strings.Split(strings.TrimSpace(string(bad)), "\n")
	if len(lines) != 3 {
		t.Fatalf("want 3 dead letters (invalid, empty, torn), got %q", bad)
	}
	if !strings.Contains(lines[2], "torn write") {
		t.Fatalf("last dead letter should be the torn line: %q", lines[2])
	}
}

func TestProcessor_ResumesFromCheckpoint(t *testing.T) {
	e := newEnv(t, nil)
	p := e.writeFile(t, "g.list", "{\"i\":1}\n{\"i\":2}\n{\"i\":3}\n{\"i\":4}\n")
	ck, _, _ := OpenCheckpoint(p + spool.CheckpointExt)
	_ = ck.Save(2)
	ck.Close()

	if err := e.proc.ProcessFile(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if got := e.sink.got(); fmt.Sprint(got) != `[{"i":3} {"i":4}]` {
		t.Fatalf("must resume after line 2, got %v", got)
	}
}

func TestShutdown_MidFileResumes(t *testing.T) {
	e := newEnv(t, nil)
	var b bytes.Buffer
	for i := 1; i <= 10; i++ {
		fmt.Fprintf(&b, "{\"i\":%d}\n", i)
	}
	p := e.writeFile(t, "h.list", b.String())

	ctx, cancel := context.WithCancel(context.Background())
	e.sink.failFn = func(line string, call int) error {
		if call == 5 {
			cancel() // shutdown arrives while processing the 5th message
		}
		return nil
	}
	err := e.proc.ProcessFile(ctx, p)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if !exists(p) {
		t.Fatal("file must stay for the next run")
	}

	e.sink.failFn = nil
	if err := e.proc.ProcessFile(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, l := range e.sink.got() {
		seen[l]++
	}
	for i := 1; i <= 10; i++ {
		if seen[fmt.Sprintf(`{"i":%d}`, i)] == 0 {
			t.Fatalf("message %d lost across the restart", i)
		}
	}
	dups := 0
	for _, n := range seen {
		dups += n - 1
	}
	if dups != 0 {
		t.Fatalf("a clean shutdown checkpoints exactly, yet %d duplicates were delivered", dups)
	}
}

func TestBackoff_IsBoundedAndGrows(t *testing.T) {
	base, max := 100*time.Millisecond, time.Second
	for attempt := 1; attempt <= 12; attempt++ {
		for i := 0; i < 50; i++ {
			d := Backoff(attempt, base, max)
			if d <= 0 || d > max {
				t.Fatalf("attempt %d: delay %s outside (0,%s]", attempt, d, max)
			}
		}
	}
	if d := Backoff(1, base, max); d > base {
		t.Fatalf("first delay %s exceeds base", d)
	}
}

// ---- runner ----

func TestRunner_RetriesPostponedFilesUntilTheSinkRecovers(t *testing.T) {
	root := t.TempDir()
	sp, err := spool.Open(root, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()

	fs := &fakeSink{}
	var mu sync.Mutex
	down := true
	fs.failFn = func(string, int) error {
		mu.Lock()
		defer mu.Unlock()
		if down {
			return errors.New("down")
		}
		return nil
	}
	proc := NewProcessor(Options{
		Sink: fs, FailedDir: sp.Dirs.Failed, Logger: quiet,
		Retry: RetryPolicy{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
	})
	r := &Runner{Spool: sp, Proc: proc, Workers: 2, RetryBase: 5 * time.Millisecond, RetryMax: 10 * time.Millisecond, Logger: quiet}

	path := filepath.Join(sp.Dirs.Processing, "x.list")
	if err := os.WriteFile(path, []byte("{\"i\":1}\n{\"i\":2}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sp.Disp.Add(path, 16)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	time.Sleep(60 * time.Millisecond) // several failed rounds
	if !exists(path) {
		t.Fatal("file must survive while the sink is down")
	}
	mu.Lock()
	down = false
	mu.Unlock()

	deadline := time.Now().Add(3 * time.Second)
	for exists(path) {
		if time.Now().After(deadline) {
			t.Fatal("file never processed after the sink recovered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(fs.got()) != 2 {
		t.Fatalf("delivered %v", fs.got())
	}
	if sp.Disp.BacklogFiles() != 0 {
		t.Fatal("backlog must be empty")
	}
	cancel()
	<-done
}

func TestRunner_GivesUpAfterMaxFileAttempts(t *testing.T) {
	root := t.TempDir()
	sp, err := spool.Open(root, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()

	fs := &fakeSink{failFn: func(string, int) error { return errors.New("down forever") }}
	proc := NewProcessor(Options{
		Sink: fs, FailedDir: sp.Dirs.Failed, Logger: quiet,
		Retry: RetryPolicy{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
	})
	r := &Runner{Spool: sp, Proc: proc, Workers: 1, MaxFileAttempts: 3, RetryBase: time.Millisecond, RetryMax: 2 * time.Millisecond, Logger: quiet}

	path := filepath.Join(sp.Dirs.Processing, "y.list")
	_ = os.WriteFile(path, []byte("{\"i\":1}\n"), 0o644)
	sp.Disp.Add(path, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	dst := filepath.Join(sp.Dirs.Failed, "y.list")
	deadline := time.Now().Add(3 * time.Second)
	for !exists(dst) {
		if time.Now().After(deadline) {
			t.Fatal("file was never quarantined")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if exists(path) {
		t.Fatal("file must have moved out of processing/")
	}
}

// Regression C3: one oversized line used to abort the whole file (and, when the
// rename failed, truncate it to zero bytes). Its leading bytes are kept in the
// dead letter instead of a 4 KiB preview.
func TestLineReader_OversizeLineQuarantined(t *testing.T) {
	e := newEnv(t, nil) // MaxLineSize = 1024
	huge := `{"big":"` + strings.Repeat("x", 5000) + `"}`
	p := e.writeFile(t, "e.list", "{\"i\":1}\n"+huge+"\n{\"i\":3}\n")
	if err := e.proc.ProcessFile(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if got := e.sink.got(); len(got) != 2 || got[1] != `{"i":3}` {
		t.Fatalf("neighbours of the oversized line must be delivered, got %v", got)
	}
	bad, _ := os.ReadFile(filepath.Join(e.failed, "e.list.bad"))
	rec := strings.SplitN(strings.TrimSuffix(string(bad), "\n"), "\t", 3)
	if len(rec) != 3 || rec[0] != "2" || !strings.Contains(rec[1], "maximum line size") {
		t.Fatalf("dead letter = %q", bad)
	}
	if rec[2] != huge[:1024] {
		t.Fatalf("the first MaxLineSize bytes must be kept, got %d bytes", len(rec[2]))
	}
}

// A line larger than the server's limit is still a message that was accepted:
// lowering MAX_MESSAGE_SIZE must not truncate what is already on disk.
func TestProcessor_LinesAboveMessageSizeAreStillDelivered(t *testing.T) {
	e := newEnv(t, func(o *Options) { o.MaxLineSize = 0 }) // default: 16 MiB
	long := `{"big":"` + strings.Repeat("x", 3000) + `"}`
	p := e.writeFile(t, "l.list", long+"\n")
	if err := e.proc.ProcessFile(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if got := e.sink.got(); len(got) != 1 || got[0] != long {
		t.Fatalf("delivered %d messages", len(got))
	}
}

// A dead letter that could not be written used to be dropped: the warning was
// logged, the file deleted and the rejected line gone for good.
func TestProcessor_UnwritableDeadLetterKeepsFile(t *testing.T) {
	e := newEnv(t, nil)
	// A regular file where failed/ should be: no .bad can be created inside it.
	blocked := filepath.Join(e.dir, "blocked")
	if err := os.WriteFile(blocked, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.proc = NewProcessor(Options{Sink: e.sink, FailedDir: blocked, MaxLineSize: 1024, CheckpointInterval: 2, Logger: quiet})
	p := e.writeFile(t, "k.list", "{\"i\":1}\nnot json\n{\"i\":3}\n")

	err := e.proc.ProcessFile(context.Background(), p)
	if !errors.Is(err, ErrRetryLater) {
		t.Fatalf("want ErrRetryLater, got %v", err)
	}
	if !exists(p) {
		t.Fatal("the file must be kept while its dead letter is not on disk")
	}
	ck, v, _ := OpenCheckpoint(p + spool.CheckpointExt)
	ck.Close()
	if v >= 2 {
		t.Fatalf("checkpoint must stay before the rejected line, got %d", v)
	}

	// failed/ becomes writable: the line is dead-lettered and the file completes.
	e.proc = NewProcessor(Options{Sink: e.sink, FailedDir: e.failed, MaxLineSize: 1024, CheckpointInterval: 2, Logger: quiet})
	if err := e.proc.ProcessFile(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	bad, err := os.ReadFile(filepath.Join(e.failed, "k.list.bad"))
	if err != nil || !strings.HasPrefix(string(bad), "2\tinvalid JSON\tnot json\n") {
		t.Fatalf("dead letter = %q (%v)", bad, err)
	}
	if exists(p) {
		t.Fatal("completed file must be deleted")
	}
	seen := map[string]int{}
	for _, l := range e.sink.got() {
		seen[l]++
	}
	if seen[`{"i":1}`] == 0 || seen[`{"i":3}`] == 0 {
		t.Fatalf("valid neighbours must be delivered, got %v", e.sink.got())
	}
}
