package wal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeFile records what was written/synced and can fail on demand.
type fakeFile struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	syncedLen int
	syncErr   error
	writeErr  error
	closed    bool
}

func (f *fakeFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.buf.Write(p)
}

func (f *fakeFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.syncErr != nil {
		return f.syncErr
	}
	f.syncedLen = f.buf.Len()
	return nil
}

func (f *fakeFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeFile) setSyncErr(err error) {
	f.mu.Lock()
	f.syncErr = err
	f.mu.Unlock()
}

func (f *fakeFile) contents() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf.String()
}

// fakeFS hands out fakeFiles and can refuse to create them.
type fakeFS struct {
	mu      sync.Mutex
	files   []*fakeFile
	openErr error
}

func (fs *fakeFS) open(path string) (File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.openErr != nil {
		return nil, fs.openErr
	}
	f := &fakeFile{}
	fs.files = append(fs.files, f)
	return f, nil
}

func (fs *fakeFS) setOpenErr(err error) {
	fs.mu.Lock()
	fs.openErr = err
	fs.mu.Unlock()
}

func (fs *fakeFS) all() []*fakeFile {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return append([]*fakeFile(nil), fs.files...)
}

type recorder struct {
	mu    sync.Mutex
	paths []string
}

func (r *recorder) Ready(path string) {
	r.mu.Lock()
	r.paths = append(r.paths, path)
	r.mu.Unlock()
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.paths)
}

// waitCount waits for n handoffs: they happen on their own goroutine, shortly
// after the rotation.
func waitCount(t *testing.T, rec *recorder, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for rec.count() < n {
		if time.Now().After(deadline) {
			t.Fatalf("expected %d handoffs, got %d", n, rec.count())
		}
		time.Sleep(time.Millisecond)
	}
}

func newTestWriter(t *testing.T, fs *fakeFS, mutate func(*Config)) (*Writer, *recorder) {
	t.Helper()
	rec := &recorder{}
	cfg := Config{
		Dir:              t.TempDir(),
		Open:             fs.open,
		Handoff:          rec,
		Logger:           quiet,
		RotationInterval: time.Hour,
		RetryMin:         10 * time.Millisecond,
		RetryMax:         20 * time.Millisecond,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	w, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = w.Close(ctx)
	})
	return w, rec
}

func TestWriter_AppendIsDurableBeforeAck(t *testing.T) {
	fs := &fakeFS{}
	w, _ := newTestWriter(t, fs, nil)

	if err := w.Append(context.Background(), []byte(`{"id":1}`)); err != nil {
		t.Fatal(err)
	}
	f := fs.all()[0]
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.syncedLen == 0 || f.syncedLen != f.buf.Len() {
		t.Fatalf("acked before sync: synced=%d written=%d", f.syncedLen, f.buf.Len())
	}
	if got := f.buf.String(); got != "{\"id\":1}\n" {
		t.Fatalf("unexpected file contents %q", got)
	}
}

// Regression C4: a failing fsync used to be acknowledged with 200.
func TestWriter_SyncErrorNacksAll(t *testing.T) {
	fs := &fakeFS{}
	w, _ := newTestWriter(t, fs, nil)
	fs.all()[0].setSyncErr(errors.New("disk on fire"))

	const n = 20
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = w.Append(context.Background(), []byte(`{"x":1}`))
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err == nil {
			t.Fatalf("message %d was acknowledged although fsync failed", i)
		}
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("message %d: want ErrUnavailable, got %v", i, err)
		}
	}
	if w.Healthy() {
		t.Fatal("writer must be degraded after a commit failure")
	}
}

func TestWriter_WriteErrorNacks(t *testing.T) {
	fs := &fakeFS{}
	w, _ := newTestWriter(t, fs, nil)
	f := fs.all()[0]
	f.mu.Lock()
	f.writeErr = errors.New("no space left on device")
	f.mu.Unlock()

	if err := w.Append(context.Background(), []byte(`{"x":1}`)); err == nil {
		t.Fatal("write failure must not be acknowledged")
	}
}

// Regression A2: failing to create the next file used to call log.Fatalf.
func TestWriter_OpenFailureDegradesAndRecovers(t *testing.T) {
	fs := &fakeFS{}
	w, rec := newTestWriter(t, fs, func(c *Config) { c.MaxFileSize = 10 })

	fs.setOpenErr(errors.New("permission denied"))
	// Fills the file, so the rotation that follows cannot open a new one.
	if err := w.Append(context.Background(), []byte(`{"a":"0123456789"}`)); err != nil {
		t.Fatalf("first append must succeed (it was committed): %v", err)
	}
	waitCount(t, rec, 1) // the rotated file must be handed over

	// The ack is sent before the rotation runs, so wait for the degraded state.
	deadline := time.Now().Add(2 * time.Second)
	for w.Healthy() {
		if time.Now().After(deadline) {
			t.Fatal("writer should be degraded")
		}
		time.Sleep(time.Millisecond)
	}
	err := w.Append(context.Background(), []byte(`{"b":1}`))
	if err == nil {
		t.Fatal("append succeeded while degraded")
	}
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("want ErrUnavailable, got %v", err)
	}

	fs.setOpenErr(nil) // the disk comes back
	deadline = time.Now().Add(2 * time.Second)
	for !w.Healthy() {
		if time.Now().After(deadline) {
			t.Fatal("writer never recovered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := w.Append(context.Background(), []byte(`{"c":1}`)); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
}

func TestWriter_RecoversAfterCommitFailure(t *testing.T) {
	fs := &fakeFS{}
	w, rec := newTestWriter(t, fs, nil)
	if err := w.Append(context.Background(), []byte(`{"ok":1}`)); err != nil {
		t.Fatal(err)
	}
	fs.all()[0].setSyncErr(errors.New("EIO"))
	if err := w.Append(context.Background(), []byte(`{"bad":1}`)); err == nil {
		t.Fatal("expected failure")
	}

	deadline := time.Now().Add(2 * time.Second)
	for !w.Healthy() {
		if time.Now().After(deadline) {
			t.Fatal("never recovered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := w.Append(context.Background(), []byte(`{"after":1}`)); err != nil {
		t.Fatal(err)
	}
	waitCount(t, rec, 1) // the file holding the synced message must be handed over
	if len(fs.all()) != 2 {
		t.Fatalf("expected a fresh file after recovery, have %d", len(fs.all()))
	}
}

func TestWriter_GroupCommitBatchesConcurrentAppends(t *testing.T) {
	fs := &fakeFS{}
	w, _ := newTestWriter(t, fs, nil)

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Append(context.Background(), []byte(`{"n":1}`)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	got := fs.all()[0].contents()
	if lines := strings.Count(got, "\n"); lines != n {
		t.Fatalf("want %d lines, got %d", n, lines)
	}
}

func TestWriter_RotatesBySize(t *testing.T) {
	fs := &fakeFS{}
	w, rec := newTestWriter(t, fs, func(c *Config) { c.MaxFileSize = 64 })
	for i := 0; i < 10; i++ {
		if err := w.Append(context.Background(), []byte(`{"k":"0123456789012345"}`)); err != nil {
			t.Fatal(err)
		}
	}
	waitCount(t, rec, 2) // several rotations
}

func TestWriter_RotatesByTimeOnlyWhenNotEmpty(t *testing.T) {
	fs := &fakeFS{}
	w, rec := newTestWriter(t, fs, func(c *Config) { c.RotationInterval = 30 * time.Millisecond })

	time.Sleep(120 * time.Millisecond)
	if rec.count() != 0 || len(fs.all()) != 1 {
		t.Fatalf("an empty file must not rotate (handoffs=%d files=%d)", rec.count(), len(fs.all()))
	}
	if err := w.Append(context.Background(), []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for rec.count() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("non-empty file never rotated by time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWriter_RejectsNewlines(t *testing.T) {
	fs := &fakeFS{}
	w, _ := newTestWriter(t, fs, nil)
	if err := w.Append(context.Background(), []byte("{\n}")); !errors.Is(err, ErrNewline) {
		t.Fatalf("want ErrNewline, got %v", err)
	}
}

func TestWriter_AdmissionLimits(t *testing.T) {
	fs := &fakeFS{}
	w, _ := newTestWriter(t, fs, func(c *Config) { c.MaxInflightBytes = 10 })
	err := w.Append(context.Background(), []byte(`{"too":"large for the budget"}`))
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("want ErrOverloaded, got %v", err)
	}
	// The budget must have been given back.
	if err := w.Append(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("small append after rejection: %v", err)
	}
}

func TestWriter_CloseDrainsAndHandsOver(t *testing.T) {
	fs := &fakeFS{}
	w, rec := newTestWriter(t, fs, nil)

	var acked atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if w.Append(context.Background(), []byte(`{"n":1}`)) == nil {
				acked.Add(1)
			}
		}()
	}
	wg.Wait()
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec.count() != 1 {
		t.Fatalf("final file must be handed over, got %d", rec.count())
	}
	if err := w.Append(context.Background(), []byte(`{}`)); !errors.Is(err, ErrClosed) {
		t.Fatalf("append after close: want ErrClosed, got %v", err)
	}
	if got := strings.Count(fs.all()[0].contents(), "\n"); int64(got) != acked.Load() {
		t.Fatalf("acked %d messages but %d are on disk", acked.Load(), got)
	}
}

// The handoff (rename + directory fsyncs) runs off the writer goroutine so it
// never delays the next group commit, yet keeps rotation order and completes
// before Close returns.
func TestWriter_HandoffKeepsOrderAndFinishesBeforeClose(t *testing.T) {
	fs := &fakeFS{}
	w, rec := newTestWriter(t, fs, func(c *Config) { c.MaxFileSize = 16 })
	const n = 40
	for i := 0; i < n; i++ {
		if err := w.Append(context.Background(), []byte(`{"k":"0123456789012345678901"}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Every append fills a file past MaxFileSize: n rotations, all handed over once Close returns.
	if rec.count() != n {
		t.Fatalf("handoffs=%d, want %d", rec.count(), n)
	}
	if !sort.StringsAreSorted(rec.paths) {
		t.Fatalf("files must be handed over in creation order: %v", rec.paths)
	}
}

func TestWriter_EmptyFileIsRemovedOnClose(t *testing.T) {
	dir := t.TempDir()
	rec := &recorder{}
	w, err := New(Config{Dir: dir, Handoff: rec, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 || rec.count() != 0 {
		t.Fatalf("empty file left behind: %v (handoffs=%d)", entries, rec.count())
	}
}

func TestWriter_AckTimeoutDoesNotBlock(t *testing.T) {
	fs := &fakeFS{}
	w, _ := newTestWriter(t, fs, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := w.Append(ctx, []byte(`{"a":1}`))
	// Either the commit won the race or the context did; both are valid, but
	// a timeout must be reported as ErrAckTimeout.
	if err != nil && !errors.Is(err, ErrAckTimeout) {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestWriter_RealFilesRoundTrip(t *testing.T) {
	dir := t.TempDir()
	rec := &recorder{}
	w, err := New(Config{Dir: dir, Handoff: rec, Logger: quiet, MaxFileSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err := w.Append(context.Background(), []byte(`{"i":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec.count() != 1 {
		t.Fatalf("handoffs=%d", rec.count())
	}
	data, err := os.ReadFile(rec.paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(data), "\n") != 100 || filepath.Ext(rec.paths[0]) != ".list" {
		t.Fatalf("bad file %s: %d lines", rec.paths[0], strings.Count(string(data), "\n"))
	}
}

func BenchmarkAppend(b *testing.B) {
	dir := b.TempDir()
	w, err := New(Config{Dir: dir, Handoff: &recorder{}, Logger: quiet, MaxFileSize: 8 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close(context.Background())
	payload := []byte(`{"event":"payment_received","amount":100,"customer_id":"cust_12345"}`)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.SetParallelism(16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := w.Append(context.Background(), payload); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
