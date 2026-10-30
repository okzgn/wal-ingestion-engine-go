package spool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func open(t *testing.T) *Spool {
	t.Helper()
	sp, err := Open(t.TempDir(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sp.Close() })
	return sp
}

func write(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func drain(d *Dispatcher) []string {
	var out []string
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		p, ok := d.Next(ctx)
		cancel()
		if !ok {
			return out
		}
		out = append(out, filepath.Base(p))
	}
}

func TestOpen_InstanceLock(t *testing.T) {
	root := t.TempDir()
	first, err := Open(root, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root, quiet); !errors.Is(err, ErrLocked) {
		t.Fatalf("second instance: want ErrLocked, got %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(root, quiet)
	if err != nil {
		t.Fatalf("lock must be released on Close: %v", err)
	}
	second.Close()
}

// Regression A1: processPendingFiles and recoverProcessingFiles raced and could
// queue the same file twice, so two workers processed it at once.
func TestSpool_RecoverNoDuplicates(t *testing.T) {
	sp := open(t)
	// Files in both directories, plus noise that must be ignored.
	write(t, sp.Dirs.Processing, NewName(time.Unix(100, 0), 1, 1), "a\n")
	write(t, sp.Dirs.Processing, NewName(time.Unix(200, 0), 1, 1), "b\n")
	write(t, sp.Dirs.Ingestions, NewName(time.Unix(300, 0), 1, 1), "c\n")
	write(t, sp.Dirs.Ingestions, NewName(time.Unix(400, 0), 1, 1), "")         // empty: deleted
	write(t, sp.Dirs.Ingestions, "notes.txt", "ignored")                       // not a data file
	write(t, sp.Dirs.Processing, NewName(time.Unix(500, 0), 1, 1)+".ckpt", "") // orphan checkpoint

	if err := sp.Recover(); err != nil {
		t.Fatal(err)
	}
	// Recovering twice (or a concurrent Ready of the same file) must not duplicate.
	if err := sp.Recover(); err != nil {
		t.Fatal(err)
	}
	got := drain(sp.Disp)
	if len(got) != 3 {
		t.Fatalf("want 3 files queued exactly once, got %v", got)
	}
	if !sort.StringsAreSorted(got) {
		t.Fatalf("files must be queued in chronological order, got %v", got)
	}
	if entries, _ := os.ReadDir(sp.Dirs.Ingestions); len(entries) != 1 || entries[0].Name() != "notes.txt" {
		t.Fatalf("ingestions/ should only keep the unrelated file, has %v", entries)
	}
}

func TestSpool_RecoverRaceWithReady(t *testing.T) {
	sp := open(t)
	var names []string
	for i := 0; i < 20; i++ {
		n := NewName(time.Unix(int64(100+i), 0), 1, 1)
		names = append(names, n)
		write(t, sp.Dirs.Processing, n, "x\n")
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = sp.Recover() }()
	}
	wg.Wait()
	got := drain(sp.Disp)
	if len(got) != len(names) {
		t.Fatalf("queued %d, want %d (duplicates or losses): %v", len(got), len(names), got)
	}
}

func TestSpool_RecoveryOrder(t *testing.T) {
	sp := open(t)
	// A later pid with an earlier timestamp must still sort first.
	late := NewName(time.Unix(1000, 0), 1, 1)
	early := NewName(time.Unix(10, 0), 1, 99999)
	write(t, sp.Dirs.Processing, late, "x\n")
	write(t, sp.Dirs.Processing, early, "x\n")
	if err := sp.Recover(); err != nil {
		t.Fatal(err)
	}
	got := drain(sp.Disp)
	if len(got) != 2 || got[0] != early {
		t.Fatalf("order %v, want %s first", got, early)
	}
}

func TestRecover_OrphanCheckpoint(t *testing.T) {
	sp := open(t)
	data := NewName(time.Unix(1, 0), 1, 1)
	write(t, sp.Dirs.Processing, data, "x\n")
	write(t, sp.Dirs.Processing, data+CheckpointExt, "keep: its data file exists")
	write(t, sp.Dirs.Processing, "gone.list.ckpt", "orphan")
	write(t, sp.Dirs.Processing, "gone2.list.done", "legacy orphan")
	// Legacy marker: the old engine finished this file but crashed before cleanup.
	write(t, sp.Dirs.Processing, "old.list", "x\n")
	write(t, sp.Dirs.Processing, "old.list.delete", "")

	if err := sp.Recover(); err != nil {
		t.Fatal(err)
	}
	for _, gone := range []string{"gone.list.ckpt", "gone2.list.done", "old.list", "old.list.delete"} {
		if _, err := os.Stat(filepath.Join(sp.Dirs.Processing, gone)); !os.IsNotExist(err) {
			t.Errorf("%s should have been swept", gone)
		}
	}
	if _, err := os.Stat(filepath.Join(sp.Dirs.Processing, data+CheckpointExt)); err != nil {
		t.Errorf("a checkpoint with data must be kept: %v", err)
	}
}

func TestSpool_ReadyMovesAndQueues(t *testing.T) {
	sp := open(t)
	p := write(t, sp.Dirs.Ingestions, NewName(time.Unix(5, 0), 1, 1), "hello\n")
	sp.Ready(p)
	sp.Ready(p) // the file is gone already; must be harmless
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatal("file must leave ingestions/")
	}
	got := drain(sp.Disp)
	if len(got) != 1 {
		t.Fatalf("queued %v", got)
	}
	if sp.Disp.BacklogBytes() != 6 || sp.Disp.BacklogFiles() != 1 {
		t.Fatalf("backlog bytes=%d files=%d", sp.Disp.BacklogBytes(), sp.Disp.BacklogFiles())
	}
}

func TestSpool_Quarantine(t *testing.T) {
	sp := open(t)
	name := NewName(time.Unix(5, 0), 1, 1)
	p := write(t, sp.Dirs.Processing, name, "x\n")
	write(t, sp.Dirs.Processing, name+CheckpointExt, "ck")
	sp.Disp.Add(p, 2)

	if err := sp.Quarantine(p); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{name, name + CheckpointExt} {
		if _, err := os.Stat(filepath.Join(sp.Dirs.Failed, f)); err != nil {
			t.Errorf("%s not in failed/: %v", f, err)
		}
	}
	if sp.Disp.BacklogFiles() != 0 || sp.Disp.BacklogBytes() != 0 {
		t.Fatal("quarantined file must leave the backlog")
	}
}

func TestNewName_SortsChronologicallyAcrossPids(t *testing.T) {
	var names []string
	for i, pid := range []int{900000, 5, 77} {
		names = append(names, NewName(time.Unix(int64(1000+i), 0), 1, pid))
	}
	if !sort.StringsAreSorted(names) {
		t.Fatalf("not chronological: %v", names)
	}
	if !strings.HasSuffix(names[0], FileExt) {
		t.Fatal("wrong extension")
	}
}

// ---- dispatcher ----

func TestDispatcher_RejectsDuplicatesUntilDone(t *testing.T) {
	d := NewDispatcher()
	if !d.Add("a", 10) || d.Add("a", 10) {
		t.Fatal("duplicate Add must be rejected")
	}
	ctx := context.Background()
	p, _ := d.Next(ctx)
	if d.Add(p, 10) {
		t.Fatal("an in-flight file must not be re-added")
	}
	d.Done(p)
	if !d.Add(p, 10) {
		t.Fatal("after Done the path may be added again")
	}
}

func TestDispatcher_NextHonoursContext(t *testing.T) {
	d := NewDispatcher()
	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan bool)
	go func() { _, ok := d.Next(ctx); res <- ok }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case ok := <-res:
		if ok {
			t.Fatal("Next must report false once cancelled")
		}
	case <-time.After(time.Second):
		t.Fatal("Next did not wake up on cancellation")
	}
}

func TestDispatcher_RequeueKeepsBacklogAndDelays(t *testing.T) {
	d := NewDispatcher()
	d.Add("a", 7)
	p, _ := d.Next(context.Background())
	d.Requeue(p, 30*time.Millisecond)
	if d.BacklogBytes() != 7 {
		t.Fatal("a delayed file still counts as backlog")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	if _, ok := d.Next(ctx); ok {
		t.Fatal("file came back before its delay")
	}
	cancel()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if got, ok := d.Next(ctx); !ok || got != "a" {
		t.Fatalf("requeued file never returned: %q %v", got, ok)
	}
}

func TestDispatcher_ConcurrentWorkersGetEachFileOnce(t *testing.T) {
	d := NewDispatcher()
	const n = 200
	for i := 0; i < n; i++ {
		d.Add(fmt.Sprintf("f%03d", i), 1)
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
				p, ok := d.Next(ctx)
				cancel()
				if !ok {
					return
				}
				mu.Lock()
				seen[p]++
				mu.Unlock()
				d.Done(p)
			}
		}()
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("processed %d of %d files", len(seen), n)
	}
	for p, c := range seen {
		if c != 1 {
			t.Fatalf("%s handed out %d times", p, c)
		}
	}
	if d.BacklogFiles() != 0 {
		t.Fatalf("backlog not empty: %d", d.BacklogFiles())
	}
}
