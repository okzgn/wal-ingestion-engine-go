package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/okzgn/wal-ingestion-engine-go/internal/spool"
	"github.com/okzgn/wal-ingestion-engine-go/internal/wal"
)

// disk models what a power loss leaves behind: for every WAL file, how many
// bytes had been written and how many of those an fsync had made durable.
type disk struct {
	mu    sync.Mutex
	files map[string]*extent
}

type extent struct{ written, synced int64 }

type trackedFile struct {
	*os.File
	d    *disk
	name string
}

func (f *trackedFile) Write(p []byte) (int, error) {
	n, err := f.File.Write(p)
	f.d.mu.Lock()
	f.d.files[f.name].written += int64(n)
	f.d.mu.Unlock()
	return n, err
}

func (f *trackedFile) Sync() error {
	err := f.File.Sync()
	if err == nil {
		f.d.mu.Lock()
		e := f.d.files[f.name]
		e.synced = e.written
		f.d.mu.Unlock()
	}
	return err
}

func (d *disk) open(path string) (wal.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	name := filepath.Base(path)
	d.mu.Lock()
	d.files[name] = &extent{}
	d.mu.Unlock()
	return &trackedFile{File: f, d: d, name: name}, nil
}

func (d *disk) snapshot() map[string]extent {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]extent, len(d.files))
	for name, e := range d.files {
		out[name] = *e
	}
	return out
}

// The end-to-end crash test kills the process, which the page cache survives:
// it cannot tell whether an acknowledgement really waited for the fsync. This
// one can. It runs the real writer under load, picks an instant, and rebuilds
// the disk as a power loss at that instant would leave it: every file cut back
// to what had been fsynced, plus a torn piece of what had not, and nothing of
// the files created later. Every message acknowledged before that instant must
// then come out of recovery, and nothing torn may be delivered.
func TestPowerLoss_AcknowledgedMessagesSurvive(t *testing.T) {
	root := t.TempDir()
	sp, err := spool.Open(root, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.Recover(); err != nil {
		t.Fatal(err)
	}
	d := &disk{files: map[string]*extent{}}
	w, err := wal.New(wal.Config{
		Dir: sp.Dirs.Ingestions, MaxFileSize: 4 << 10, RotationInterval: 20 * time.Millisecond,
		Handoff: sp, Open: d.open, Logger: quiet,
	})
	if err != nil {
		t.Fatal(err)
	}

	var (
		mu    sync.Mutex
		acked []string
		stop  atomic.Bool
		wg    sync.WaitGroup
	)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				id := fmt.Sprintf("g%d-%d", g, i)
				if w.Append(context.Background(), []byte(fmt.Sprintf(`{"id":%q}`, id))) == nil {
					mu.Lock()
					acked = append(acked, id)
					mu.Unlock()
				}
			}
		}()
	}

	// The instant of the power loss. Order matters: the acknowledgements are
	// read first, so each of them precedes the disk state read after it.
	time.Sleep(150 * time.Millisecond)
	mu.Lock()
	before := append([]string(nil), acked...)
	mu.Unlock()
	atLoss := d.snapshot()

	time.Sleep(100 * time.Millisecond) // the engine carries on: none of this survives
	stop.Store(true)
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sp.Close(); err != nil {
		t.Fatal(err)
	}
	if len(before) < 100 {
		t.Fatalf("only %d messages acknowledged before the loss: the test proves nothing", len(before))
	}

	// Rebuild the disk as it was at the loss.
	var survivors []string
	for _, dir := range []string{sp.Dirs.Ingestions, sp.Dirs.Processing} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			path := filepath.Join(dir, e.Name())
			ext, existed := atLoss[e.Name()]
			if !existed {
				if err := os.Remove(path); err != nil { // created after the loss
					t.Fatal(err)
				}
				continue
			}
			// What was fsynced is there; of what was not, an arbitrary part.
			if err := os.Truncate(path, ext.synced+(ext.written-ext.synced)/2); err != nil {
				t.Fatal(err)
			}
			survivors = append(survivors, path)
		}
	}
	// And a write that was under way: half a line at the end of the last file.
	sort.Strings(survivors)
	last, err := os.OpenFile(survivors[len(survivors)-1], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := last.WriteString(`{"id":"torn-by-the-power-lo`); err != nil {
		t.Fatal(err)
	}
	last.Close()

	// The next start.
	sp2, err := spool.Open(root, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer sp2.Close()
	if err := sp2.Recover(); err != nil {
		t.Fatal(err)
	}
	s := &slowSink{}
	proc := NewProcessor(Options{Sink: s, FailedDir: sp2.Dirs.Failed, Concurrency: 8, Logger: quiet})
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		path, ok := sp2.Disp.Next(ctx)
		cancel()
		if !ok {
			break
		}
		if err := proc.ProcessFile(context.Background(), path); err != nil {
			t.Fatalf("%s: %v", filepath.Base(path), err)
		}
		sp2.Disp.Done(path)
	}

	delivered := map[string]int{}
	for _, line := range s.got() {
		var m struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(line), &m); err != nil || m.ID == "" {
			t.Fatalf("something that is not a whole message was delivered: %q", line)
		}
		delivered[m.ID]++
	}
	lost := 0
	for _, id := range before {
		if delivered[id] == 0 {
			lost++
		}
	}
	if lost != 0 {
		t.Fatalf("%d of %d messages acknowledged before the power loss are gone", lost, len(before))
	}
	if delivered["torn-by-the-power-lo"] != 0 {
		t.Fatal("the torn write was delivered")
	}
	bad, _ := filepath.Glob(filepath.Join(sp2.Dirs.Failed, "*.bad"))
	torn := false
	for _, p := range bad {
		if b, _ := os.ReadFile(p); strings.Contains(string(b), "torn write") {
			torn = true
		}
	}
	if !torn {
		t.Fatal("the torn tail must be kept as a dead letter, not dropped")
	}
	t.Logf("acknowledged before the loss: %d, files surviving: %d, all recovered", len(before), len(survivors))
}
