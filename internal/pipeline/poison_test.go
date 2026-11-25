package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/okzgn/wal-ingestion-engine-go/internal/spool"
)

// A message the destination keeps failing on with a transient error (a 500,
// a timeout) used to stall its file for ever: every round stopped at the same
// line, and what lay behind it never went out. With MaxLineAttempts it is
// dead-lettered, but only on fresh evidence that the destination is up.
func TestProcessor_PoisonLineIsDeadLetteredWhenSinkIsAlive(t *testing.T) {
	for _, conc := range []int{1, 4} {
		t.Run(fmt.Sprintf("concurrency=%d", conc), func(t *testing.T) {
			s := &slowSink{failFn: func(l string) error {
				if l == msg(5) {
					return errors.New("500 internal server error")
				}
				return nil
			}}
			e := concurrentEnv(t, s, conc, func(o *Options) { o.MaxLineAttempts = 3 })
			p := e.writeFile(t, "poison.list", numbered(20))

			rounds := 0
			for {
				err := e.proc.ProcessFile(context.Background(), p)
				rounds++
				if err == nil {
					break
				}
				var se *StallError
				if !errors.As(err, &se) || se.Line != 5 || !errors.Is(err, ErrRetryLater) {
					t.Fatalf("round %d: want a stall on line 5, got %v", rounds, err)
				}
				if rounds > 10 {
					t.Fatal("the file never got past the poison message")
				}
			}
			if rounds != 4 {
				t.Fatalf("gave up after %d rounds, want 4 (3 stalls, then the failure with evidence)", rounds)
			}
			c := s.counts()
			for i := 1; i <= 20; i++ {
				want := 1
				if i == 5 {
					want = 0
				}
				if c[msg(i)] != want {
					t.Fatalf("%s delivered %d times, want %d", msg(i), c[msg(i)], want)
				}
			}
			bad, err := os.ReadFile(filepath.Join(e.failed, "poison.list.bad"))
			if err != nil {
				t.Fatal(err)
			}
			rec := strings.SplitN(strings.TrimSuffix(string(bad), "\n"), "\t", 3)
			if len(rec) != 3 || rec[0] != "5" || !strings.Contains(rec[1], "gave up") || rec[2] != msg(5) {
				t.Fatalf("dead letter = %q", bad)
			}
			if exists(p) {
				t.Fatal("the file must complete once the poison message is out of the way")
			}
		})
	}
}

// An outage must never dead-letter anything, however long it lasts: with the
// destination down there is no evidence that the message is the problem.
func TestProcessor_NeverGivesUpWhileSinkIsDown(t *testing.T) {
	for _, conc := range []int{1, 4} {
		t.Run(fmt.Sprintf("concurrency=%d", conc), func(t *testing.T) {
			var mu sync.Mutex
			down := true
			s := &slowSink{failFn: func(string) error {
				mu.Lock()
				defer mu.Unlock()
				if down {
					return errors.New("connection refused")
				}
				return nil
			}}
			e := concurrentEnv(t, s, conc, func(o *Options) { o.MaxLineAttempts = 2 })
			p := e.writeFile(t, "outage.list", numbered(10))

			for round := 1; round <= 8; round++ {
				if err := e.proc.ProcessFile(context.Background(), p); !errors.Is(err, ErrRetryLater) {
					t.Fatalf("round %d: want ErrRetryLater, got %v", round, err)
				}
			}
			if entries, _ := os.ReadDir(e.failed); len(entries) != 0 {
				t.Fatalf("an outage dead-lettered something: %v", entries)
			}
			if len(s.got()) != 0 {
				t.Fatalf("nothing can have been delivered, got %v", s.got())
			}

			mu.Lock()
			down = false
			mu.Unlock()
			if err := e.proc.ProcessFile(context.Background(), p); err != nil {
				t.Fatal(err)
			}
			c := s.counts()
			for i := 1; i <= 10; i++ {
				if c[msg(i)] != 1 {
					t.Fatalf("%s delivered %d times after the outage, want 1", msg(i), c[msg(i)])
				}
			}
			if entries, _ := os.ReadDir(e.failed); len(entries) != 0 {
				t.Fatalf("failed/ must stay empty, has %v", entries)
			}
		})
	}
}

// 0 keeps the old behaviour: never give up.
func TestProcessor_MaxLineAttemptsZeroNeverGivesUp(t *testing.T) {
	s := &slowSink{failFn: func(l string) error {
		if l == msg(2) {
			return errors.New("500")
		}
		return nil
	}}
	e := concurrentEnv(t, s, 4, nil) // MaxLineAttempts = 0
	p := e.writeFile(t, "never.list", numbered(6))
	for round := 1; round <= 6; round++ {
		if err := e.proc.ProcessFile(context.Background(), p); !errors.Is(err, ErrRetryLater) {
			t.Fatalf("round %d: want ErrRetryLater, got %v", round, err)
		}
	}
	if exists(filepath.Join(e.failed, "never.list.bad")) {
		t.Fatal("nothing may be dead-lettered with MaxLineAttempts 0")
	}
}

// MAX_FILE_ATTEMPTS used to count every postponement, so a file that advanced
// a little between failures during a long, flapping outage was quarantined
// with messages that had never been refused. It now counts consecutive
// failures on the same line only.
func TestRunner_FileAttemptsResetOnProgress(t *testing.T) {
	root := t.TempDir()
	sp, err := spool.Open(root, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()

	// Every message fails twice, then goes through: 2 postponements per line,
	// 10 in total, never 3 in a row on the same line.
	tries := map[string]int{}
	fs := &fakeSink{}
	fs.failFn = func(line string, _ int) error {
		tries[line]++ // fakeSink holds its lock while calling failFn
		if tries[line] <= 2 {
			return errors.New("flapping")
		}
		return nil
	}
	proc := NewProcessor(Options{
		Sink: fs, FailedDir: sp.Dirs.Failed, Logger: quiet,
		Retry: RetryPolicy{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
	})
	r := &Runner{Spool: sp, Proc: proc, Workers: 1, MaxFileAttempts: 3, RetryBase: time.Millisecond, RetryMax: 2 * time.Millisecond, Logger: quiet}

	path := filepath.Join(sp.Dirs.Processing, "flap.list")
	if err := os.WriteFile(path, []byte(numbered(5)), 0o644); err != nil {
		t.Fatal(err)
	}
	sp.Disp.Add(path, 40)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	deadline := time.Now().Add(20 * time.Second)
	for exists(path) {
		if time.Now().After(deadline) {
			t.Fatal("file never completed")
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done

	if exists(filepath.Join(sp.Dirs.Failed, "flap.list")) {
		t.Fatal("a file that keeps advancing must not be quarantined")
	}
	if got := fs.got(); len(got) != 5 {
		t.Fatalf("delivered %v, want all 5", got)
	}
}
