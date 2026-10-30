// Command loadgen sends JSON webhooks to an ingestion endpoint and reports
// throughput and latency percentiles. It is used for before/after benchmarks
// and for end-to-end crash tests.
package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		url     = flag.String("url", "http://127.0.0.1:8080/webhook", "target endpoint")
		total   = flag.Int("n", 10000, "total requests")
		conc    = flag.Int("c", 64, "concurrent workers")
		size    = flag.Int("size", 200, "approximate payload size in bytes")
		secret  = flag.String("secret", "", "HMAC-SHA256 secret (optional)")
		header  = flag.String("header", "X-Signature-256", "signature header name")
		idsFile = flag.String("ids", "", "write the id of every request acknowledged with 200 to this file")
		prefix  = flag.String("prefix", "m", "message id prefix")
	)
	flag.Parse()

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *conc,
			MaxIdleConnsPerHost: *conc,
		},
	}

	pad := ""
	if *size > 60 {
		pad = strings.Repeat("x", *size-60)
	}

	var (
		next      atomic.Int64
		mu        sync.Mutex
		latencies = make([]time.Duration, 0, *total)
		codes     = map[int]int{}
		acked     []string
		netErrors atomic.Int64
		wg        sync.WaitGroup
	)

	start := time.Now()
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= *total {
					return
				}
				id := fmt.Sprintf("%s-%d", *prefix, i)
				body := []byte(fmt.Sprintf(`{"id":%q,"pad":%q}`, id, pad))
				req, _ := http.NewRequest(http.MethodPost, *url, bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				if *secret != "" {
					mac := hmac.New(sha256.New, []byte(*secret))
					mac.Write(body)
					req.Header.Set(*header, "sha256="+hex.EncodeToString(mac.Sum(nil)))
				}
				t0 := time.Now()
				resp, err := client.Do(req)
				d := time.Since(t0)
				if err != nil {
					netErrors.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				mu.Lock()
				latencies = append(latencies, d)
				codes[resp.StatusCode]++
				if resp.StatusCode == http.StatusOK && *idsFile != "" {
					acked = append(acked, id)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(float64(len(latencies)-1)*p)]
	}
	fmt.Printf("requests=%d concurrency=%d size=%dB elapsed=%s\n", *total, *conc, *size, elapsed.Round(time.Millisecond))
	fmt.Printf("throughput=%.0f req/s net_errors=%d\n", float64(len(latencies))/elapsed.Seconds(), netErrors.Load())
	fmt.Printf("latency p50=%s p90=%s p99=%s max=%s\n", pct(0.50), pct(0.90), pct(0.99), pct(1))
	fmt.Printf("status codes: %v\n", codes)

	if *idsFile != "" {
		if err := os.WriteFile(*idsFile, []byte(strings.Join(acked, "\n")+"\n"), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "write ids:", err)
			os.Exit(1)
		}
	}
}
