/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streamsql

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Stress scenario: emit a large volume concurrently, measure sustained
// throughput, peak heap, and retained heap after GC. Retained heap must stay
// bounded regardless of N (rows are released once processed; aggregation state
// is bounded by low group cardinality + window purge) — a growing retained
// heap signals a leak.
type stressScenario struct {
	name string
	sql  string
}

func TestStress_MemoryAndThroughput(t *testing.T) {
	scenarios := []stressScenario{
		{
			name: "Transform_NoState",
			// Non-aggregation: each row is projected and released, no accumulation.
			sql: "SELECT deviceId, temperature FROM stream WHERE temperature > 0",
		},
		{
			name: "Aggregation_BoundedCardinality",
			// Few keys + count-purged window: state is bounded, not row-proportional.
			sql: "SELECT deviceId, AVG(temperature) AS avg_t FROM stream GROUP BY deviceId, CountingWindow(1000)",
		},
	}

	const (
		totalRows    = 2_000_000
		producers    = 8
		drainTimeout = 10 * time.Second
	)

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			ssql := New(WithHighPerformance())
			defer ssql.Stop()
			if err := ssql.Execute(sc.sql); err != nil {
				t.Fatalf("Execute: %v", err)
			}

			// Drain results so ingest is not result-channel backpressure bound.
			ctxCancel := make(chan struct{})
			var produced int64
			go func() {
				for {
					select {
					case <-ssql.Stream().GetResultsChan():
					case <-ctxCancel:
						return
					}
				}
			}()

			// Baseline heap after a full GC.
			runtime.GC()
			var baseline runtime.MemStats
			runtime.ReadMemStats(&baseline)

			rowsPerProducer := totalRows / producers
			var wg sync.WaitGroup
			wg.Add(producers)
			start := time.Now()
			for p := 0; p < producers; p++ {
				go func(pid int) {
					defer wg.Done()
					for i := 0; i < rowsPerProducer; i++ {
						ssql.Emit(map[string]any{
							"deviceId":    fmt.Sprintf("dev%d", (pid*rowsPerProducer+i)%5), // 5 keys
							"temperature": 25.0,
						})
						atomic.AddInt64(&produced, 1)
					}
				}(p)
			}
			wg.Wait()
			ingestDuration := time.Since(start)

			// Drain input channel, then sample peak heap (includes processing backlog
			// when producers outrun the single processor goroutine).
			deadline := time.Now().Add(drainTimeout)
			for ssql.Stream().GetStats()["data_chan_len"] > 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			var peak runtime.MemStats
			runtime.ReadMemStats(&peak)

			// Let the window/aggregator/result pipeline flush remaining in-flight
			// work before measuring retained heap, so backlog is not mistaken for a leak.
			time.Sleep(500 * time.Millisecond)
			runtime.GC()
			var after runtime.MemStats
			runtime.ReadMemStats(&after)

			close(ctxCancel)

			throughput := float64(produced) / ingestDuration.Seconds()
			peakHeapMB := float64(peak.HeapAlloc) / 1e6
			retainedMB := float64(after.HeapAlloc-baseline.HeapAlloc) / 1e6
			perRowB := float64(peak.TotalAlloc-baseline.TotalAlloc) / float64(produced)

			t.Logf("[%s] rows=%d producers=%d", sc.name, produced, producers)
			t.Logf("  ingest throughput : %.0f rows/sec (%.1f万/s)", throughput, throughput/1e4)
			t.Logf("  peak heap         : %.1f MB", peakHeapMB)
			t.Logf("  retained after GC : %.2f MB (delta vs baseline)", retainedMB)
			t.Logf("  total alloc/row   : %.0f B", perRowB)

			// Leak guard: retained heap must not scale with N. With 5 keys and a
			// 1000-row count window, live state is tiny; allow a generous ceiling
			// for goroutine/channel plumbing and GC slack.
			const retainedCeilingMB = 64.0
			if retainedMB > retainedCeilingMB {
				t.Errorf("retained heap %.1f MB exceeds %.0f MB ceiling — possible leak", retainedMB, retainedCeilingMB)
			}
		})
	}
}
