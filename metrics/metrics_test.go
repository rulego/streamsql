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

package metrics

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCounter(t *testing.T) {
	c := NewCounter("rows")
	c.Inc()
	c.IncBy(5)
	assert.Equal(t, int64(6), c.Value())
	assert.Equal(t, "rows", c.Name())
	c.Reset()
	assert.Equal(t, int64(0), c.Value())
}

func TestGauge(t *testing.T) {
	g := NewGauge("queue_depth")
	g.Set(10)
	g.Inc(5)
	g.Dec(3)
	assert.Equal(t, int64(12), g.Value())
	g.Inc(-2)
	assert.Equal(t, int64(10), g.Value())
}

func TestHistogramObserve(t *testing.T) {
	h := NewHistogram("latency", []time.Duration{time.Millisecond, 10 * time.Millisecond})
	for _, d := range []time.Duration{
		100 * time.Microsecond, // <= 1ms bucket
		500 * time.Microsecond, // <= 1ms bucket
		5 * time.Millisecond,   // <= 10ms bucket
	} {
		h.Observe(d)
	}
	snap := h.Snapshot()
	require.Equal(t, int64(3), snap.Count)
	assert.Equal(t, 100*time.Microsecond, snap.Min)
	assert.Equal(t, 5*time.Millisecond, snap.Max)
	assert.Equal(t, int64(2), snap.Buckets["1ms"], "two obs <= 1ms")
	assert.Equal(t, int64(3), snap.Buckets["10ms"], "all three obs <= 10ms")
}

func TestHistogramAvgAndPercentile(t *testing.T) {
	h := NewHistogram("lat", nil) // default buckets
	h.Observe(1 * time.Millisecond)
	h.Observe(2 * time.Millisecond)
	h.Observe(3 * time.Millisecond)
	snap := h.Snapshot()
	assert.Equal(t, 2*time.Millisecond, snap.Avg)
	// p50 target = 1.5 -> first bucket whose cumulative >= 1.5 is the one holding obs 2&3.
	assert.GreaterOrEqual(t, h.Percentile(0.5), time.Duration(0))
	assert.GreaterOrEqual(t, h.Percentile(0.99), h.Percentile(0.5))
}

func TestHistogramEmptySnapshot(t *testing.T) {
	snap := NewHistogram("none", nil).Snapshot()
	assert.Equal(t, int64(0), snap.Count)
	assert.Equal(t, time.Duration(0), snap.Avg)
	assert.Equal(t, time.Duration(0), snap.Min, "no observations -> zero min")
}

func TestRegistryGetOrCreate(t *testing.T) {
	r := NewRegistry()
	c1 := r.Counter("errors")
	c1.Inc()
	c2 := r.Counter("errors") // same name -> same instance
	c2.Inc()
	assert.Same(t, c1, c2)
	assert.Equal(t, int64(2), c1.Value())

	g := r.Gauge("depth")
	g.Set(7)
	h := r.Histogram("latency")
	h.Observe(time.Millisecond)

	names := r.Names()
	assert.Equal(t, []string{"depth", "errors", "latency"}, names)

	got, ok := r.Get("errors")
	require.True(t, ok)
	assert.Equal(t, int64(2), got.SnapshotValue())
	_, ok = r.Get("nope")
	assert.False(t, ok)
}

func TestRegistrySnapshot(t *testing.T) {
	r := NewRegistry()
	r.Counter("in").IncBy(10)
	r.Gauge("q").Set(3)
	r.Histogram("lat").Observe(time.Millisecond)

	snap := r.Snapshot()
	assert.Equal(t, int64(10), snap["in"])
	assert.Equal(t, int64(3), snap["q"])
	hSnap, ok := snap["lat"].(HistogramSnapshot)
	require.True(t, ok)
	assert.Equal(t, int64(1), hSnap.Count)
}

func TestCounterConcurrent(t *testing.T) {
	c := NewCounter("c")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.Inc()
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(10000), c.Value(), "atomic counter must be exact under concurrency")
}

func TestHistogramConcurrent(t *testing.T) {
	h := NewHistogram("h", []time.Duration{time.Millisecond, 10 * time.Millisecond})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				h.Observe(time.Duration(n) * time.Microsecond)
			}
		}(i)
	}
	wg.Wait()
	snap := h.Snapshot()
	assert.Equal(t, int64(5000), snap.Count, "all observations counted")
	// Min must be >= 0 and Max must be a plausible observed value (<= 49us).
	assert.GreaterOrEqual(t, snap.Min, time.Duration(0))
	assert.LessOrEqual(t, snap.Max, 49*time.Microsecond)
}
