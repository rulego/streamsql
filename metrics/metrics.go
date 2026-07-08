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
	"sync/atomic"
	"time"
)

// Counter is a monotonically increasing counter.
type Counter struct {
	name string
	val  int64
}

func NewCounter(name string) *Counter { return &Counter{name: name} }

func (c *Counter) Inc()              { atomic.AddInt64(&c.val, 1) }
func (c *Counter) IncBy(delta int64) { atomic.AddInt64(&c.val, delta) }
func (c *Counter) Value() int64      { return atomic.LoadInt64(&c.val) }
func (c *Counter) Name() string      { return c.name }
func (c *Counter) Reset()            { atomic.StoreInt64(&c.val, 0) }

func (c *Counter) SnapshotValue() interface{} { return c.Value() }

// Gauge is a settable signed value.
type Gauge struct {
	name string
	val  int64
}

func NewGauge(name string) *Gauge { return &Gauge{name: name} }

func (g *Gauge) Set(v int64)  { atomic.StoreInt64(&g.val, v) }
func (g *Gauge) Inc(n int64)  { atomic.AddInt64(&g.val, n) }
func (g *Gauge) Dec(n int64)  { atomic.AddInt64(&g.val, -n) }
func (g *Gauge) Value() int64 { return atomic.LoadInt64(&g.val) }
func (g *Gauge) Name() string { return g.name }

func (g *Gauge) SnapshotValue() interface{} { return g.Value() }

// Histogram records the distribution of durations into fixed upper-bound buckets.
type Histogram struct {
	name    string
	buckets []int64 // upper bounds in nanoseconds, ascending
	counts  []int64 // cumulative count per bucket
	count   int64
	sum     int64
	min     int64
	max     int64
}

var DefaultLatencyBuckets = []time.Duration{
	10 * time.Microsecond,
	100 * time.Microsecond,
	500 * time.Microsecond,
	time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	time.Second,
}

func NewHistogram(name string, buckets []time.Duration) *Histogram {
	if len(buckets) == 0 {
		buckets = DefaultLatencyBuckets
	}
	bs := make([]int64, len(buckets))
	for i, b := range buckets {
		bs[i] = int64(b)
	}
	return &Histogram{name: name, buckets: bs, counts: make([]int64, len(buckets)), min: 1 << 62}
}

// Observe records d, incrementing every bucket whose upper bound >= d.
func (h *Histogram) Observe(d time.Duration) {
	ns := int64(d)
	atomic.AddInt64(&h.count, 1)
	atomic.AddInt64(&h.sum, ns)
	for i, ub := range h.buckets {
		if ns <= ub {
			atomic.AddInt64(&h.counts[i], 1)
		}
	}
	for {
		cur := atomic.LoadInt64(&h.min)
		if ns >= cur || atomic.CompareAndSwapInt64(&h.min, cur, ns) {
			break
		}
	}
	for {
		cur := atomic.LoadInt64(&h.max)
		if ns <= cur || atomic.CompareAndSwapInt64(&h.max, cur, ns) {
			break
		}
	}
}

type HistogramSnapshot struct {
	Name    string           `json:"name"`
	Count   int64            `json:"count"`
	Sum     time.Duration    `json:"sum"`
	Min     time.Duration    `json:"min"`
	Max     time.Duration    `json:"max"`
	Avg     time.Duration    `json:"avg"`
	Buckets map[string]int64 `json:"buckets"` // upper bound -> cumulative count
}

func (h *Histogram) Snapshot() HistogramSnapshot {
	count := atomic.LoadInt64(&h.count)
	sum := atomic.LoadInt64(&h.sum)
	snap := HistogramSnapshot{
		Name:    h.name,
		Count:   count,
		Sum:     time.Duration(sum),
		Buckets: make(map[string]int64, len(h.buckets)),
	}
	if min := atomic.LoadInt64(&h.min); min != 1<<62 {
		snap.Min = time.Duration(min)
	}
	snap.Max = time.Duration(atomic.LoadInt64(&h.max))
	if count > 0 {
		snap.Avg = time.Duration(sum / count)
	}
	for i, ub := range h.buckets {
		snap.Buckets[time.Duration(ub).String()] = atomic.LoadInt64(&h.counts[i])
	}
	return snap
}

func (h *Histogram) SnapshotValue() interface{} { return h.Snapshot() }

func (h *Histogram) Name() string { return h.name }

// Percentile returns the approximate duration at quantile q (0..1).
func (h *Histogram) Percentile(q float64) time.Duration {
	if q < 0 {
		q = 0
	} else if q > 1 {
		q = 1
	}
	count := atomic.LoadInt64(&h.count)
	if count == 0 {
		return 0
	}
	target := int64(q * float64(count))
	for i, ub := range h.buckets {
		if atomic.LoadInt64(&h.counts[i]) >= target {
			return time.Duration(ub)
		}
	}
	return time.Duration(h.buckets[len(h.buckets)-1])
}

func (h *Histogram) Reset() {
	atomic.StoreInt64(&h.count, 0)
	atomic.StoreInt64(&h.sum, 0)
	atomic.StoreInt64(&h.min, 1<<62)
	atomic.StoreInt64(&h.max, 0)
	for i := range h.counts {
		atomic.StoreInt64(&h.counts[i], 0)
	}
}
