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
	"sort"
	"sync"
)

// Metric is implemented by all registry instruments.
type Metric interface {
	Name() string
	SnapshotValue() interface{}
}

// Registry is a named collection of metrics.
type Registry struct {
	mu      sync.RWMutex
	metrics map[string]Metric
}

func NewRegistry() *Registry {
	return &Registry{metrics: make(map[string]Metric)}
}

// Register stores m under its name, replacing any prior metric with that name.
func (r *Registry) Register(m Metric) {
	r.mu.Lock()
	r.metrics[m.Name()] = m
	r.mu.Unlock()
}

// Counter returns the Counter by name, creating it if absent.
func (r *Registry) Counter(name string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.metrics[name]; ok {
		if c, ok := m.(*Counter); ok {
			return c
		}
	}
	c := NewCounter(name)
	r.metrics[name] = c
	return c
}

// Gauge returns the Gauge by name, creating it if absent.
func (r *Registry) Gauge(name string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.metrics[name]; ok {
		if g, ok := m.(*Gauge); ok {
			return g
		}
	}
	g := NewGauge(name)
	r.metrics[name] = g
	return g
}

// Histogram returns the Histogram by name, creating it with default buckets if
// absent. For custom buckets, build with NewHistogram and pass to Register.
func (r *Registry) Histogram(name string) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.metrics[name]; ok {
		if h, ok := m.(*Histogram); ok {
			return h
		}
	}
	h := NewHistogram(name, nil)
	r.metrics[name] = h
	return h
}

func (r *Registry) Get(name string) (Metric, bool) {
	r.mu.RLock()
	m, ok := r.metrics[name]
	r.mu.RUnlock()
	return m, ok
}

// Names returns the sorted names of all registered metrics.
func (r *Registry) Names() []string {
	r.mu.RLock()
	names := make([]string, 0, len(r.metrics))
	for n := range r.metrics {
		names = append(names, n)
	}
	r.mu.RUnlock()
	sort.Strings(names)
	return names
}

// Snapshot returns name -> snapshot value for every registered metric.
func (r *Registry) Snapshot() map[string]interface{} {
	r.mu.RLock()
	out := make(map[string]interface{}, len(r.metrics))
	for n, m := range r.metrics {
		out[n] = m.SnapshotValue()
	}
	r.mu.RUnlock()
	return out
}
