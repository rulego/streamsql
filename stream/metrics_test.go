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

package stream

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rulego/streamsql/types"
)

func newTestStream(t *testing.T) *Stream {
	t.Helper()
	s, err := NewStream(types.Config{SimpleFields: []string{"name", "age"}})
	require.NoError(t, err)
	t.Cleanup(func() {
		if s != nil {
			close(s.done)
		}
	})
	return s
}

func TestStreamRegistry_CountersOnInit(t *testing.T) {
	s := newTestStream(t)
	assert.NotNil(t, s.metricsRegistry)
	assert.NotNil(t, s.mInput)
	assert.NotNil(t, s.mOutput)
	assert.NotNil(t, s.mDropped)
	assert.Equal(t, int64(0), s.mInput.Value())
	assert.Equal(t, int64(0), s.mOutput.Value())
	assert.Equal(t, int64(0), s.mDropped.Value())
	// The three counters are registered under their stat keys.
	_, ok := s.metricsRegistry.Get(InputCount)
	assert.True(t, ok)
}

func TestStream_InputCounterIncrements(t *testing.T) {
	s := newTestStream(t)
	s.Emit(map[string]interface{}{"name": "a", "age": 1})
	s.Emit(map[string]interface{}{"name": "b", "age": 2})
	assert.Equal(t, int64(2), s.mInput.Value())
}

func TestStream_GetStatsFromRegistry(t *testing.T) {
	s := newTestStream(t)
	s.Emit(map[string]interface{}{"name": "a", "age": 1})
	stats := s.GetStats()
	assert.Equal(t, int64(1), stats[InputCount], "input_count sourced from registry")
	assert.Contains(t, stats, OutputCount)
	assert.Contains(t, stats, DroppedCount)
}

func TestStream_ResetStats(t *testing.T) {
	s := newTestStream(t)
	s.mInput.IncBy(5)
	s.mOutput.IncBy(3)
	s.mDropped.IncBy(2)
	s.ResetStats()
	assert.Equal(t, int64(0), s.mInput.Value())
	assert.Equal(t, int64(0), s.mOutput.Value())
	assert.Equal(t, int64(0), s.mDropped.Value())
}

func TestStream_MetricsRegistrySnapshot(t *testing.T) {
	s := newTestStream(t)
	s.mInput.IncBy(7)
	snap := s.metricsRegistry.Snapshot()
	assert.Equal(t, int64(7), snap[InputCount])
}

func TestStream_CountersConcurrent(t *testing.T) {
	s := newTestStream(t)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.mInput.Inc()
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(10000), s.mInput.Value())
}
