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
	"testing"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStream_MetricsRegistryIsExposed(t *testing.T) {
	s, err := NewStream(types.Config{})
	require.NoError(t, err)
	defer s.Stop()

	reg := s.MetricsRegistry()
	require.NotNil(t, reg, "the registry backs GetStats and must be reachable")

	// The registry is the same one the counters were built from.
	s.mInput.Inc()
	assert.EqualValues(t, 1, s.GetStats()[InputCount])
}

func TestGetDetailedStats_ShapeAndDerivedRates(t *testing.T) {
	s, err := NewStream(types.Config{})
	require.NoError(t, err)
	defer s.Stop()

	// 4 in, 3 out, 1 dropped → 75% processed, 25% dropped.
	s.mInput.IncBy(4)
	s.mOutput.IncBy(3)
	s.mInputDropped.IncBy(1)

	d := s.GetDetailedStats()

	basic, ok := d[BasicStats].(map[string]int64)
	require.True(t, ok, "detailed stats must embed the basic stats")
	assert.EqualValues(t, 4, basic[InputCount])

	assert.InDelta(t, 75.0, d[ProcessRate], 0.001)
	assert.InDelta(t, 25.0, d[DropRate], 0.001)

	for _, k := range []string{DataChanUsage, ResultChanUsage, SinkPoolUsage} {
		v, ok := d[k].(float64)
		require.Truef(t, ok, "%s must be a float64", k)
		assert.GreaterOrEqual(t, v, 0.0)
		assert.LessOrEqual(t, v, 100.0)
	}

	assert.NotEmpty(t, d[PerformanceLevel])
}

// With no input the rates must not divide by zero.
func TestGetDetailedStats_NoInputIsFullyHealthy(t *testing.T) {
	s, err := NewStream(types.Config{})
	require.NoError(t, err)
	defer s.Stop()

	d := s.GetDetailedStats()
	assert.InDelta(t, 100.0, d[ProcessRate], 0.001, "no input must report 100% processed, not NaN")
	assert.InDelta(t, 0.0, d[DropRate], 0.001)
}

func TestGetDetailedStats_HighDropRateDegradesLevel(t *testing.T) {
	s, err := NewStream(types.Config{})
	require.NoError(t, err)
	defer s.Stop()

	s.mInput.IncBy(10)
	s.mInputDropped.IncBy(6) // 60% drop rate

	d := s.GetDetailedStats()
	assert.Equal(t, PerformanceLevelCritical, d[PerformanceLevel],
		"a 60% drop rate must surface as critical")
}

func TestResetStats_ZeroesCounters(t *testing.T) {
	s, err := NewStream(types.Config{})
	require.NoError(t, err)
	defer s.Stop()

	s.mInput.IncBy(5)
	s.mOutput.IncBy(5)
	s.mInputDropped.IncBy(2)
	s.mOutputDropped.IncBy(1)

	s.ResetStats()

	stats := s.GetStats()
	assert.Zero(t, stats[InputCount])
	assert.Zero(t, stats[OutputCount])
	assert.Zero(t, stats[DroppedCount])
}

func TestAssessPerformanceLevel_Thresholds(t *testing.T) {
	cases := []struct {
		usage, dropRate float64
		want            string
	}{
		{0, 60, PerformanceLevelCritical}, // drop rate dominates
		{0, 30, PerformanceLevelWarning},
		{95, 0, PerformanceLevelHighLoad},
		{80, 0, PerformanceLevelModerateLoad},
		{10, 0, PerformanceLevelOptimal},
	}
	for _, c := range cases {
		assert.Equalf(t, c.want, AssessPerformanceLevel(c.usage, c.dropRate),
			"usage=%v dropRate=%v", c.usage, c.dropRate)
	}
}

func TestNumericFloat_CoversNumericKinds(t *testing.T) {
	cases := []any{
		float64(1), float32(1),
		int(1), int8(1), int16(1), int32(1), int64(1),
		uint(1), uint8(1), uint16(1), uint32(1), uint64(1),
	}
	for _, v := range cases {
		f, ok := numericFloat(v)
		if assert.Truef(t, ok, "%T must be numeric", v) {
			assert.EqualValues(t, 1, f)
		}
	}

	// Non-numerics fall through to the string comparator.
	for _, v := range []any{"1", true, nil, map[string]any{}} {
		_, ok := numericFloat(v)
		assert.Falsef(t, ok, "%T must not be treated as numeric", v)
	}
}
