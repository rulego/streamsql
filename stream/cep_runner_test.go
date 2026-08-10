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
	"time"

	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/rsql"
	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cepSQL is a two-step rising pattern: B fires once a row's v exceeds A's.
const cepSQL = `SELECT av, bv FROM stream MATCH_RECOGNIZE (
	PARTITION BY deviceId
	ORDER BY ts
	MEASURES A.v AS av, B.v AS bv
	PATTERN (A B)
	DEFINE A AS v > 0, B AS v > 10
)`

// newCEPStream builds a stream running the CEP path from real parsed SQL.
func newCEPStream(t *testing.T, sql string) *Stream {
	t.Helper()
	cfg, _, err := rsql.Parse(sql)
	require.NoError(t, err)
	require.Equal(t, types.ExecCEP, cfg.Mode, "SQL must resolve to the CEP path")

	s, err := NewStream(*cfg)
	require.NoError(t, err)
	t.Cleanup(s.Stop)
	return s
}

func TestNewCepRunner_RejectsInvalidSpec(t *testing.T) {
	// A spec with no pattern cannot build an engine.
	_, err := newCepRunner(&types.MatchRecognizeSpec{}, 0, logger.GetDefault())
	require.Error(t, err, "a spec without a PATTERN must fail to build")
}

func TestNewCepRunner_AppliesPartitionCap(t *testing.T) {
	cfg, _, err := rsql.Parse(cepSQL)
	require.NoError(t, err)

	cr, err := newCepRunner(cfg.MatchRecognize, 5, logger.GetDefault())
	require.NoError(t, err)
	require.NotNil(t, cr)

	// Start/Stop must be safe to call around the WITHIN sweeper.
	cr.Start()
	cr.Stop()
}

func TestCepRunner_PartitionKeySeparatesAndTypesKeys(t *testing.T) {
	cfg, _, err := rsql.Parse(cepSQL)
	require.NoError(t, err)
	cr, err := newCepRunner(cfg.MatchRecognize, 0, logger.GetDefault())
	require.NoError(t, err)
	defer cr.Stop()

	k1 := cr.partitionKey(map[string]any{"deviceId": "d1"})
	k2 := cr.partitionKey(map[string]any{"deviceId": "d2"})
	assert.NotEqual(t, k1, k2, "distinct partition values must produce distinct keys")

	same := cr.partitionKey(map[string]any{"deviceId": "d1"})
	assert.Equal(t, k1, same, "the same partition value must be stable")

	// "1" (string) and 1 (int) must not collide.
	ks := cr.partitionKey(map[string]any{"deviceId": "1"})
	kn := cr.partitionKey(map[string]any{"deviceId": 1})
	assert.NotEqual(t, ks, kn, `partition key "1" must not collide with 1`)
}

func TestCepRunner_PartitionKeyEmptyWithoutPartitionBy(t *testing.T) {
	cfg, _, err := rsql.Parse(`SELECT av FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES A.v AS av
		PATTERN (A B)
		DEFINE A AS v > 0, B AS v > 10
	)`)
	require.NoError(t, err)
	cr, err := newCepRunner(cfg.MatchRecognize, 0, logger.GetDefault())
	require.NoError(t, err)
	defer cr.Stop()

	assert.Empty(t, cr.partitionKey(map[string]any{"deviceId": "d1"}),
		"no PARTITION BY means a single bucket (empty key)")
}

func TestStream_IsCEPQuery(t *testing.T) {
	s := newCEPStream(t, cepSQL)
	assert.True(t, s.IsCEPQuery())
	assert.False(t, s.IsAggregationQuery(), "CEP is not an aggregation query")

	plain, err := NewStream(types.Config{})
	require.NoError(t, err)
	defer plain.Stop()
	assert.False(t, plain.IsCEPQuery())
}

// The CEP path must emit a projected match through the async sink.
func TestProcessCEP_EmitsProjectedMatch(t *testing.T) {
	s := newCEPStream(t, cepSQL)

	var mu sync.Mutex
	var got []map[string]any
	s.AddSink(func(rows []map[string]any) {
		mu.Lock()
		got = append(got, rows...)
		mu.Unlock()
	})
	s.Start()

	dp := &DataProcessor{stream: s}
	dp.processCEP(map[string]any{"deviceId": "d1", "v": 5.0, "ts": 1})  // A
	dp.processCEP(map[string]any{"deviceId": "d1", "v": 20.0, "ts": 2}) // B: completes A B

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) > 0
	}, 2*time.Second, 10*time.Millisecond, "a completed pattern must emit")

	mu.Lock()
	defer mu.Unlock()
	row := got[0]
	assert.EqualValues(t, 5, row["av"], "MEASURES A.v must project")
	assert.EqualValues(t, 20, row["bv"], "MEASURES B.v must project")
}

// Partitions must not cross-match: A from one device plus B from another is not
// a match.
func TestProcessCEP_PartitionsDoNotCrossMatch(t *testing.T) {
	s := newCEPStream(t, cepSQL)

	var mu sync.Mutex
	count := 0
	s.AddSink(func(rows []map[string]any) {
		mu.Lock()
		count += len(rows)
		mu.Unlock()
	})
	s.Start()

	dp := &DataProcessor{stream: s}
	dp.processCEP(map[string]any{"deviceId": "d1", "v": 5.0, "ts": 1})
	dp.processCEP(map[string]any{"deviceId": "d2", "v": 20.0, "ts": 2})

	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	assert.Zero(t, count, "A and B from different partitions must not form a match")
}

// The filter runs before the CEP engine, so rejected rows never reach it.
func TestProcessCEP_FilterRunsBeforeEngine(t *testing.T) {
	s := newCEPStream(t, cepSQL)
	require.NoError(t, s.RegisterFilter("v < 100"))

	var mu sync.Mutex
	count := 0
	s.AddSink(func(rows []map[string]any) {
		mu.Lock()
		count += len(rows)
		mu.Unlock()
	})
	s.Start()

	dp := &DataProcessor{stream: s}
	dp.processCEP(map[string]any{"deviceId": "d1", "v": 5.0, "ts": 1})
	dp.processCEP(map[string]any{"deviceId": "d1", "v": 500.0, "ts": 2}) // rejected by the filter
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	filtered := count
	mu.Unlock()
	assert.Zero(t, filtered, "a row rejected by WHERE must not reach the CEP engine")

	dp.processCEP(map[string]any{"deviceId": "d1", "v": 20.0, "ts": 3}) // passes: completes the match
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return count > 0
	}, 2*time.Second, 10*time.Millisecond, "a passing row must still match")
}

func TestProcessCEP_NoPanicWhenRunnerAbsent(t *testing.T) {
	s, err := NewStream(types.Config{})
	require.NoError(t, err)
	defer s.Stop()

	dp := &DataProcessor{stream: s}
	assert.NotPanics(t, func() {
		dp.processCEP(map[string]any{"v": 1.0})
	}, "a nil CEP runner must be a no-op, not a panic")
}

func TestProjectCep_AppliesOuterSelect(t *testing.T) {
	// Outer SELECT keeps only av, so bv must be dropped from the output.
	s := newCEPStream(t, `SELECT av FROM stream MATCH_RECOGNIZE (
		PARTITION BY deviceId
		ORDER BY ts
		MEASURES A.v AS av, B.v AS bv
		PATTERN (A B)
		DEFINE A AS v > 0, B AS v > 10
	)`)

	out := s.projectCep([]map[string]any{{"av": 5.0, "bv": 20.0}})
	require.Len(t, out, 1)
	assert.Contains(t, out[0], "av")
	assert.NotContains(t, out[0], "bv",
		"outer SELECT must drop MEASURES columns it does not list")
}

func TestEmitCepResults_EmptyIsNoOp(t *testing.T) {
	s := newCEPStream(t, cepSQL)
	var called bool
	s.AddSink(func([]map[string]any) { called = true })
	s.Start()

	s.emitCepResults(nil)
	time.Sleep(100 * time.Millisecond)
	assert.False(t, called, "no results means no sink dispatch")
}

// Stop-time flush dispatches inline (the worker pool is already gone), so an
// unclosed match must still reach sinks.
func TestEmitCepFlushSync_DispatchesInline(t *testing.T) {
	s := newCEPStream(t, cepSQL)

	var mu sync.Mutex
	var got []map[string]any
	s.AddSink(func(rows []map[string]any) {
		mu.Lock()
		got = append(got, rows...)
		mu.Unlock()
	})

	s.emitCepFlushSync([]map[string]any{{"av": 1.0, "bv": 2.0}})

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 1, "flush must dispatch inline without the worker pool")
	assert.EqualValues(t, 1, got[0]["av"])
}

func TestEmitCepFlushSync_EmptyIsNoOp(t *testing.T) {
	s := newCEPStream(t, cepSQL)
	called := false
	s.AddSink(func([]map[string]any) { called = true })

	s.emitCepFlushSync(nil)
	assert.False(t, called)
}

// A panicking sink must not take down the inline flush path.
func TestInvokeSinksInline_RecoversFromSinkPanic(t *testing.T) {
	s := newCEPStream(t, cepSQL)

	reached := false
	s.AddSink(func([]map[string]any) { panic("boom") })
	s.AddSink(func([]map[string]any) { reached = true })

	assert.NotPanics(t, func() {
		s.invokeSinksInline([]map[string]any{{"av": 1.0}})
	}, "a panicking sink must be contained")
	assert.True(t, reached, "a panicking sink must not stop the remaining sinks")
}

func TestInvokeSinksInline_IncludesSyncSinks(t *testing.T) {
	s := newCEPStream(t, cepSQL)

	syncCalled := false
	s.AddSyncSink(func([]map[string]any) { syncCalled = true })

	s.invokeSinksInline([]map[string]any{{"av": 1.0}})
	assert.True(t, syncCalled, "inline dispatch must include sync sinks")
}
