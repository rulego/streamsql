package aggregator

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// LRU 淘汰必须可观测：淘汰会丢弃该组已累计的聚合值，若无计数则表现为
// 输出里凭空少了一批分组，属静默错值。
func TestGroupAggregator_EvictionIsCounted(t *testing.T) {
	const cap = 10
	ga := NewGroupAggregator([]string{"deviceId"}, []AggregationField{
		{InputField: "v", AggregateType: Sum, OutputAlias: "s"},
	})
	ga.SetMaxPartitions(cap)

	require.Zero(t, ga.EvictedGroups(), "未超上限前不应有淘汰")

	// 恰好到上限：不应淘汰
	for i := 0; i < cap; i++ {
		require.NoError(t, ga.Add(map[string]any{"deviceId": fmt.Sprintf("d%02d", i), "v": 1.0}))
	}
	assert.Zero(t, ga.EvictedGroups(), "恰好到上限不应淘汰")

	// 再加 15 个新分组：应淘汰 15 个
	for i := cap; i < cap+15; i++ {
		require.NoError(t, ga.Add(map[string]any{"deviceId": fmt.Sprintf("d%02d", i), "v": 1.0}))
	}
	assert.EqualValues(t, 15, ga.EvictedGroups(), "淘汰数应与超出的分组数一致")

	// 保留组数仍等于上限
	res, err := ga.GetResults()
	require.NoError(t, err)
	assert.Len(t, res, cap, "保留组数应等于上限")
}

// 重复出现的分组不产生淘汰（只提升 LRU 位置）。
func TestGroupAggregator_RepeatedKeysDoNotEvict(t *testing.T) {
	ga := NewGroupAggregator([]string{"deviceId"}, []AggregationField{
		{InputField: "v", AggregateType: Count, OutputAlias: "c"},
	})
	ga.SetMaxPartitions(5)

	for round := 0; round < 20; round++ {
		for i := 0; i < 5; i++ {
			require.NoError(t, ga.Add(map[string]any{"deviceId": fmt.Sprintf("d%d", i), "v": 1}))
		}
	}
	assert.Zero(t, ga.EvictedGroups(), "分组数未超上限，重复 Add 不应淘汰")
}

// Reset 后计数保留（累计语义），便于上层按增量上报。
func TestGroupAggregator_EvictionCountSurvivesReset(t *testing.T) {
	ga := NewGroupAggregator([]string{"k"}, []AggregationField{
		{InputField: "v", AggregateType: Sum, OutputAlias: "s"},
	})
	ga.SetMaxPartitions(2)

	for i := 0; i < 5; i++ {
		require.NoError(t, ga.Add(map[string]any{"k": fmt.Sprintf("k%d", i), "v": 1.0}))
	}
	before := ga.EvictedGroups()
	require.NotZero(t, before)

	ga.Reset()
	assert.Equal(t, before, ga.EvictedGroups(), "Reset 不应清零累计淘汰数（累计语义）")
}

// 并发 Add 下计数不丢（用 -race 跑有效）。
func TestGroupAggregator_EvictionCountConcurrent(t *testing.T) {
	ga := NewGroupAggregator([]string{"k"}, []AggregationField{
		{InputField: "v", AggregateType: Sum, OutputAlias: "s"},
	})
	ga.SetMaxPartitions(10)

	var wg sync.WaitGroup
	const goroutines, perG = 8, 100
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				_ = ga.Add(map[string]any{"k": fmt.Sprintf("g%d-k%d", g, i), "v": 1.0})
			}
		}(g)
	}
	wg.Wait()

	total := goroutines * perG
	// 共 total 个不同键，上限 10 → 淘汰 total-10
	assert.EqualValues(t, total-10, ga.EvictedGroups(), "并发下淘汰计数应无丢失")

	res, err := ga.GetResults()
	require.NoError(t, err)
	assert.Len(t, res, 10)
}

// EnhancedGroupAggregator（含后聚合表达式）同样暴露淘汰计数。
func TestEnhancedGroupAggregator_EvictionIsCounted(t *testing.T) {
	ega := NewEnhancedGroupAggregator([]string{"k"}, []AggregationField{
		{InputField: "v", AggregateType: Sum, OutputAlias: "s"},
	})
	ega.SetMaxPartitions(3)

	for i := 0; i < 10; i++ {
		require.NoError(t, ega.Add(map[string]any{"k": fmt.Sprintf("k%d", i), "v": 1.0}))
	}
	assert.EqualValues(t, 7, ega.EvictedGroups(), "增强聚合器应继承淘汰计数")
}
