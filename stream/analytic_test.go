package stream

import (
	"container/list"
	"testing"

	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
)

// D2：PARTITION 状态受 LRU 上限约束，超限淘汰最久未使用的分区（含其 lastResults）。
func TestAnalyticPartitionLRUEviction(t *testing.T) {
	fe := &analyticFieldEngine{
		af:          types.AnalyticField{Over: &types.OverSpec{PartitionBy: []string{"k"}}},
		stateCtors:  []func() functions.AnalyticState{func() functions.AnalyticState { return new(functions.LagFunction).NewState() }},
		partitions:  make(map[string]*list.Element),
		lru:         list.New(),
		lastResults: make(map[string]any),
		maxPartitions: 3,
	}

	keyOf := func(v any) string { return fe.partitionKey(map[string]any{"k": v}) }
	touch := func(v any) {
		fe.mu.Lock()
		defer fe.mu.Unlock()
		fe.getStateLocked(keyOf(v))
	}

	// 填满 3 个分区。
	touch(1)
	touch(2)
	touch(3)
	assert.Equal(t, 3, fe.lru.Len())

	// 加入第 4 个 → 淘汰最久未用的 k=1。
	touch(4)
	assert.Equal(t, 3, fe.lru.Len(), "超出上限应按 LRU 淘汰")
	_, has1 := fe.partitions[keyOf(1)]
	assert.False(t, has1, "k=1 应被淘汰")
	for _, v := range []any{2, 3, 4} {
		_, ok := fe.partitions[keyOf(v)]
		assert.True(t, ok, "k=%v 应保留", v)
	}

	// 访问 k=2（提升为最近使用），再加入 k=5 → 应淘汰 k=3（当前最久未用）。
	touch(2)
	touch(5)
	_, has3 := fe.partitions[keyOf(3)]
	assert.False(t, has3, "k=3 应在 k=2 被访问后成为最久未用而被淘汰")
	for _, v := range []any{2, 4, 5} {
		_, ok := fe.partitions[keyOf(v)]
		assert.True(t, ok, "k=%v 应保留", v)
	}
}

// D1：partitionKey 带类型前缀，同值不同类型落不同键。
func TestAnalyticPartitionKeyTypeTag(t *testing.T) {
	assert.Equal(t, "int|1", typeKey(1))
	assert.Equal(t, "string|1", typeKey("1"))
	assert.Equal(t, "int64|1", typeKey(int64(1)))
	assert.Equal(t, "nil|", typeKey(nil))
}

// B2：多列 PARTITION 键在值含 '|' 时不得碰撞串台。
func TestAnalyticPartitionKeyPipeCollision(t *testing.T) {
	fe := &analyticFieldEngine{
		af: types.AnalyticField{Over: &types.OverSpec{PartitionBy: []string{"a", "b"}}},
	}
	k1 := fe.partitionKey(map[string]any{"a": "x", "b": "string|Y"})
	k2 := fe.partitionKey(map[string]any{"a": "x|string", "b": "Y"})
	assert.NotEqual(t, k1, k2, "值含 | 的不同分区元组不得生成同一键")
}
