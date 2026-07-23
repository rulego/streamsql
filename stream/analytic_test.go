package stream

import (
	"container/list"
	"testing"

	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
)

// D2: The PARTITION state is constrained by the LRU limit; the most unused partition (including its lastResults) is eliminated when overrun.
func TestAnalyticPartitionLRUEviction(t *testing.T) {
	fe := &analyticFieldEngine{
		af:            types.AnalyticField{Over: &types.OverSpec{PartitionBy: []string{"k"}}},
		stateCtors:    []func() functions.AnalyticState{func() functions.AnalyticState { return new(functions.LagFunction).NewState() }},
		partitions:    make(map[string]*list.Element),
		lru:           list.New(),
		lastResults:   make(map[string]any),
		maxPartitions: 3,
	}

	keyOf := func(v any) string { return fe.partitionKey(map[string]any{"k": v}) }
	touch := func(v any) {
		fe.mu.Lock()
		defer fe.mu.Unlock()
		fe.getStateLocked(keyOf(v))
	}

	// Fill 3 sections.
	touch(1)
	touch(2)
	touch(3)
	assert.Equal(t, 3, fe.lru.Len())

	// Add the 4th → to eliminate the oldest unused k=1.
	touch(4)
	assert.Equal(t, 3, fe.lru.Len(), "超出上限应按 LRU 淘汰")
	_, has1 := fe.partitions[keyOf(1)]
	assert.False(t, has1, "k=1 应被淘汰")
	for _, v := range []any{2, 3, 4} {
		_, ok := fe.partitions[keyOf(v)]
		assert.True(t, ok, "k=%v 应保留", v)
	}

	// Access k=2 (elevate to recently used), then add k=5 → k=3 should be eliminated (currently the longest unused).
	touch(2)
	touch(5)
	_, has3 := fe.partitions[keyOf(3)]
	assert.False(t, has3, "k=3 应在 k=2 被访问后成为最久未用而被淘汰")
	for _, v := range []any{2, 4, 5} {
		_, ok := fe.partitions[keyOf(v)]
		assert.True(t, ok, "k=%v 应保留", v)
	}
}

// D1: partitionKey with type prefix; different keys of the same value but different types are used.
func TestAnalyticPartitionKeyTypeTag(t *testing.T) {
	assert.Equal(t, "int|1", typeKey(1))
	assert.Equal(t, "string|1", typeKey("1"))
	assert.Equal(t, "int64|1", typeKey(int64(1)))
	assert.Equal(t, "nil|", typeKey(nil))
}

// B2: Multi-row PARTITION keys must not collide with serial stations when the value contains '|'.
func TestAnalyticPartitionKeyPipeCollision(t *testing.T) {
	fe := &analyticFieldEngine{
		af: types.AnalyticField{Over: &types.OverSpec{PartitionBy: []string{"a", "b"}}},
	}
	k1 := fe.partitionKey(map[string]any{"a": "x", "b": "string|Y"})
	k2 := fe.partitionKey(map[string]any{"a": "x|string", "b": "Y"})
	assert.NotEqual(t, k1, k2, "值含 | 的不同分区元组不得生成同一键")
}
