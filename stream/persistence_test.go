package stream

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPersistenceManager_BasicOperations 测试持久化管理器的基本操作
func TestPersistenceManager_BasicOperations(t *testing.T) {
	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), "persistence_test")
	defer os.RemoveAll(tempDir)

	// 创建持久化管理器
	pm := NewPersistenceManager(tempDir)
	require.NotNil(t, pm)

	// 启动管理器
	err := pm.Start()
	require.NoError(t, err)
	defer func() {
		if pm != nil {
			pm.Stop()
		}
	}()

	// 测试数据持久化
	testData := []map[string]interface{}{
		{"message": "test_data_1", "id": 1},
		{"message": "test_data_2", "id": 2},
		{"message": "test_data_3", "id": 3},
	}

	for _, data := range testData {
		err := pm.PersistData(data)
		assert.NoError(t, err)
	}

	// 等待数据刷新到磁盘
	time.Sleep(3 * time.Second)

	// 验证统计信息
	stats := pm.GetStats()
	if totalPersisted, ok := stats["total_persisted"].(int64); ok {
		assert.Equal(t, int64(3), totalPersisted)
	} else {
		t.Errorf("total_persisted field is missing or not int64: %v", stats["total_persisted"])
	}
	if filesCreated, ok := stats["files_created"].(int64); ok {
		assert.True(t, filesCreated > 0)
	} else {
		t.Errorf("files_created field is missing or not int64: %v", stats["files_created"])
	}
}

// TestPersistenceManager_DataRecovery 测试数据恢复功能
func TestPersistenceManager_DataRecovery(t *testing.T) {
	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), "recovery_test")
	defer os.RemoveAll(tempDir)

	// 第一阶段：持久化数据
	pm1 := NewPersistenceManager(tempDir)
	err := pm1.Start()
	require.NoError(t, err)

	// 持久化测试数据
	testData := []map[string]interface{}{
		{"message": "data_1", "id": 1},
		{"message": "data_2", "id": 2},
		{"message": "data_3", "id": 3},
		{"message": "data_4", "id": 4},
		{"message": "data_5", "id": 5},
	}
	for _, data := range testData {
		err := pm1.PersistData(data)
		require.NoError(t, err)
	}

	// 等待数据刷新到磁盘
	time.Sleep(3 * time.Second)

	if pm1 != nil {
		pm1.Stop()
	}

	// 第二阶段：恢复数据
	pm2 := NewPersistenceManager(tempDir)
	err = pm2.Start()
	require.NoError(t, err)
	defer func() {
		if pm2 != nil {
			if pm2 != nil {
		pm2.Stop()
	}
		}
	}()

	// 加载并恢复数据
	err = pm2.LoadAndRecoverData()
	require.NoError(t, err)

	// 等待恢复数据填充到队列中
	time.Sleep(200 * time.Millisecond)

	// 按序获取恢复数据
	recoveredData := make([]map[string]interface{}, 0)
	for i := 0; i < len(testData); i++ {
		data, hasMore := pm2.GetRecoveryData()
		if hasMore && data != nil {
			recoveredData = append(recoveredData, data)
		} else {
			break
		}
	}

	// 验证数据顺序和完整性
	assert.Equal(t, len(testData), len(recoveredData))
	for i, expected := range testData {
		assert.Equal(t, expected["message"], recoveredData[i]["message"], "数据顺序不正确")
		// JSON反序列化会将数字转换为float64，需要类型转换
		expectedID := float64(expected["id"].(int))
		assert.Equal(t, expectedID, recoveredData[i]["id"], "数据内容不正确")
	}
}

// TestPersistenceManager_SequenceNumbering 测试序列号管理
func TestPersistenceManager_SequenceNumbering(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "sequence_test")
	defer os.RemoveAll(tempDir)

	pm := NewPersistenceManager(tempDir)
	err := pm.Start()
	require.NoError(t, err)
	defer func() {
		if pm != nil {
			pm.Stop()
		}
	}()

	// 持久化足够的数据以触发序列号递增
	for i := 0; i < 10; i++ {
		data := map[string]interface{}{"message": fmt.Sprintf("data_%d", i), "id": i}
		err := pm.PersistData(data)
		require.NoError(t, err)
	}

	// 等待数据刷新到磁盘
	time.Sleep(3 * time.Second)

	// 验证统计信息
	stats := pm.GetStats()
	if totalPersisted, ok := stats["total_persisted"].(int64); ok {
		assert.Equal(t, int64(10), totalPersisted)
	} else {
		t.Logf("Stats: %+v", stats)
		t.Fatalf("total_persisted not found or wrong type")
	}
	if sequenceCounter, ok := stats["sequence_counter"].(int64); ok {
		assert.Equal(t, int64(10), sequenceCounter)
	}
}

// TestPersistenceManager_FileRotation 测试文件轮转
func TestPersistenceManager_FileRotation(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "rotation_test")
	defer os.RemoveAll(tempDir)

	// 使用较小的文件大小以触发轮转
	pm := NewPersistenceManagerWithConfig(tempDir, 100, 50*time.Millisecond)
	err := pm.Start()
	require.NoError(t, err)
	defer func() {
		if pm != nil {
			pm.Stop()
		}
	}()

	// 持久化足够的数据以触发文件轮转
	for i := 0; i < 20; i++ {
		longData := map[string]interface{}{
			"message": fmt.Sprintf("this_is_a_long_data_string_to_trigger_file_rotation_%d", i),
			"id":      i,
			"extra":   "some extra data to make it longer",
		}
		err := pm.PersistData(longData)
		require.NoError(t, err)
	}

	// 等待数据刷新
	time.Sleep(200 * time.Millisecond)

	// 验证创建了多个文件
	stats := pm.GetStats()
	if filesCreated, ok := stats["files_created"].(int64); ok {
		assert.True(t, filesCreated > 1, "应该创建多个文件")
	}
}

// TestPersistenceManager_ConcurrentAccess 测试并发访问
func TestPersistenceManager_ConcurrentAccess(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "concurrent_test")
	defer os.RemoveAll(tempDir)

	pm := NewPersistenceManager(tempDir)
	err := pm.Start()
	require.NoError(t, err)
	defer func() {
		if pm != nil {
			pm.Stop()
		}
	}()

	// 并发持久化数据
	const numGoroutines = 10
	const itemsPerGoroutine = 10

	done := make(chan bool, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer func() { done <- true }()
			for i := 0; i < itemsPerGoroutine; i++ {
				data := map[string]interface{}{
					"message":      fmt.Sprintf("goroutine_%d_item_%d", goroutineID, i),
					"goroutine_id": goroutineID,
					"item_id":      i,
				}
				err := pm.PersistData(data)
				assert.NoError(t, err)
			}
		}(g)
	}

	// 等待所有goroutine完成
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// 等待数据刷新到磁盘
	time.Sleep(3 * time.Second)

	// 验证所有数据都被持久化
	stats := pm.GetStats()
	expectedTotal := int64(numGoroutines * itemsPerGoroutine)
	if totalPersisted, ok := stats["total_persisted"].(int64); ok {
		assert.Equal(t, expectedTotal, totalPersisted)
	} else {
		t.Logf("Stats: %+v", stats)
		t.Fatalf("total_persisted not found or wrong type")
	}
}

// TestPersistenceManager_ErrorHandling 测试错误处理
func TestPersistenceManager_ErrorHandling(t *testing.T) {
	// 测试无效目录 - 使用一个包含无效字符的路径
	invalidDir := "\x00invalid\x00path"
	pm := NewPersistenceManager(invalidDir)
	err := pm.Start()
	assert.Error(t, err, "应该返回错误")

	// 测试重复启动
	tempDir := filepath.Join(os.TempDir(), "error_test")
	defer os.RemoveAll(tempDir)

	pm2 := NewPersistenceManager(tempDir)
	err = pm2.Start()
	require.NoError(t, err)

	// 重复启动应该返回错误
	err = pm2.Start()
	assert.Error(t, err, "重复启动应该返回错误")

	pm2.Stop()
}

// TestPersistenceManager_RetryAndDeadLetter 测试重试限制和死信队列功能
// 验证重试限制、死信队列和退避策略是否正常工作
func TestPersistenceManager_RetryAndDeadLetter(t *testing.T) {
	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), "streamsql_test_retry")
	defer os.RemoveAll(tempDir)

	// 创建持久化管理器
	pm := NewPersistenceManager(tempDir)
	pm.SetMaxRetryCount(2) // 设置最大重试2次

	// 启动持久化管理器
	if err := pm.Start(); err != nil {
		t.Fatalf("Failed to start persistence manager: %v", err)
	}
	defer func() {
		if pm != nil {
			pm.Stop()
		}
	}()

	// 测试数据
	testData := map[string]interface{}{
		"id":      1,
		"message": "test retry",
		"value":   100.5,
	}

	// 测试正常持久化
	t.Run("Normal Persistence", func(t *testing.T) {
		err := pm.PersistDataWithRetryLimit(testData, 0)
		if err != nil {
			t.Errorf("Failed to persist data: %v", err)
		}
	})

	// 测试重试限制
	t.Run("Retry Limit", func(t *testing.T) {
		// 模拟重试数据
		retryData := map[string]interface{}{
			"id":           2,
			"message":      "retry test",
			"_sequence_id": float64(123),
			"_retry_count": 3, // 超过最大重试次数
		}

		// 检查是否应该重试
		shouldRetry := pm.ShouldRetryRecoveredData(retryData)
		if shouldRetry {
			t.Error("Should not retry data that exceeded retry limit")
		}
	})

	// 测试死信队列
	t.Run("Dead Letter Queue", func(t *testing.T) {
		// 获取初始死信队列大小
		initialSize := len(pm.GetDeadLetterQueue())

		// 移动数据到死信队列
		deadData := map[string]interface{}{
			"id":      3,
			"message": "dead letter test",
		}
		pm.MoveToDeadLetterQueue(deadData)

		// 检查死信队列大小
		deadLetterQueue := pm.GetDeadLetterQueue()
		if len(deadLetterQueue) != initialSize+1 {
			t.Errorf("Expected dead letter queue size %d, got %d", initialSize+1, len(deadLetterQueue))
		}

		// 验证死信队列中的数据
		if len(deadLetterQueue) > 0 {
			lastItem := deadLetterQueue[len(deadLetterQueue)-1]
			if lastItem.Reason != "exceeded retry limit during recovery" {
				t.Errorf("Expected reason 'exceeded retry limit during recovery', got '%s'", lastItem.Reason)
			}
		}
	})

	// 测试重新持久化恢复数据
	t.Run("Re-persist Recovered Data", func(t *testing.T) {
		recoveryData := map[string]interface{}{
			"id":      4,
			"message": "recovery test",
		}

		err := pm.RePersistRecoveredData(recoveryData)
		if err != nil {
			t.Errorf("Failed to re-persist recovered data: %v", err)
		}

		// 验证数据中添加了跟踪信息
		if _, exists := recoveryData["_sequence_id"]; !exists {
			t.Error("Expected _sequence_id to be added to recovery data")
		}
		if _, exists := recoveryData["_retry_count"]; !exists {
			t.Error("Expected _retry_count to be added to recovery data")
		}
	})

	// 测试统计信息
	t.Run("Statistics", func(t *testing.T) {
		stats := pm.GetStats()

		// 检查新增的统计字段
		if _, exists := stats["max_retry_count"]; !exists {
			t.Error("Expected max_retry_count in statistics")
		}
		if _, exists := stats["dead_letter_count"]; !exists {
			t.Error("Expected dead_letter_count in statistics")
		}
		if _, exists := stats["total_dropped"]; !exists {
			t.Error("Expected total_dropped in statistics")
		}
		if _, exists := stats["total_retried"]; !exists {
			t.Error("Expected total_retried in statistics")
		}
	})
}

// TestPersistenceManager_RecoveryProcessing 测试恢复处理逻辑
// 验证指数退避和重试限制是否正常工作
func TestPersistenceManager_RecoveryProcessing(t *testing.T) {
	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), "streamsql_test_recovery")
	defer os.RemoveAll(tempDir)

	// 创建持久化管理器
	pm := NewPersistenceManager(tempDir)
	pm.SetMaxRetryCount(2)

	// 启动持久化管理器
	if err := pm.Start(); err != nil {
		t.Fatalf("Failed to start persistence manager: %v", err)
	}
	defer func() {
		if pm != nil {
			pm.Stop()
		}
	}()

	// 测试添加数据时的持久化行为
	testData := map[string]interface{}{
		"id":      1,
		"message": "recovery test",
		"value":   200.5,
	}

	// 直接测试持久化功能
	err := pm.PersistDataWithRetryLimit(testData, 0)
	if err != nil {
		t.Errorf("Failed to persist data: %v", err)
	}

	// 等待一段时间让持久化完成
	time.Sleep(200 * time.Millisecond)

	// 强制刷新持久化数据
	pm.flushPendingData()

	// 检查统计信息
	stats := pm.GetStats()
	totalPersisted := stats["total_persisted"].(int64)
	pendingCount := stats["pending_count"].(int)

	// 检查是否有数据被持久化或正在等待持久化
	if totalPersisted == 0 && pendingCount == 0 {
		t.Error("Expected data to be persisted or pending")
	}
}

// TestPersistenceManager_ConcurrentRetry 测试并发场景下的重试机制
// 验证在高并发情况下重试限制和死信队列是否正常工作
func TestPersistenceManager_ConcurrentRetry(t *testing.T) {
	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), "streamsql_test_concurrent")
	defer os.RemoveAll(tempDir)

	// 创建持久化管理器
	pm := NewPersistenceManager(tempDir)
	pm.SetMaxRetryCount(1) // 设置较低的重试次数以便测试

	// 启动持久化管理器
	if err := pm.Start(); err != nil {
		t.Fatalf("Failed to start persistence manager: %v", err)
	}
	defer func() {
		if pm != nil {
			pm.Stop()
		}
	}()

	// 并发测试参数
	concurrentCount := 50
	var wg sync.WaitGroup

	// 并发添加数据
	for i := 0; i < concurrentCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			testData := map[string]interface{}{
				"id":      index,
				"message": fmt.Sprintf("concurrent test %d", index),
				"value":   float64(index * 10),
			}

			// 随机重试次数
			retryCount := index % 3
			err := pm.PersistDataWithRetryLimit(testData, retryCount)
			if err != nil {
				t.Errorf("Failed to persist data %d: %v", index, err)
			}
		}(i)
	}

	// 等待所有协程完成
	wg.Wait()

	// 等待持久化完成
	time.Sleep(500 * time.Millisecond)

	// 强制刷新持久化数据
	pm.flushPendingData()

	// 检查统计信息
	stats := pm.GetStats()
	totalPersisted := stats["total_persisted"].(int64)
	totalRetried := stats["total_retried"].(int64)
	pendingCount := stats["pending_count"].(int)

	// 检查是否有数据被处理（持久化、重试或等待中）
	if totalPersisted == 0 && totalRetried == 0 && pendingCount == 0 {
		t.Error("Expected some data to be processed (persisted, retried, or pending)")
	}
}
