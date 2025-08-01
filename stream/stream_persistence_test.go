package stream

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPersistenceManagerBasic 测试持久化管理器基本功能
func TestPersistenceManagerBasic(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()

	// 创建持久化管理器
	pm := NewPersistenceManagerWithConfig(tmpDir, 1024, 100*time.Millisecond)

	// 启动管理器
	err := pm.Start()
	require.NoError(t, err)
	defer pm.Stop()

	// 测试数据持久化
	testData := []interface{}{
		map[string]interface{}{"id": 1, "value": "test1"},
		map[string]interface{}{"id": 2, "value": "test2"},
		map[string]interface{}{"id": 3, "value": "test3"},
	}

	// 写入数据
	for _, data := range testData {
		err := pm.PersistData(data)
		require.NoError(t, err)
	}

	// 等待数据写入
	time.Sleep(200 * time.Millisecond)

	// 读取持久化数据
	loadedData, err := pm.LoadPersistedData()
	require.NoError(t, err)

	// 验证数据
	assert.GreaterOrEqual(t, len(loadedData), len(testData))
}

// TestPersistenceManagerFileRotation 测试文件轮转功能
func TestPersistenceManagerFileRotation(t *testing.T) {
	tmpDir := t.TempDir()

	// 创建小文件大小的持久化管理器以触发文件轮转
	pm := NewPersistenceManagerWithConfig(tmpDir, 100, 50*time.Millisecond)

	err := pm.Start()
	require.NoError(t, err)
	defer pm.Stop()

	// 写入大量数据以触发文件轮转
	for i := 0; i < 50; i++ {
		data := map[string]interface{}{
			"id":    i,
			"value": fmt.Sprintf("test_data_with_long_content_%d", i),
		}
		err := pm.PersistData(data)
		require.NoError(t, err)
	}

	// 等待数据写入和文件轮转
	time.Sleep(200 * time.Millisecond)

	// 验证创建了多个文件
	files, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	assert.Greater(t, len(files), 1, "应该创建多个持久化文件")
}

// TestPersistenceManagerConcurrency 测试持久化管理器并发安全性
func TestPersistenceManagerConcurrency(t *testing.T) {
	tmpDir := t.TempDir()

	pm := NewPersistenceManagerWithConfig(tmpDir, 2048, 100*time.Millisecond)

	err := pm.Start()
	require.NoError(t, err)
	defer pm.Stop()

	// 并发写入数据
	const numGoroutines = 10
	const dataPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < dataPerGoroutine; j++ {
				data := map[string]interface{}{
					"goroutine": goroutineID,
					"sequence":  j,
					"value":     fmt.Sprintf("data_%d_%d", goroutineID, j),
				}
				err := pm.PersistData(data)
				assert.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// 等待所有数据写入
	time.Sleep(300 * time.Millisecond)

	// 验证数据完整性
	loadedData, err := pm.LoadPersistedData()
	require.NoError(t, err)

	// 应该至少有部分数据被持久化
	assert.Greater(t, len(loadedData), 0)
	t.Logf("并发测试: 持久化了 %d 条数据", len(loadedData))
}

// TestPersistenceManagerStats 测试持久化统计功能
func TestPersistenceManagerStats(t *testing.T) {
	tmpDir := t.TempDir()

	pm := NewPersistenceManagerWithConfig(tmpDir, 1024, 50*time.Millisecond)

	err := pm.Start()
	require.NoError(t, err)
	defer pm.Stop()

	// 写入一些数据
	for i := 0; i < 10; i++ {
		data := map[string]interface{}{"index": i, "data": "test"}
		err := pm.PersistData(data)
		require.NoError(t, err)
	}

	// 等待数据处理
	time.Sleep(200 * time.Millisecond)

	// 获取统计信息
	stats := pm.GetStats()
	require.NotNil(t, stats)

	// 验证统计信息包含预期字段
	assert.Contains(t, stats, "data_dir")
	assert.Contains(t, stats, "max_file_size")
	assert.Contains(t, stats, "flush_interval")
	assert.Contains(t, stats, "running")
	assert.Equal(t, tmpDir, stats["data_dir"])
	assert.Equal(t, int64(1024), stats["max_file_size"])
	assert.Equal(t, true, stats["running"])

	t.Logf("持久化统计信息: %+v", stats)
}

// TestPersistenceManagerConfiguration 测试不同配置的持久化管理器
func TestPersistenceManagerConfiguration(t *testing.T) {
	tests := []struct {
		name          string
		maxFileSize   int64
		flushInterval time.Duration
		dataCount     int
		expectedFiles int // 预期的最小文件数
	}{
		{
			name:          "Small files, fast flush",
			maxFileSize:   50,
			flushInterval: 10 * time.Millisecond,
			dataCount:     20,
			expectedFiles: 2,
		},
		{
			name:          "Large files, slow flush",
			maxFileSize:   2048,
			flushInterval: 100 * time.Millisecond,
			dataCount:     10,
			expectedFiles: 1,
		},
		{
			name:          "Medium files, medium flush",
			maxFileSize:   512,
			flushInterval: 50 * time.Millisecond,
			dataCount:     30,
			expectedFiles: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			pm := NewPersistenceManagerWithConfig(tmpDir, tt.maxFileSize, tt.flushInterval)

			err := pm.Start()
			require.NoError(t, err)
			defer pm.Stop()

			// 写入测试数据
			for i := 0; i < tt.dataCount; i++ {
				data := map[string]interface{}{
					"index": i,
					"data":  fmt.Sprintf("test_data_item_%d_with_some_content", i),
				}
				err := pm.PersistData(data)
				require.NoError(t, err)
			}

			// 等待数据写入
			time.Sleep(tt.flushInterval*3 + 100*time.Millisecond)

			// 检查文件数量
			files, err := os.ReadDir(tmpDir)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(files), tt.expectedFiles, "文件数量不符合预期")

			// 验证数据完整性
			loadedData, err := pm.LoadPersistedData()
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(loadedData), tt.dataCount/2, "加载的数据数量过少")

			t.Logf("%s: 创建了 %d 个文件，加载了 %d 条数据", tt.name, len(files), len(loadedData))
		})
	}
}

// TestPersistenceManagerErrorHandling 测试持久化管理器错误处理
func TestPersistenceManagerErrorHandling(t *testing.T) {
	t.Run("Stop before start", func(t *testing.T) {
		tmpDir := t.TempDir()
		pm := NewPersistenceManagerWithConfig(tmpDir, 1024, 100*time.Millisecond)

		// 在启动前停止不应该出错
		err := pm.Stop()
		assert.NoError(t, err, "在启动前停止不应该出错")
	})

	t.Run("Persist data before start", func(t *testing.T) {
		tmpDir := t.TempDir()
		pm := NewPersistenceManagerWithConfig(tmpDir, 1024, 100*time.Millisecond)

		// 在启动前持久化数据应该失败
		data := map[string]interface{}{"test": "data"}
		err := pm.PersistData(data)
		assert.Error(t, err, "在启动前持久化数据应该失败")
	})

	t.Run("Load data from empty directory", func(t *testing.T) {
		tmpDir := t.TempDir()
		pm := NewPersistenceManagerWithConfig(tmpDir, 1024, 100*time.Millisecond)

		err := pm.Start()
		require.NoError(t, err)
		defer pm.Stop()

		// 从空目录加载数据应该返回空切片
		loadedData, err := pm.LoadPersistedData()
		assert.NoError(t, err)
		assert.Empty(t, loadedData, "从空目录加载应该返回空数据")
	})
}

// TestPersistenceManagerLifecycle 测试持久化管理器生命周期
func TestPersistenceManagerLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	pm := NewPersistenceManagerWithConfig(tmpDir, 1024, 50*time.Millisecond)

	// 初始状态
	stats := pm.GetStats()
	assert.Equal(t, false, stats["running"], "初始状态应该是未运行")

	// 启动
	err := pm.Start()
	require.NoError(t, err)
	defer func() {
		// 安全停止
		if stats := pm.GetStats(); stats["running"].(bool) {
			pm.Stop()
		}
	}()

	stats = pm.GetStats()
	assert.Equal(t, true, stats["running"], "启动后应该是运行状态")

	// 写入一些数据
	for i := 0; i < 5; i++ {
		data := map[string]interface{}{"id": i, "value": fmt.Sprintf("test_%d", i)}
		err := pm.PersistData(data)
		require.NoError(t, err)
	}

	// 等待数据写入
	time.Sleep(100 * time.Millisecond)

	// 验证数据已持久化
	files, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	assert.Greater(t, len(files), 0, "应该有持久化文件")

	// 加载数据
	loadedData, err := pm.LoadPersistedData()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(loadedData), 5, "应该能加载持久化的数据")
}
