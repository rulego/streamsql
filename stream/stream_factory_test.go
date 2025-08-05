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
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewStreamFactory 测试流工厂创建
func TestNewStreamFactory(t *testing.T) {
	factory := NewStreamFactory()
	assert.NotNil(t, factory)
}

// TestStreamFactory_CreateStream 测试创建默认配置的流
func TestStreamFactory_CreateStream(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}

	stream, err := factory.CreateStream(config)
	require.NoError(t, err)
	assert.NotNil(t, stream)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	// 验证默认性能配置已应用
	assert.NotEqual(t, types.PerformanceConfig{}, stream.config.PerformanceConfig)
}

// TestStreamFactory_CreateHighPerformanceStream 测试创建高性能流
func TestStreamFactory_CreateHighPerformanceStream(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}

	stream, err := factory.CreateHighPerformanceStream(config)
	require.NoError(t, err)
	assert.NotNil(t, stream)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	// 验证高性能配置
	expectedConfig := types.HighPerformanceConfig()
	assert.Equal(t, expectedConfig, stream.config.PerformanceConfig)
}

// TestStreamFactory_CreateLowLatencyStream 测试创建低延迟流
func TestStreamFactory_CreateLowLatencyStream(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}

	stream, err := factory.CreateLowLatencyStream(config)
	require.NoError(t, err)
	assert.NotNil(t, stream)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	// 验证低延迟配置
	expectedConfig := types.LowLatencyConfig()
	assert.Equal(t, expectedConfig, stream.config.PerformanceConfig)
}

// TestStreamFactory_CreateZeroDataLossStream 测试创建零数据丢失流
func TestStreamFactory_CreateZeroDataLossStream(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}

	stream, err := factory.CreateZeroDataLossStream(config)
	require.NoError(t, err)
	assert.NotNil(t, stream)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	// 验证零数据丢失配置
	expectedConfig := types.ZeroDataLossConfig()
	assert.Equal(t, expectedConfig, stream.config.PerformanceConfig)
}

// TestStreamFactory_CreateCustomPerformanceStream 测试创建自定义性能配置流
func TestStreamFactory_CreateCustomPerformanceStream(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}

	customPerfConfig := types.PerformanceConfig{
		BufferConfig: types.BufferConfig{
			DataChannelSize:   500,
			ResultChannelSize: 200,
		},
		OverflowConfig: types.OverflowConfig{
			Strategy:      StrategyDrop,
			AllowDataLoss: true,
			BlockTimeout:  time.Second,
		},
		WorkerConfig: types.WorkerConfig{
			SinkWorkerCount:  4,
			SinkPoolSize:     100,
			MaxRetryRoutines: 2,
		},
	}

	stream, err := factory.CreateCustomPerformanceStream(config, customPerfConfig)
	require.NoError(t, err)
	assert.NotNil(t, stream)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	// 验证自定义配置
	assert.Equal(t, customPerfConfig, stream.config.PerformanceConfig)
}

// TestStreamFactory_CreateStreamWithWindow 测试创建带窗口的流
func TestStreamFactory_CreateStreamWithWindow(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		SimpleFields: []string{"name", "age"},
		NeedWindow:   true,
		WindowConfig: types.WindowConfig{
			Type: "tumbling",
			Params: map[string]interface{}{
				"size": "5s",
			},
		},
	}

	stream, err := factory.CreateStream(config)
	require.NoError(t, err)
	assert.NotNil(t, stream)
	assert.NotNil(t, stream.Window)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()
}

// TestStreamFactory_CreateStreamWithPersistence 测试创建带持久化的流
func TestStreamFactory_CreateStreamWithPersistence(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		SimpleFields: []string{"name", "age"},
		PerformanceConfig: types.PerformanceConfig{
			BufferConfig: types.BufferConfig{
				DataChannelSize:   100,
				ResultChannelSize: 50,
			},
			OverflowConfig: types.OverflowConfig{
				Strategy: StrategyPersist,
				PersistenceConfig: &types.PersistenceConfig{
					DataDir:       "./test_data",
					MaxFileSize:   1024 * 1024,
					FlushInterval: 5 * time.Second,
				},
			},
			WorkerConfig: types.WorkerConfig{
				SinkWorkerCount:  2,
				SinkPoolSize:     50,
				MaxRetryRoutines: 1,
			},
		},
	}

	stream, err := factory.CreateStream(config)
	require.NoError(t, err)
	assert.NotNil(t, stream)
	assert.NotNil(t, stream.persistenceManager)
	defer func() {
		// 清理测试数据
		if stream.persistenceManager != nil {
			stream.persistenceManager.Stop()
		}
	}()
}

// TestStreamFactory_CreateStreamWithInvalidPersistence 测试创建无效持久化配置的流
func TestStreamFactory_CreateStreamWithInvalidPersistence(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		SimpleFields: []string{"name", "age"},
		PerformanceConfig: types.PerformanceConfig{
			OverflowConfig: types.OverflowConfig{
				Strategy: StrategyPersist,
				// 缺少PersistenceConfig
			},
		},
	}

	_, err := factory.CreateStream(config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "PersistenceConfig is not provided")
}

// TestStreamFactory_CreateStreamWithInvalidStrategy 测试创建无效策略的流
func TestStreamFactory_CreateStreamWithInvalidStrategy(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		SimpleFields: []string{"name", "age"},
		PerformanceConfig: types.PerformanceConfig{
			OverflowConfig: types.OverflowConfig{
				Strategy: "invalid_strategy",
			},
		},
	}

	stream, err := factory.CreateStream(config)
	// 应该使用默认策略而不是报错
	require.NoError(t, err)
	assert.NotNil(t, stream)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	// 验证使用了默认的丢弃策略
	assert.Equal(t, StrategyDrop, stream.dataStrategy.GetStrategyName())
}

// TestStreamFactory_CreateWindow 测试窗口创建
func TestStreamFactory_CreateWindow(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		WindowConfig: types.WindowConfig{
			Type: "tumbling",
			Params: map[string]interface{}{
				"size": "5s",
			},
		},
		PerformanceConfig: types.DefaultPerformanceConfig(),
	}

	win, err := factory.createWindow(config)
	require.NoError(t, err)
	assert.NotNil(t, win)
}

// TestStreamFactory_CreateStreamInstance 测试流实例创建
func TestStreamFactory_CreateStreamInstance(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		SimpleFields:      []string{"name", "age"},
		PerformanceConfig: types.DefaultPerformanceConfig(),
	}

	stream := factory.createStreamInstance(config, nil)
	assert.NotNil(t, stream)
	assert.NotNil(t, stream.dataChan)
	assert.NotNil(t, stream.resultChan)
	assert.NotNil(t, stream.done)
	assert.NotNil(t, stream.sinkWorkerPool)
	assert.Equal(t, config, stream.config)
}

// TestStreamFactory_SetupDataProcessingStrategy 测试数据处理策略设置
func TestStreamFactory_SetupDataProcessingStrategy(t *testing.T) {
	factory := NewStreamFactory()
	stream := &Stream{}

	tests := []struct {
		name     string
		strategy string
		wantErr  bool
	}{
		{"Drop strategy", StrategyDrop, false},
		{"Block strategy", StrategyBlock, false},
		{"Expand strategy", StrategyExpand, false},
		{"Persist strategy", StrategyPersist, false},
		{"Invalid strategy", "invalid", false}, // 应该使用默认策略
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			perfConfig := types.PerformanceConfig{
				OverflowConfig: types.OverflowConfig{
					Strategy: tt.strategy,
				},
			}

			err := factory.setupDataProcessingStrategy(stream, perfConfig)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, stream.dataStrategy)
			}
		})
	}
}

// TestStreamFactory_InitializePersistenceManager 测试持久化管理器初始化
func TestStreamFactory_InitializePersistenceManager(t *testing.T) {
	factory := NewStreamFactory()
	stream := &Stream{}

	// 测试非持久化策略
	perfConfig := types.PerformanceConfig{
		OverflowConfig: types.OverflowConfig{
			Strategy: StrategyDrop,
		},
	}
	err := factory.initializePersistenceManager(stream, perfConfig)
	assert.NoError(t, err)
	assert.Nil(t, stream.persistenceManager)

	// 测试持久化策略但缺少配置
	perfConfig.OverflowConfig.Strategy = StrategyPersist
	err = factory.initializePersistenceManager(stream, perfConfig)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "PersistenceConfig is not provided")

	// 测试有效的持久化配置
	perfConfig.OverflowConfig.PersistenceConfig = &types.PersistenceConfig{
		DataDir:       "./test_data",
		MaxFileSize:   1024 * 1024,
		FlushInterval: 5 * time.Second,
	}
	err = factory.initializePersistenceManager(stream, perfConfig)
	assert.NoError(t, err)
	assert.NotNil(t, stream.persistenceManager)

	// 清理
	if stream.persistenceManager != nil {
		stream.persistenceManager.Stop()
	}
}

// TestStreamFactory_Performance 测试工厂性能
func TestStreamFactory_Performance(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}

	// 创建多个流实例，验证工厂性能
	streams := make([]*Stream, 10)
	for i := 0; i < 10; i++ {
		stream, err := factory.CreateStream(config)
		require.NoError(t, err)
		streams[i] = stream
	}

	// 清理
	for _, stream := range streams {
		stream.Stop()
	}
}

// TestStreamFactory_ConcurrentCreation 测试并发创建流
func TestStreamFactory_ConcurrentCreation(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}

	const numGoroutines = 10
	streams := make([]*Stream, numGoroutines)
	errors := make([]error, numGoroutines)
	done := make(chan struct{})

	// 并发创建流
	for i := 0; i < numGoroutines; i++ {
		go func(index int) {
			stream, err := factory.CreateStream(config)
			streams[index] = stream
			errors[index] = err
			done <- struct{}{}
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// 验证结果
	for i := 0; i < numGoroutines; i++ {
		assert.NoError(t, errors[i])
		assert.NotNil(t, streams[i])
		if streams[i] != nil {
			streams[i].Stop()
		}
	}
}