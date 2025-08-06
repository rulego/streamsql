package stream

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/require"
)

// TestStrategyFactory 测试策略工厂
func TestStrategyFactory(t *testing.T) {
	factory := NewStrategyFactory()

	tests := []struct {
		name         string
		strategyName string
		expectedType string
	}{
		{
			name:         "Blocking Strategy",
			strategyName: StrategyBlock,
			expectedType: StrategyBlock,
		},
		{
			name:         "Expansion Strategy",
			strategyName: StrategyExpand,
			expectedType: StrategyExpand,
		},
		{
			name:         "Drop Strategy",
			strategyName: StrategyDrop,
			expectedType: StrategyDrop,
		},
		{
			name:         "Unknown Strategy (Default to Drop)",
			strategyName: "unknown",
			expectedType: StrategyDrop,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy, err := factory.CreateStrategy(tt.strategyName)
			if err != nil {
				t.Fatalf("Failed to create strategy: %v", err)
			}

			if strategy.GetStrategyName() != tt.expectedType {
				t.Errorf("Expected strategy name %s, got %s", tt.expectedType, strategy.GetStrategyName())
			}
		})
	}
}

// TestStrategy_Constructor 测试策略构造函数
func TestStrategy_Constructor(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()
}

// TestBlockingStrategy_ProcessData 测试阻塞策略数据处理
func TestBlockingStrategy_ProcessData(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()
}

// TestExpansionStrategy_ProcessData 测试扩容策略数据处理
func TestExpansionStrategy_ProcessData(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()
}

// TestPersistenceStrategy_ProcessData 测试持久化策略数据处理


// TestDropStrategy_ProcessData 测试丢弃策略数据处理
func TestDropStrategy_ProcessData(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()
}

// TestStrategyInitialization 测试策略初始化
func TestStrategyInitialization(t *testing.T) {
	// 创建测试配置
	config := types.Config{
		PerformanceConfig: types.PerformanceConfig{
			BufferConfig: types.BufferConfig{
				DataChannelSize:   100,
				ResultChannelSize: 50,
			},
			OverflowConfig: types.OverflowConfig{
				Strategy:      StrategyBlock,
				AllowDataLoss: false,
				BlockTimeout:  time.Second * 5,
			},
			WorkerConfig: types.WorkerConfig{
				SinkPoolSize:     10,
				SinkWorkerCount:  3,
				MaxRetryRoutines: 5,
			},
		},
	}

	// 创建Stream实例
	stream, err := NewStream(config)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Stop()

	// 验证策略是否正确设置
	if stream.dataStrategy == nil {
		t.Fatal("Data strategy not set")
	}

	if stream.dataStrategy.GetStrategyName() != StrategyBlock {
		t.Errorf("Expected blocking strategy, got %s", stream.dataStrategy.GetStrategyName())
	}
}

// TestStrategyProcessData 测试策略数据处理
func TestStrategyProcessData(t *testing.T) {
	tests := []struct {
		name     string
		strategy string
	}{
		{"Blocking Strategy", StrategyBlock},
		{"Expansion Strategy", StrategyExpand},
		{"Drop Strategy", StrategyDrop},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建测试配置
			config := types.Config{
				PerformanceConfig: types.PerformanceConfig{
					BufferConfig: types.BufferConfig{
						DataChannelSize:   10,
						ResultChannelSize: 5,
					},
					OverflowConfig: types.OverflowConfig{
						Strategy:      tt.strategy,
						AllowDataLoss: true,
						BlockTimeout:  time.Millisecond * 100,
					},
					WorkerConfig: types.WorkerConfig{
						SinkPoolSize:     5,
						SinkWorkerCount:  2,
						MaxRetryRoutines: 3,
					},
				},
			}

			// 创建Stream实例
			stream, err := NewStream(config)
			if err != nil {
				t.Fatalf("Failed to create stream: %v", err)
			}
			defer stream.Stop()

			// 测试数据处理
			testData := map[string]interface{}{
				"test": "data",
				"id":   1,
			}

			// 这里主要测试策略能够正常调用，不会panic
			stream.Emit(testData)

			// 验证输入计数增加
			if stream.inputCount != 1 {
				t.Errorf("Expected input count 1, got %d", stream.inputCount)
			}
		})
	}
}

// TestStrategyCleanup 测试策略清理
func TestStrategyCleanup(t *testing.T) {
	config := types.Config{
		PerformanceConfig: types.PerformanceConfig{
			BufferConfig: types.BufferConfig{
				DataChannelSize:   10,
				ResultChannelSize: 5,
			},
			OverflowConfig: types.OverflowConfig{
				Strategy:      StrategyBlock,
				AllowDataLoss: false,
				BlockTimeout:  time.Second,
			},
			WorkerConfig: types.WorkerConfig{
				SinkPoolSize:     5,
				SinkWorkerCount:  2,
				MaxRetryRoutines: 3,
			},
		},
	}

	// 创建Stream实例
	stream, err := NewStream(config)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	// 验证策略存在
	if stream.dataStrategy == nil {
		t.Fatal("Data strategy not set")
	}

	// 测试停止和清理
	stream.Stop()

	// 这里主要验证Stop方法能够正常执行，不会panic
	// 实际的清理逻辑在各个策略的Cleanup方法中实现
}

// MockStrategy 模拟策略，用于测试扩展性
type MockStrategy struct {
	stream      *Stream
	processed   int
	initialized bool
	cleaned     bool
}

// NewMockStrategy 创建模拟策略实例
func NewMockStrategy() *MockStrategy {
	return &MockStrategy{}
}

// ProcessData 模拟数据处理
func (ms *MockStrategy) ProcessData(data map[string]interface{}) {
	ms.processed++
	// 模拟处理逻辑
}

// GetStrategyName 获取策略名称
func (ms *MockStrategy) GetStrategyName() string {
	return "mock"
}

// Init 初始化模拟策略
func (ms *MockStrategy) Init(stream *Stream, config types.PerformanceConfig) error {
	ms.stream = stream
	ms.initialized = true
	return nil
}

// Stop 停止并清理模拟策略资源
func (ms *MockStrategy) Stop() error {
	ms.cleaned = true
	return nil
}

// TestCustomStrategy 测试自定义策略的扩展性
func TestCustomStrategy(t *testing.T) {
	// 创建自定义策略
	mockStrategy := NewMockStrategy()

	// 创建基本的Stream实例（不通过工厂）
	stream := &Stream{
		dataChan:    make(chan map[string]interface{}, 10),
		done:        make(chan struct{}),
		inputCount:  0,
		outputCount: 0,
	}

	// 手动设置策略
	config := types.PerformanceConfig{}
	err := mockStrategy.Init(stream, config)
	if err != nil {
		t.Fatalf("Failed to init mock strategy: %v", err)
	}

	stream.dataStrategy = mockStrategy

	// 测试策略功能
	if !mockStrategy.initialized {
		t.Error("Mock strategy not initialized")
	}

	if mockStrategy.GetStrategyName() != "mock" {
		t.Errorf("Expected strategy name 'mock', got %s", mockStrategy.GetStrategyName())
	}

	// 测试数据处理
	testData := map[string]interface{}{"test": "data"}
	stream.Emit(testData)

	if mockStrategy.processed != 1 {
		t.Errorf("Expected processed count 1, got %d", mockStrategy.processed)
	}

	// 测试清理
	stream.Stop()
	if !mockStrategy.cleaned {
		t.Error("Mock strategy not cleaned")
	}
}

// TestStrategyRegistration 测试策略注册机制
func TestStrategyRegistration(t *testing.T) {
	factory := NewStrategyFactory()

	// 测试内置策略是否已注册
	registeredStrategies := factory.GetRegisteredStrategies()
	expectedStrategies := []string{StrategyBlock, StrategyExpand, StrategyDrop}

	for _, expected := range expectedStrategies {
		found := false
		for _, registered := range registeredStrategies {
			if registered == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected strategy %s to be registered", expected)
		}
	}

	// 测试注册自定义策略
	factory.RegisterStrategy("kafka", func() DataProcessingStrategy {
		return &KafkaStrategy{}
	})

	// 验证自定义策略已注册
	strategy, err := factory.CreateStrategy("kafka")
	if err != nil {
		t.Fatalf("Failed to create kafka strategy: %v", err)
	}

	if strategy.GetStrategyName() != "kafka" {
		t.Errorf("Expected strategy name 'kafka', got %s", strategy.GetStrategyName())
	}

	// 测试注销策略
	factory.UnregisterStrategy("kafka")
	strategy, err = factory.CreateStrategy("kafka")
	if err != nil {
		t.Fatalf("Failed to create default strategy: %v", err)
	}
	// 注销后应该返回默认的丢弃策略
	if strategy.GetStrategyName() != StrategyDrop {
		t.Errorf("Expected default strategy '%s', got %s", StrategyDrop, strategy.GetStrategyName())
	}
}

// KafkaStrategy Kafka削峰策略示例
type KafkaStrategy struct {
	stream *Stream
}

// ProcessData 实现Kafka削峰数据处理
func (ks *KafkaStrategy) ProcessData(data map[string]interface{}) {
	// 模拟Kafka削峰逻辑
	// 实际实现中会将数据发送到Kafka队列
	logger.Debug("Data sent to Kafka for peak shaving")
}

// GetStrategyName 获取策略名称
func (ks *KafkaStrategy) GetStrategyName() string {
	return "kafka"
}

// Init 初始化Kafka策略
func (ks *KafkaStrategy) Init(stream *Stream, config types.PerformanceConfig) error {
	ks.stream = stream
	// 这里可以初始化Kafka连接等
	return nil
}

// Stop 停止并清理Kafka策略资源
func (ks *KafkaStrategy) Stop() error {
	// 这里可以关闭Kafka连接等
	return nil
}
