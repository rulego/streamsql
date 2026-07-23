package stream

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/metrics"
	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/require"
)

// TestStrategyFactory
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

// TestStrategy_Constructor Test policy constructor
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

// TestBlockingStrategy_ProcessData Test blocking policy data processing
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

// TestExpansionStrategy_ProcessData Testing scaling strategy data processing
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

// TestDropStrategy_ProcessData Test the processing of discarding policy data
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

// TestStrategyInitialization
func TestStrategyInitialization(t *testing.T) {
	// Create test configurations
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

	// Create a Stream instance
	stream, err := NewStream(config)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Stop()

	// Verify that the policy is set correctly
	if stream.dataStrategy == nil {
		t.Fatal("Data strategy not set")
	}

	if stream.dataStrategy.GetStrategyName() != StrategyBlock {
		t.Errorf("Expected blocking strategy, got %s", stream.dataStrategy.GetStrategyName())
	}
}

// TestStrategyProcessData Processing of test strategy data
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
			// Create test configurations
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

			// Create a Stream instance
			stream, err := NewStream(config)
			if err != nil {
				t.Fatalf("Failed to create stream: %v", err)
			}
			defer stream.Stop()

			// Test data processing
			testData := map[string]any{
				"test": "data",
				"id":   1,
			}

			// The main test here is that the policy can be called normally without panic
			stream.Emit(testData)

			// Validation input count increases
			if stream.mInput.Value() != 1 {
				t.Errorf("Expected input count 1, got %d", stream.mInput.Value())
			}
		})
	}
}

// TestStrategyCleanup
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

	// Create a Stream instance
	stream, err := NewStream(config)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	// Verification strategies exist
	if stream.dataStrategy == nil {
		t.Fatal("Data strategy not set")
	}

	// Testing stops and cleans up
	stream.Stop()

	// Here, the main goal is to verify that the Stop method can execute normally without panic
	// The actual cleanup logic is implemented in the Cleanup methods of each policy
}

// MockStrategy is a simulation strategy used to test scalability
type MockStrategy struct {
	stream      *Stream
	processed   int
	initialized bool
	cleaned     bool
}

// NewMockStrategy creates a simulation strategy example
func NewMockStrategy() *MockStrategy {
	return &MockStrategy{}
}

// ProcessData simulates data processing
func (ms *MockStrategy) ProcessData(data map[string]any) {
	ms.processed++
	// Simulating processing logic
}

// GetStrategyName: Get the strategy name
func (ms *MockStrategy) GetStrategyName() string {
	return "mock"
}

// Init initializes the simulation strategy
func (ms *MockStrategy) Init(stream *Stream, config types.PerformanceConfig) error {
	ms.stream = stream
	ms.initialized = true
	return nil
}

// Stop and clear the simulation strategy resources
func (ms *MockStrategy) Stop() error {
	ms.cleaned = true
	return nil
}

// TestCustomStrategy Tests the scalability of custom strategies
func TestCustomStrategy(t *testing.T) {
	// Create custom policies
	mockStrategy := NewMockStrategy()

	// Create a basic Stream instance (without going through the factory)
	reg := metrics.NewRegistry()
	stream := &Stream{
		dataChan:        make(chan map[string]any, 10),
		done:            make(chan struct{}),
		metricsRegistry: reg,
		mInput:          reg.Counter(InputCount),
		mOutput:         reg.Counter(OutputCount),
		mInputDropped:   reg.Counter(InputDroppedCount),
		mOutputDropped:  reg.Counter(OutputDroppedCount),
	}

	// Manually set the strategy
	config := types.PerformanceConfig{}
	err := mockStrategy.Init(stream, config)
	if err != nil {
		t.Fatalf("Failed to init mock strategy: %v", err)
	}

	stream.dataStrategy = mockStrategy

	// Test strategy functionality
	if !mockStrategy.initialized {
		t.Error("Mock strategy not initialized")
	}

	if mockStrategy.GetStrategyName() != "mock" {
		t.Errorf("Expected strategy name 'mock', got %s", mockStrategy.GetStrategyName())
	}

	// Test data processing
	testData := map[string]any{"test": "data"}
	stream.Emit(testData)

	if mockStrategy.processed != 1 {
		t.Errorf("Expected processed count 1, got %d", mockStrategy.processed)
	}

	// Test cleanup
	stream.Stop()
	if !mockStrategy.cleaned {
		t.Error("Mock strategy not cleaned")
	}
}

// TestStrategyRegistration: Test the strategy registration mechanism
func TestStrategyRegistration(t *testing.T) {
	factory := NewStrategyFactory()

	// Test whether the built-in policy is registered
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

	// Test the registration custom policy
	factory.RegisterStrategy("kafka", func() DataProcessingStrategy {
		return &KafkaStrategy{}
	})

	// Verify that the custom policy is registered
	strategy, err := factory.CreateStrategy("kafka")
	if err != nil {
		t.Fatalf("Failed to create kafka strategy: %v", err)
	}

	if strategy.GetStrategyName() != "kafka" {
		t.Errorf("Expected strategy name 'kafka', got %s", strategy.GetStrategyName())
	}

	// Test the deregistration strategy
	factory.UnregisterStrategy("kafka")
	strategy, err = factory.CreateStrategy("kafka")
	if err != nil {
		t.Fatalf("Failed to create default strategy: %v", err)
	}
	// After logout, the default discarding policy should be restored
	if strategy.GetStrategyName() != StrategyDrop {
		t.Errorf("Expected default strategy '%s', got %s", StrategyDrop, strategy.GetStrategyName())
	}
}

// KafkaStrategy Kafka Peak Shaving Strategy Example
type KafkaStrategy struct {
	stream *Stream
}

// ProcessData implements Kafka peak shaving data processing
func (ks *KafkaStrategy) ProcessData(data map[string]any) {
	// Simulates Kafka peak-cutting logic
	// In actual implementations, data is sent to the Kafka queue
	logger.Debug("Data sent to Kafka for peak shaving")
}

// GetStrategyName: Get the strategy name
func (ks *KafkaStrategy) GetStrategyName() string {
	return "kafka"
}

// Init initializes the Kafka strategy
func (ks *KafkaStrategy) Init(stream *Stream, config types.PerformanceConfig) error {
	ks.stream = stream
	// Here, you can initialize Kafka connections, etc
	return nil
}

// Stop and clean up Kafka's strategic resources
func (ks *KafkaStrategy) Stop() error {
	// You can disable Kafka connections and similar connections here
	return nil
}
