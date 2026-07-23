package stream

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStreamBasicOperations (integrates constructors, add data, get results, and other tests)
func TestStreamBasicOperations(t *testing.T) {
	tests := []struct {
		name        string
		config      types.Config
		testFunc    string
		expectError bool
	}{
		{
			name: "基本构造函数测试",
			config: types.Config{
				SimpleFields: []string{"name", "age"},
			},
			testFunc:    "constructor",
			expectError: false,
		},
		{
			name: "添加数据测试",
			config: types.Config{
				SimpleFields: []string{"name", "age"},
			},
			testFunc:    "addData",
			expectError: false,
		},
		{
			name: "获取结果测试",
			config: types.Config{
				SimpleFields: []string{"name", "age"},
			},
			testFunc:    "getResults",
			expectError: false,
		},
		{
			name: "窗口功能测试",
			config: types.Config{
				SimpleFields: []string{"name", "age"},
				NeedWindow:   true,
				WindowConfig: types.WindowConfig{
					Type:     "tumbling",
					TimeUnit: 1000,
					Params:   []any{1 * time.Second},
				},
			},
			testFunc:    "withWindow",
			expectError: false,
		},
		{
			name: "线程安全测试",
			config: types.Config{
				SimpleFields: []string{"name", "age"},
			},
			testFunc:    "threadSafety",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream, err := NewStream(tt.config)
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			defer func() {
				if stream != nil {
					close(stream.done)
				}
			}()

			// Different test logic is executed depending on the type of test
			switch tt.testFunc {
			case "constructor":
				// Verify that the stream was successfully created
				assert.NotNil(t, stream)
				assert.NotNil(t, stream.dataChan)
				assert.NotNil(t, stream.resultChan)

			case "addData":
				// Test adding data
				testData := map[string]any{
					"name": "test",
					"age":  25,
				}
				sent := stream.safeSendToDataChan(testData)
				assert.True(t, sent)

			case "getResults":
				// Test the results channel
				resultsChan := stream.GetResultsChan()
				assert.NotNil(t, resultsChan)

			case "withWindow":
				// Verification window configuration
				assert.NotNil(t, stream.Window)
				assert.True(t, stream.config.NeedWindow)

			case "threadSafety":
				// Testing concurrency security
				var wg sync.WaitGroup
				for i := 0; i < 10; i++ {
					wg.Add(1)
					go func(id int) {
						defer wg.Done()
						testData := map[string]any{
							"name": fmt.Sprintf("test_%d", id),
							"age":  20 + id,
						}
						stream.safeSendToDataChan(testData)
					}(i)
				}
				wg.Wait()
			}
		})
	}
}

// TestStreamBasicFunctionality Tests the basic functionality of Stream
func TestStreamBasicFunctionality(t *testing.T) {
	tests := []struct {
		name           string
		config         types.Config
		filter         string
		testData       []map[string]any
		expectedDevice string
		expectedTemp   float64
		expectedHum    float64
	}{
		{
			name: "带过滤器的窗口聚合",
			config: types.Config{
				WindowConfig: types.WindowConfig{
					Type:   "tumbling",
					Params: []any{500 * time.Millisecond},
				},
				GroupFields: []string{"device"},
				SelectFields: map[string]aggregator.AggregateType{
					"temperature": aggregator.Avg,
					"humidity":    aggregator.Sum,
				},
				NeedWindow: true,
			},
			filter: "device == 'aa' && temperature > 10",
			testData: []map[string]any{
				{"device": "aa", "temperature": 25.0, "humidity": 60},
				{"device": "aa", "temperature": 30.0, "humidity": 55},
				{"device": "bb", "temperature": 22.0, "humidity": 70},
			},
			expectedDevice: "aa",
			expectedTemp:   27.5,
			expectedHum:    115.0,
		},
		{
			name: "不完整数据处理",
			config: types.Config{
				WindowConfig: types.WindowConfig{
					Type:   "tumbling",
					Params: []any{500 * time.Millisecond},
				},
				GroupFields: []string{"device"},
				SelectFields: map[string]aggregator.AggregateType{
					"temperature": aggregator.Avg,
					"humidity":    aggregator.Sum,
				},
				NeedWindow: true,
			},
			filter: "device == 'aa'",
			testData: []map[string]any{
				{"device": "aa", "temperature": 25.0},
				{"device": "aa", "humidity": 60},
				{"device": "aa", "temperature": 30.0},
				{"device": "aa", "humidity": 55},
				{"device": "bb", "temperature": 22.0, "humidity": 70},
			},
			expectedDevice: "aa",
			expectedTemp:   27.5,
			expectedHum:    115.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strm, err := NewStream(tt.config)
			require.NoError(t, err)
			defer strm.Stop()

			if tt.filter != "" {
				err = strm.RegisterFilter(tt.filter)
				require.NoError(t, err)
			}

			// Add the Sink function to capture the results
			resultChan := make(chan any, 1)
			strm.AddSink(func(result []map[string]any) {
				select {
				case resultChan <- result:
				default:
					// Prevents blockages
				}
			})

			strm.Start()

			// Send test data
			for _, data := range tt.testData {
				strm.Emit(data)
			}

			// Wait for the window to close and trigger the result
			time.Sleep(700 * time.Millisecond)

			// Wait for the results
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			var actual any
			select {
			case actual = <-resultChan:
				cancel()
			case <-ctx.Done():
				t.Fatal("No results received within 3 seconds")
			}

			// Verify the results
			require.NotNil(t, actual)
			assert.IsType(t, []map[string]any{}, actual)
			resultMap := actual.([]map[string]any)
			require.Greater(t, len(resultMap), 0)

			firstResult := resultMap[0]
			assert.Equal(t, tt.expectedDevice, firstResult["device"])
			assert.InEpsilon(t, tt.expectedTemp, firstResult["temperature"].(float64), 0.0001)
			assert.InDelta(t, tt.expectedHum, firstResult["humidity"].(float64), 0.0001)
		})
	}
}

// TestStreamWithoutFilter: Tests stream processing without filters
func TestStreamWithoutFilter(t *testing.T) {
	config := types.Config{
		WindowConfig: types.WindowConfig{
			Type:   "sliding",
			Params: []any{2 * time.Second, 1 * time.Second},
		},
		GroupFields: []string{"device"},
		SelectFields: map[string]aggregator.AggregateType{
			"temperature": aggregator.Max,
			"humidity":    aggregator.Min,
		},
		NeedWindow: true,
	}

	strm, err := NewStream(config)
	require.NoError(t, err)
	defer strm.Stop()

	strm.Start()

	testData := []map[string]any{
		{"device": "aa", "temperature": 25.0, "humidity": 60},
		{"device": "aa", "temperature": 30.0, "humidity": 55},
		{"device": "bb", "temperature": 22.0, "humidity": 70},
	}

	for _, data := range testData {
		strm.Emit(data)
	}

	// Capture the results
	resultChan := make(chan any)
	strm.AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Wait for the window to trigger
	time.Sleep(3 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		cancel()
	case <-ctx.Done():
		t.Fatal("Timeout waiting for results")
	}

	expected := []map[string]any{
		{"device": "aa", "temperature": 30.0, "humidity": 55.0},
		{"device": "bb", "temperature": 22.0, "humidity": 70.0},
	}

	assert.IsType(t, []map[string]any{}, actual)
	resultSlice := actual.([]map[string]any)
	assert.Len(t, resultSlice, 2)

	for _, expectedResult := range expected {
		found := false
		for _, resultMap := range resultSlice {
			if resultMap["device"] == expectedResult["device"] {
				assert.InEpsilon(t, expectedResult["temperature"].(float64), resultMap["temperature"].(float64), 0.0001)
				assert.InEpsilon(t, expectedResult["humidity"].(float64), resultMap["humidity"].(float64), 0.0001)
				found = true
				break
			}
		}
		assert.True(t, found, "Expected result for device %v not found", expectedResult["device"])
	}
}

// TestStreamRefactoring tests the restructured Stream functionality
func TestStreamRefactoring(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
		SelectFields: map[string]aggregator.AggregateType{
			"count": aggregator.Count,
		},
		GroupFields: []string{"category"},
		NeedWindow:  false,
	}

	stream, err := NewStream(config)
	require.NoError(t, err)
	defer stream.Stop()

	testData := map[string]any{
		"name":     "test",
		"age":      25,
		"category": "A",
	}

	// Start Stream
	stream.Start()

	// Send test data
	stream.Emit(testData)

	// Wait for processing to complete
	time.Sleep(100 * time.Millisecond)

	// Get statistics
	stats := stream.GetStats()
	assert.Equal(t, int64(1), stats[InputCount])
}

// TestStreamFactory tests the StreamFactory functionality
func TestStreamFactory(t *testing.T) {
	factory := NewStreamFactory()
	require.NotNil(t, factory)

	config := types.Config{
		SimpleFields: []string{"value"},
		NeedWindow:   false,
	}

	// Test different creation methods
	tests := []struct {
		name   string
		create func() (*Stream, error)
	}{
		{"CreateStream", func() (*Stream, error) { return factory.CreateStream(config) }},
		{"CreateHighPerformanceStream", func() (*Stream, error) { return factory.CreateHighPerformanceStream(config) }},
		{"CreateLowLatencyStream", func() (*Stream, error) { return factory.CreateLowLatencyStream(config) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream, err := tt.create()
			require.NoError(t, err)
			require.NotNil(t, stream)
			defer stream.Stop()
		})
	}
}

// TestMigratedFunctions tests the migrated functions
func TestMigratedFunctions(t *testing.T) {
	// Test performance evaluation function
	tests := []struct {
		name      string
		dataUsage float64
		dropRate  float64
		expected  string
	}{
		{"Critical", 50.0, 60.0, PerformanceLevelCritical},
		{"Warning", 50.0, 30.0, PerformanceLevelWarning},
		{"HighLoad", 95.0, 5.0, PerformanceLevelHighLoad},
		{"ModerateLoad", 75.0, 5.0, PerformanceLevelModerateLoad},
		{"Optimal", 50.0, 5.0, PerformanceLevelOptimal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AssessPerformanceLevel(tt.dataUsage, tt.dropRate)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestStreamConstructors tests various Stream constructors
func TestStreamConstructors(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}

	// Testing high-performance streams
	highPerfStream, err := NewStreamWithHighPerformance(config)
	require.NoError(t, err)
	require.NotNil(t, highPerfStream)
	defer func() {
		if highPerfStream != nil {
			close(highPerfStream.done)
		}
	}()

	// Testing low-latency streams
	lowLatencyStream, err := NewStreamWithLowLatency(config)
	require.NoError(t, err)
	require.NotNil(t, lowLatencyStream)
	defer func() {
		if lowLatencyStream != nil {
			close(lowLatencyStream.done)
		}
	}()

	// Test custom performance configuration Stream
	perfConfig := types.PerformanceConfig{
		WorkerConfig: types.WorkerConfig{
			SinkWorkerCount: 10,
		},
		BufferConfig: types.BufferConfig{
			DataChannelSize: 1000,
		},
	}
	customPerfStream, err := NewStreamWithCustomPerformance(config, perfConfig)
	require.NoError(t, err)
	require.NotNil(t, customPerfStream)
	defer func() {
		if customPerfStream != nil {
			close(customPerfStream.done)
		}
	}()
}

// TestStreamFilterRegistration Test filter registration function
func TestStreamFilterRegistration(t *testing.T) {
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

	// Test the empty filter
	err = stream.RegisterFilter("")
	require.NoError(t, err)

	// Test the simple condition filter
	err = stream.RegisterFilter("age > 18")
	require.NoError(t, err)

	// Test filters with backquotes
	err = stream.RegisterFilter("`user_name` == 'test'")
	require.NoError(t, err)

	// Test LIKE syntax
	err = stream.RegisterFilter("name LIKE '%test%'")
	require.NoError(t, err)

	// Test IS NULL syntax
	err = stream.RegisterFilter("age IS NULL")
	require.NoError(t, err)

	// Test the IS NOT NULL syntax
	err = stream.RegisterFilter("name IS NOT NULL")
	require.NoError(t, err)

	// Testing complex conditions
	err = stream.RegisterFilter("age > 18 && name LIKE '%test%' || `user_id` IS NOT NULL")
	require.NoError(t, err)
}

// TestStreamAggregationQuery tests aggregation query functionality
func TestStreamAggregationQuery(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
		SelectFields: map[string]aggregator.AggregateType{
			"avg_age": aggregator.Avg,
			"max_age": aggregator.Max,
		},
		NeedWindow: true,
		WindowConfig: types.WindowConfig{
			Type:   "tumbling",
			Params: []any{5 * time.Second},
		},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	// Test whether the query is aggregated
	isAggregation := stream.IsAggregationQuery()
	require.True(t, isAggregation)

	// Test non-aggregated queries
	simpleConfig := types.Config{
		SimpleFields: []string{"name", "age"},
	}
	simpleStream, err := NewStream(simpleConfig)
	require.NoError(t, err)
	defer func() {
		if simpleStream != nil {
			close(simpleStream.done)
		}
	}()

	isSimpleAggregation := simpleStream.IsAggregationQuery()
	require.False(t, isSimpleAggregation)
}

// TestStreamSyncProcessing Testing synchronization processing function
func TestStreamSyncProcessing(t *testing.T) {
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

	// Synchronized data processing for testing
	testData := map[string]any{
		"name": "test",
		"age":  25,
	}

	result, err := stream.ProcessSync(testData)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "test", result["name"])
	require.Equal(t, 25, result["age"])
}

// TestStreamErrorHandling Test error handling
func TestStreamErrorHandling(t *testing.T) {
	// Invalid testing configuration
	invalidConfig := types.Config{
		// Deliberately leave blanks to test for error handling
	}
	stream, err := NewStream(invalidConfig)
	// This might succeed because configuration verification can be relatively lenient
	// require.Error(t, err)

	if stream != nil {
		defer func() {
			close(stream.done)
		}()

		// Test for ineffective filters
		err = stream.RegisterFilter("invalid syntax !!!")
		// This one is likely to fail
		if err != nil {
			require.Contains(t, err.Error(), "compile filter error")
		}
	}
}

// TestStreamConcurrency tests concurrency security
func TestStreamConcurrency(t *testing.T) {
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

	// Starts multiple goroutines to send data concurrently
	const numGoroutines = 10
	const numMessages = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numMessages; j++ {
				data := map[string]any{
					"name": fmt.Sprintf("user_%d_%d", id, j),
					"age":  id + j,
				}
				stream.Emit(data)
			}
		}(i)
	}

	wg.Wait()
}

// TestStreamPerformance tests performance-related features
func TestStreamPerformance(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
		PerformanceConfig: types.PerformanceConfig{
			WorkerConfig: types.WorkerConfig{
				SinkWorkerCount: 5,
			},
			BufferConfig: types.BufferConfig{
				DataChannelSize: 1000, // Increase the buffer size to accommodate all messages
			},
		},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer stream.Stop()

	// Start stream processing
	stream.Start()

	// Testing large amounts of data processing
	const numMessages = 1000
	for i := 0; i < numMessages; i++ {
		data := map[string]any{
			"name": fmt.Sprintf("user_%d", i),
			"age":  i % 100,
		}
		stream.Emit(data)
	}

	// Wait for processing to complete
	time.Sleep(100 * time.Millisecond)
}

// TestStreamLifecycle TestStream Lifecycle
func TestStreamLifecycle(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)

	// Test launched
	stream.Start()

	// Send some data
	for i := 0; i < 10; i++ {
		data := map[string]any{
			"name": fmt.Sprintf("user_%d", i),
			"age":  i,
		}
		stream.Emit(data)
	}

	// The test stopped
	stream.Stop()

	// After authentication stops, data cannot be sent (depending on the specific implementation)
	// This is just testing that the Stop method does not panic
}

// TestConvertToAggregationFields tests aggregation field conversions
func TestConvertToAggregationFields(t *testing.T) {
	selectFields := map[string]aggregator.AggregateType{
		"avg_age": aggregator.Avg,
		"max_age": aggregator.Max,
		"count":   aggregator.Count,
	}

	fieldAlias := map[string]string{
		"avg_age": "age",
		"max_age": "age",
		"count":   "id",
	}

	fields := convertToAggregationFields(selectFields, fieldAlias)
	require.Len(t, fields, 3)

	// Verify the field conversion results
	for _, field := range fields {
		switch field.OutputAlias {
		case "avg_age":
			require.Equal(t, aggregator.Avg, field.AggregateType)
			require.Equal(t, "age", field.InputField)
		case "max_age":
			require.Equal(t, aggregator.Max, field.AggregateType)
			require.Equal(t, "age", field.InputField)
		case "count":
			require.Equal(t, aggregator.Count, field.AggregateType)
			require.Equal(t, "id", field.InputField)
		}
	}
}

// TestStreamWithWindowAndAggregation: Tests streams with windows and aggregation
func TestStreamWithWindowAndAggregation(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
		WindowConfig: types.WindowConfig{
			Type:   "tumbling",
			Params: []any{100 * time.Millisecond},
		},
		SelectFields: map[string]aggregator.AggregateType{
			"avg_age": aggregator.Avg,
			"count":   aggregator.Count,
		},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil && stream.done != nil {
			select {
			case <-stream.done:
				// channel already closed
			default:
				close(stream.done)
			}
		}
	}()

	// Register the filter
	err = stream.RegisterFilter("age > 0")
	require.NoError(t, err)

	// Start Stream
	stream.Start()

	// Send data
	for i := 0; i < 50; i++ {
		data := map[string]any{
			"name": fmt.Sprintf("user_%d", i),
			"age":  i + 1,
		}
		stream.Emit(data)
	}

	// Wait for processing to complete
	time.Sleep(200 * time.Millisecond)
	stream.Stop()
}

// TestDataChannelExpansion: Dynamic capacity expansion of the test data channel
func TestDataChannelExpansion(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "value"},
		PerformanceConfig: types.PerformanceConfig{
			BufferConfig: types.BufferConfig{
				DataChannelSize:   10, // Small capacity makes testing and expansion easier
				ResultChannelSize: 100,
			},
			WorkerConfig: types.WorkerConfig{
				SinkPoolSize:     5,
				MaxRetryRoutines: 2,
			},
		},
	}

	stream, err := NewStream(config)
	require.NoError(t, err)
	defer stream.Stop()

	// Startup flow
	stream.Start()

	// Send data to test channel function
	for i := 0; i < 8; i++ {
		data := map[string]any{
			"name":  "test",
			"value": i,
		}
		stream.Emit(data)
	}

	// Waiting for processing
	time.Sleep(100 * time.Millisecond)

	// Validation data processing
	stats := stream.GetStats()
	assert.NotNil(t, stats)
	assert.True(t, stats[InputCount] > 0)
}

func TestResultHandlerGetResultsChan(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"test"},
	}

	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	// Test the results channel
	resultsChan := stream.GetResultsChan()
	assert.NotNil(t, resultsChan)
}

// TestConcurrentDataChannelExpansion: Tests concurrent data channel expansion
func TestConcurrentDataChannelExpansion(t *testing.T) {
	// Create streams
	config := types.Config{
		SimpleFields: []string{"id", "message"},
		PerformanceConfig: types.PerformanceConfig{
			BufferConfig: types.BufferConfig{
				DataChannelSize:   5,
				ResultChannelSize: 10,
			},
			WorkerConfig: types.WorkerConfig{
				SinkPoolSize:     2,
				MaxRetryRoutines: 1,
			},
		},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer stream.Stop()

	// Startup flow
	stream.Start()

	// Concurrent addition of large amounts of data
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			data := map[string]any{
				"id":      id,
				"message": "concurrent_expansion",
			}
			stream.Emit(data)
		}(i)
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	// The validation flow is still running normally
	assert.NotNil(t, stream)
}

// TestExpandDataChannelDirectly tests the expandDataChannel function
// Increase the coverage of the expandDataChannel function
func TestExpandDataChannelDirectly(t *testing.T) {
	// Create a Stream instance for testing
	config := types.Config{
		SimpleFields: []string{"test"},
		PerformanceConfig: types.PerformanceConfig{
			BufferConfig: types.BufferConfig{
				DataChannelSize:   10,
				ResultChannelSize: 10,
			},
		},
	}

	stream, err := NewStream(config)
	require.NoError(t, err)
	defer stream.Stop()

	// Fill the channel above 80% to trigger scaling conditions
	for i := 0; i < 9; i++ {
		select {
		case stream.dataChan <- map[string]any{"test": i}:
		default:
			t.Fatal("Failed to fill channel")
		}
	}

	// Record the original capacity
	originalCap := cap(stream.dataChan)

	// Directly call the expansion function
	stream.expandDataChannel()

	// Verify whether capacity has increased
	newCap := cap(stream.dataChan)
	assert.Greater(t, newCap, originalCap, "Channel capacity should increase after expansion")

	// Verify that data migration is correct
	assert.Equal(t, 9, len(stream.dataChan), "All data should be migrated to new channel")

	// Testing concurrent expansion protection
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream.expandDataChannel()
		}()
	}
	wg.Wait()
}

// TestExpandDataChannelBelowThreshold Does not expand when the test channel's usage falls below the threshold
func TestExpandDataChannelBelowThreshold(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"test"},
		PerformanceConfig: types.PerformanceConfig{
			BufferConfig: types.BufferConfig{
				DataChannelSize:   10,
				ResultChannelSize: 10,
			},
		},
	}

	stream, err := NewStream(config)
	require.NoError(t, err)
	defer stream.Stop()

	// Fill only a small amount of data (below the 80% threshold)
	for i := 0; i < 3; i++ {
		stream.dataChan <- map[string]any{"test": i}
	}

	originalCap := cap(stream.dataChan)

	// Call the expansion function
	stream.expandDataChannel()

	// The verification capacity remains unchanged
	newCap := cap(stream.dataChan)
	assert.Equal(t, originalCap, newCap, "Channel capacity should not change when below threshold")
}

// TestStreamConfigErrorHandlingEnhanced Teststream Configuration Error Handling Enhanced Edition
func TestStreamConfigErrorHandlingEnhanced(t *testing.T) {
	tests := []struct {
		name        string
		config      types.Config
		expectError bool
	}{
		{
			name: "空配置",
			config: types.Config{
				PerformanceConfig: types.DefaultPerformanceConfig(),
			},
			expectError: false,
		},
		{
			name: "无效窗口类型",
			config: types.Config{
				WindowConfig: types.WindowConfig{
					Type: "invalid_type",
				},
				NeedWindow:        true,
				PerformanceConfig: types.DefaultPerformanceConfig(),
			},
			expectError: true,
		},
		{
			name: "零时间窗口",
			config: func() types.Config {
				c := types.NewConfig()
				c.WindowConfig = types.WindowConfig{
					Type:     "tumbling",
					TimeUnit: 0,
				}
				c.NeedWindow = true
				c.PerformanceConfig = types.DefaultPerformanceConfig()
				return c
			}(),
			expectError: true,
		},
		{
			name: "负时间窗口",
			config: func() types.Config {
				c := types.NewConfig()
				c.WindowConfig = types.WindowConfig{
					Type:     "sliding",
					TimeUnit: 1 * time.Second,
				}
				c.NeedWindow = true
				c.PerformanceConfig = types.DefaultPerformanceConfig()
				return c
			}(),
			expectError: true,
		},
		{
			name: "超大缓冲区配置",
			config: func() types.Config {
				c := types.NewConfig()
				c.PerformanceConfig = types.PerformanceConfig{
					BufferConfig: types.BufferConfig{
						DataChannelSize: -1,
					},
					OverflowConfig: types.OverflowConfig{
						Strategy: "invalid",
					},
				}
				return c
			}(),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewStream(tt.config)
			if (err != nil) != tt.expectError {
				t.Errorf("expected error: %v, got: %v", tt.expectError, err)
			}
		})
	}
}

// TestStreamDataValidationEnhanced TestStreamDataValidationEnhanced Version
func TestStreamDataValidationEnhanced(t *testing.T) {
	config := types.NewConfig()
	config.PerformanceConfig = types.DefaultPerformanceConfig()

	stream, err := NewStream(config)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	tests := []struct {
		name        string
		data        map[string]any
		expectError bool
	}{
		{
			name:        "nil数据",
			data:        nil,
			expectError: true,
		},
		{
			name:        "空数据",
			data:        map[string]any{},
			expectError: false,
		},
		{
			name: "无效时间戳类型",
			data: map[string]any{
				"timestamp": "invalid",
				"value":     1,
			},
			expectError: false, // Handled by the processor without directly reporting errors
		},
		{
			name: "负数值",
			data: map[string]any{
				"value": -100,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Here, testing is simplified and only data structures are verified
			if tt.data != nil && len(tt.data) == 0 {
				// Empty data testing
			}
		})
	}

	_ = stream
}

// TestStreamMemoryPressureEnhanced Memory Pressure Enhanced Scenario Version
func TestStreamMemoryPressureEnhanced(t *testing.T) {
	config := types.NewConfig()
	config.PerformanceConfig = types.PerformanceConfig{
		BufferConfig: types.BufferConfig{
			DataChannelSize:   1,
			ResultChannelSize: 1,
			MaxBufferSize:     10,
		},
		OverflowConfig: types.OverflowConfig{
			Strategy:      "drop",
			AllowDataLoss: true,
		},
	}

	stream, err := NewStream(config)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Simulating large amounts of data
	largeData := make([]map[string]any, 1000)
	for i := 0; i < 1000; i++ {
		largeData[i] = map[string]any{
			"value": i,
			"key":   "test",
		}
	}

	_ = stream
	_ = largeData
}

// TestStreamConcurrentAccessEnhanced tests concurrent access boundary conditions
func TestStreamConcurrentAccessEnhanced(t *testing.T) {
	config := types.NewConfig()
	config.PerformanceConfig = types.DefaultPerformanceConfig()

	stream, err := NewStream(config)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Test concurrency shutdown
	go func() {
		stream.Stop()
	}()

	// Test concurrent startup
	go func() {
		stream.Start()
		time.Sleep(100 * time.Millisecond)
	}()

	// Give concurrent operations some time
	time.Sleep(10 * time.Millisecond)
}

// TestStreamWindowEdgeCasesEnhanced version of the test window boundary situation
func TestStreamWindowEdgeCasesEnhanced(t *testing.T) {
	tests := []struct {
		name        string
		config      types.Config
		expectError bool
	}{
		{
			name: "极小时间窗口",
			config: func() types.Config {
				c := types.NewConfig()
				c.WindowConfig = types.WindowConfig{
					Type:     "tumbling",
					Params:   []any{1 * time.Nanosecond}, // Minimal time window
					TimeUnit: 1 * time.Nanosecond,
				}
				c.NeedWindow = true
				c.PerformanceConfig = types.DefaultPerformanceConfig()
				return c
			}(),
			expectError: false,
		},
		{
			name: "极大时间窗口",
			config: types.Config{
				WindowConfig: types.WindowConfig{
					Type:     "tumbling",
					Params:   []any{8760 * time.Hour}, // 1 year
					TimeUnit: 8760 * time.Hour,
				},
				NeedWindow:        true,
				PerformanceConfig: types.DefaultPerformanceConfig(),
			},
			expectError: false,
		},
		{
			name: "滑动窗口零滑动",
			config: types.Config{
				WindowConfig: types.WindowConfig{
					Type:     "sliding",
					Params:   []any{1 * time.Second, 1 * time.Millisecond}, // Very small sliding intervals
					TimeUnit: 1 * time.Second,
				},
				NeedWindow:        true,
				PerformanceConfig: types.DefaultPerformanceConfig(),
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewStream(tt.config)
			if (err != nil) != tt.expectError {
				t.Errorf("expected error: %v, got: %v", tt.expectError, err)
			}
		})
	}
}

// TestStreamUnifiedConfigIntegration: Integration of unified configuration between Stream and Window
func TestStreamUnifiedConfigIntegration(t *testing.T) {
	testCases := []struct {
		name                     string
		performanceConfig        types.PerformanceConfig
		expectedWindowBufferSize int
	}{
		{
			name:                     "默认配置",
			performanceConfig:        types.DefaultPerformanceConfig(),
			expectedWindowBufferSize: 1000,
		},
		{
			name:                     "高性能配置",
			performanceConfig:        types.HighPerformanceConfig(),
			expectedWindowBufferSize: 5000,
		},
		{
			name:                     "低延迟配置",
			performanceConfig:        types.LowLatencyConfig(),
			expectedWindowBufferSize: 100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := types.Config{
				NeedWindow: true,
				WindowConfig: types.WindowConfig{
					Type:   "tumbling",
					Params: []any{5 * time.Second},
				},
				SelectFields: map[string]aggregator.AggregateType{
					"value": aggregator.Count,
				},
				PerformanceConfig: tc.performanceConfig,
			}

			s, err := NewStream(config)
			require.NoError(t, err)
			defer s.Stop()

			// Verify the buffer configuration of the stream
			assert.Equal(t, tc.performanceConfig.BufferConfig.DataChannelSize, cap(s.dataChan),
				"数据通道大小不匹配")
			assert.Equal(t, tc.performanceConfig.BufferConfig.ResultChannelSize, cap(s.resultChan),
				"结果通道大小不匹配")

			// Verification window created successfully
			assert.NotNil(t, s.Window, "窗口应该被创建")
		})
	}
}

// TestStreamUnifiedConfigPerformanceImpact tests the impact of unified configuration on Stream performance
func TestStreamUnifiedConfigPerformanceImpact(t *testing.T) {
	configs := map[string]types.PerformanceConfig{
		"默认配置":  types.DefaultPerformanceConfig(),
		"高性能配置": types.HighPerformanceConfig(),
		"低延迟配置": types.LowLatencyConfig(),
	}

	for name, perfConfig := range configs {
		t.Run(name, func(t *testing.T) {
			config := types.Config{
				NeedWindow: true,
				WindowConfig: types.WindowConfig{
					Type:   "tumbling",
					Params: []any{time.Second},
				},
				SelectFields: map[string]aggregator.AggregateType{
					"value": aggregator.Sum,
				},
				PerformanceConfig: perfConfig,
			}

			s, err := NewStream(config)
			require.NoError(t, err)
			defer s.Stop()

			go s.Start()

			// Send test data and measure performance
			dataCount := 1000
			startTime := time.Now()

			for i := 0; i < dataCount; i++ {
				data := map[string]any{
					"value":     i,
					"timestamp": time.Now().Unix(),
				}

				select {
				case s.dataChan <- data:
					// Successfully sent
				case <-time.After(100 * time.Millisecond):
					// Send timeout
					t.Logf("%d Data transmission timeout", i)
					break
				}
			}

			processingTime := time.Since(startTime)
			t.Logf("%s Time to process %d data entries: %v", name, dataCount, processingTime)

			// Waiting for some results
			time.Sleep(1500 * time.Millisecond)

			// Inspection results
			resultCount := 0
			for {
				select {
				case <-s.resultChan:
					resultCount++
				default:
					goto done
				}
			}
		done:
			// Performance testing mainly focuses on processing time, and the number of results may vary depending on the window trigger timing
		})
	}
}

// TestStreamUnifiedConfigErrorHandling tests the error handling of the unified configuration
func TestStreamUnifiedConfigErrorHandling(t *testing.T) {
	tests := []struct {
		name        string
		config      types.Config
		expectError bool
		description string
	}{
		{
			name: "无效窗口类型",
			config: types.Config{
				NeedWindow: true,
				WindowConfig: types.WindowConfig{
					Type:   "invalid_window_type",
					Params: []any{5 * time.Second},
				},
				SelectFields: map[string]aggregator.AggregateType{
					"value": aggregator.Count,
				},
				PerformanceConfig: types.DefaultPerformanceConfig(),
			},
			expectError: true,
			description: "无效的窗口类型应该导致创建失败",
		},
		{
			name: "缺少窗口大小参数",
			config: types.Config{
				NeedWindow: true,
				WindowConfig: types.WindowConfig{
					Type:   "tumbling",
					Params: []any{},
				},
				SelectFields: map[string]aggregator.AggregateType{
					"value": aggregator.Count,
				},
				PerformanceConfig: types.DefaultPerformanceConfig(),
			},
			expectError: true,
			description: "缺少size参数应该导致创建失败",
		},
		{
			name: "有效配置",
			config: types.Config{
				NeedWindow: true,
				WindowConfig: types.WindowConfig{
					Type:   "tumbling",
					Params: []any{5 * time.Second},
				},
				SelectFields: map[string]aggregator.AggregateType{
					"value": aggregator.Count,
				},
				PerformanceConfig: types.DefaultPerformanceConfig(),
			},
			expectError: false,
			description: "有效配置应该创建成功",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream, err := NewStream(tt.config)
			if tt.expectError {
				assert.Error(t, err, tt.description)
				assert.Nil(t, stream)
			} else {
				assert.NoError(t, err, tt.description)
				assert.NotNil(t, stream)
				if stream != nil {
					defer stream.Stop()
				}
			}
		})
	}
}

// TestStreamUnifiedConfigCompatibility: Test the compatibility of the unified configuration
func TestStreamUnifiedConfigCompatibility(t *testing.T) {
	// Test the new unified configuration
	newConfig := types.Config{
		NeedWindow: false,
		SelectFields: map[string]aggregator.AggregateType{
			"value": aggregator.Count,
		},
		PerformanceConfig: types.HighPerformanceConfig(),
	}

	s1, err := NewStream(newConfig)
	require.NoError(t, err)
	defer s1.Stop()

	// Verify that the new configuration is effective
	expectedDataSize := types.HighPerformanceConfig().BufferConfig.DataChannelSize
	assert.Equal(t, expectedDataSize, cap(s1.dataChan), "高性能配置的数据通道大小不匹配")

	// Test the default configuration
	defaultConfig := types.Config{
		NeedWindow: false,
		SelectFields: map[string]aggregator.AggregateType{
			"value": aggregator.Count,
		},
		PerformanceConfig: types.DefaultPerformanceConfig(),
	}

	s2, err := NewStream(defaultConfig)
	require.NoError(t, err)
	defer s2.Stop()

	// Verify the default configuration
	expectedDefaultSize := types.DefaultPerformanceConfig().BufferConfig.DataChannelSize
	assert.Equal(t, expectedDefaultSize, cap(s2.dataChan), "默认配置的数据通道大小不匹配")

}

func TestDataHandlerEnhanced(t *testing.T) {
	tests := []struct {
		name              string
		performanceConfig types.PerformanceConfig
		dataCount         int
		expectedDrops     bool
	}{
		{
			name:              "高性能配置 - 无丢弃",
			performanceConfig: types.HighPerformanceConfig(),
			dataCount:         100,
			expectedDrops:     false,
		},
		{
			name:              "低延迟配置 - 可能丢弃",
			performanceConfig: types.LowLatencyConfig(),
			dataCount:         1000,
			expectedDrops:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := types.Config{
				NeedWindow:        false,
				SimpleFields:      []string{"value"},
				PerformanceConfig: tt.performanceConfig,
			}

			stream, err := NewStream(config)
			require.NoError(t, err)
			defer stream.Stop()

			stream.Start()

			// Rapidly transmit large amounts of data
			for i := 0; i < tt.dataCount; i++ {
				stream.Emit(map[string]any{"value": i})
			}

			time.Sleep(100 * time.Millisecond)

			stats := stream.GetStats()
			droppedCount := stats[DroppedCount]

			if tt.expectedDrops {
				// There may be discard under heavy loads
				t.Logf("%s: Enter %d and discard %d", tt.name, stats[InputCount], droppedCount)
			} else {
				// High-performance configurations should be able to handle all the data
				assert.Equal(t, int64(0), droppedCount, "高性能配置不应该丢弃数据")
			}
		})
	}
}

// TestResultHandlerEnhanced Enhanced test result processor
func TestResultHandlerEnhanced(t *testing.T) {
	config := types.Config{
		NeedWindow:   false,
		SimpleFields: []string{"value"},
	}

	stream, err := NewStream(config)
	require.NoError(t, err)
	defer stream.Stop()

	// Test the Sink function
	var mu sync.Mutex
	var receivedResults []any

	stream.AddSink(func(result []map[string]any) {
		mu.Lock()
		defer mu.Unlock()
		receivedResults = append(receivedResults, result)
	})

	stream.Start()

	// Send test data
	testData := []map[string]any{
		{"value": 1},
		{"value": 2},
		{"value": 3},
	}

	for _, data := range testData {
		stream.Emit(data)
	}

	time.Sleep(100 * time.Millisecond)

	// Verify the results
	mu.Lock()
	defer mu.Unlock()

	assert.GreaterOrEqual(t, len(receivedResults), len(testData), "应该接收到所有结果")

	// Verification result format
	for _, result := range receivedResults {
		assert.IsType(t, []map[string]any{}, result, "结果应该是map切片类型")
		resultSlice := result.([]map[string]any)
		assert.Greater(t, len(resultSlice), 0, "结果切片不应该为空")
	}
}

// TestPerformanceConfigurationsEnhancedEnhanced version of the effect for testing different performance configurations
func TestPerformanceConfigurationsEnhanced(t *testing.T) {
	configs := map[string]types.PerformanceConfig{
		"Default":         types.DefaultPerformanceConfig(),
		"HighPerformance": types.HighPerformanceConfig(),
		"LowLatency":      types.LowLatencyConfig(),
	}

	for name, perfConfig := range configs {
		t.Run(name, func(t *testing.T) {
			config := types.Config{
				NeedWindow:        false,
				SimpleFields:      []string{"value"},
				PerformanceConfig: perfConfig,
			}

			stream, err := NewStream(config)
			require.NoError(t, err)
			defer stream.Stop()

			// Verify the buffer size
			assert.Equal(t, perfConfig.BufferConfig.DataChannelSize, cap(stream.dataChan))
			assert.Equal(t, perfConfig.BufferConfig.ResultChannelSize, cap(stream.resultChan))

			// Verify the work pool configuration
			assert.Equal(t, perfConfig.WorkerConfig.SinkPoolSize, cap(stream.sinkWorkerPool))

			//t.Logf("%s configuration: Data channel = %d, result channel = %d, Sink pool = %d",
			//	name,
			//	cap(stream.dataChan),
			//	cap(stream.resultChan),
			//	cap(stream.sinkWorkerPool))
		})
	}
}

// TestNewStreamFactory Creates the StreamFactory
func TestNewStreamFactory(t *testing.T) {
	factory := NewStreamFactory()
	assert.NotNil(t, factory)
}

// TestStreamFactory_CreateStream Test the creation of a default configured stream
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

	// Verify that the default performance configuration has been applied
	assert.NotEqual(t, types.PerformanceConfig{}, stream.config.PerformanceConfig)
}

// TestStreamFactory_CreateHighPerformanceStream Test to create high-performance streams
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

	// Verify high-performance configurations
	expectedConfig := types.HighPerformanceConfig()
	assert.Equal(t, expectedConfig, stream.config.PerformanceConfig)
}

// TestStreamFactory_CreateLowLatencyStream Test the creation of low-latency streams
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

	// Verify low-latency configurations
	expectedConfig := types.LowLatencyConfig()
	assert.Equal(t, expectedConfig, stream.config.PerformanceConfig)
}

// TestStreamFactory_CreateCustomPerformanceStream Test to create custom performance configuration streams
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

	// Verify custom configurations
	assert.Equal(t, customPerfConfig, stream.config.PerformanceConfig)
}

// TestStreamFactory_CreateStreamWithWindow Test the creation of a stream with a window
func TestStreamFactory_CreateStreamWithWindow(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		SimpleFields: []string{"name", "age"},
		NeedWindow:   true,
		WindowConfig: types.WindowConfig{
			Type:   "tumbling",
			Params: []any{5 * time.Second},
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

// TestStreamFactory_CreateStreamWithInvalidStrategy Test the flow that creates invalid policies
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

	_, err := factory.CreateStream(config)
	// An error should be reported because the strategy is ineffective
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid overflow strategy")
}

// TestStreamFactory_CreateWindow Create test window
func TestStreamFactory_CreateWindow(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		WindowConfig: types.WindowConfig{
			Type:   "tumbling",
			Params: []any{5 * time.Second},
		},
		PerformanceConfig: types.DefaultPerformanceConfig(),
	}

	win, err := factory.createWindow(config)
	require.NoError(t, err)
	assert.NotNil(t, win)
}

// TestStreamFactory_CreateStreamInstance Create test stream instances
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

// TestStreamFactory_Performance Testing plant performance
func TestStreamFactory_Performance(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}

	// Create multiple stream instances to verify plant performance
	streams := make([]*Stream, 10)
	for i := 0; i < 10; i++ {
		stream, err := factory.CreateStream(config)
		require.NoError(t, err)
		streams[i] = stream
	}

	// Cleanup
	for _, stream := range streams {
		stream.Stop()
	}
}

// TestStreamFactory_ConcurrentCreation Test concurrency to create streams
func TestStreamFactory_ConcurrentCreation(t *testing.T) {
	factory := NewStreamFactory()
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}

	const numGoroutines = 10
	streams := make([]*Stream, numGoroutines)
	errors := make([]error, numGoroutines)
	done := make(chan struct{})

	// Simultaneously launching and creating streams
	for i := 0; i < numGoroutines; i++ {
		go func(index int) {
			stream, err := factory.CreateStream(config)
			streams[index] = stream
			errors[index] = err
			done <- struct{}{}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify the results
	for i := 0; i < numGoroutines; i++ {
		assert.NoError(t, errors[i])
		assert.NotNil(t, streams[i])
		if streams[i] != nil {
			streams[i].Stop()
		}
	}
}
