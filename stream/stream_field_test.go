package stream

import (
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSelectStarWithExpressionFields 测试SELECT *与表达式字段的组合
func TestSelectStarWithExpressionFields(t *testing.T) {
	tests := []struct {
		name             string
		simpleFields     []string
		fieldExpressions map[string]types.FieldExpression
		testData         map[string]interface{}
		expectedFields   map[string]interface{}
	}{
		{
			name:         "SELECT * with additional expressions",
			simpleFields: []string{"*"},
			fieldExpressions: map[string]types.FieldExpression{
				"name": {
					Expression: "UPPER(name)",
					Fields:     []string{"name"},
				},
				"full_info": {
					Expression: "CONCAT(name, ' - ', status)",
					Fields:     []string{"name", "status"},
				},
			},
			testData: map[string]interface{}{
				"name":   "john",
				"status": "active",
				"age":    25,
			},
			expectedFields: map[string]interface{}{
				"name":      "JOHN",
				"full_info": "john - active",
				"status":    "active",
				"age":       25,
			},
		},
		{
			name:         "SELECT * with field override",
			simpleFields: []string{"*"},
			fieldExpressions: map[string]types.FieldExpression{
				"name": {
					Expression: "UPPER(name)",
					Fields:     []string{"name"},
				},
				"age": {
					Expression: "age * 2",
					Fields:     []string{"age"},
				},
			},
			testData: map[string]interface{}{
				"name":   "alice",
				"age":    30,
				"status": "active",
			},
			expectedFields: map[string]interface{}{
				"name":   "ALICE",
				"age":    60.0, // 表达式结果
				"status": "active",
			},
		},
		{
			name:             "SELECT * without expressions",
			simpleFields:     []string{"*"},
			fieldExpressions: nil,
			testData: map[string]interface{}{
				"name":   "bob",
				"age":    35,
				"status": "inactive",
			},
			expectedFields: map[string]interface{}{
				"name":   "bob",
				"age":    35,
				"status": "inactive",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := types.Config{
				NeedWindow:       false,
				SimpleFields:     tt.simpleFields,
				FieldExpressions: tt.fieldExpressions,
			}

			stream, err := NewStream(config)
			require.NoError(t, err)
			defer stream.Stop()

			// 收集结果
			var mu sync.Mutex
			var results []interface{}
			stream.AddSink(func(result []map[string]interface{}) {
				mu.Lock()
				defer mu.Unlock()
				results = append(results, result)
			})

			stream.Start()
			stream.Emit(tt.testData)

			// 等待处理完成
			time.Sleep(100 * time.Millisecond)

			// 验证结果
			mu.Lock()
			defer mu.Unlock()

			require.Len(t, results, 1)
			resultData := results[0].([]map[string]interface{})[0]

			for field, expected := range tt.expectedFields {
				actual, exists := resultData[field]
				assert.True(t, exists, "Field %s should exist", field)
				if expected != nil {
					// 处理数值类型的比较
					if expectedFloat, ok := expected.(float64); ok {
						if actualFloat, ok := actual.(float64); ok {
							assert.InEpsilon(t, expectedFloat, actualFloat, 0.0001)
						} else if actualInt, ok := actual.(int); ok {
							assert.InEpsilon(t, expectedFloat, float64(actualInt), 0.0001)
						} else {
							t.Errorf("Expected %s to be numeric, got %T", field, actual)
						}
					} else {
						assert.Equal(t, expected, actual, "Field %s mismatch", field)
					}
				}
			}
		})
	}
}

// TestFieldProcessor 测试字段处理器
func TestFieldProcessor(t *testing.T) {
	tests := []struct {
		name         string
		simpleFields []string
		testData     map[string]interface{}
		expected     map[string]interface{}
	}{
		{
			name:         "Specific fields",
			simpleFields: []string{"name", "age"},
			testData: map[string]interface{}{
				"name":   "test",
				"age":    25,
				"status": "active",
			},
			expected: map[string]interface{}{
				"name": "test",
				"age":  25,
			},
		},
		{
			name:         "All fields with *",
			simpleFields: []string{"*"},
			testData: map[string]interface{}{
				"name":   "test",
				"age":    25,
				"status": "active",
			},
			expected: map[string]interface{}{
				"name":   "test",
				"age":    25,
				"status": "active",
			},
		},
		{
			name:         "Mixed fields",
			simpleFields: []string{"name", "*"},
			testData: map[string]interface{}{
				"name":   "test",
				"age":    25,
				"status": "active",
			},
			expected: map[string]interface{}{
				"name":   "test",
				"age":    25,
				"status": "active",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := types.Config{
				NeedWindow:   false,
				SimpleFields: tt.simpleFields,
			}

			stream, err := NewStream(config)
			require.NoError(t, err)
			defer stream.Stop()

			var mu sync.Mutex
			var results []interface{}
			stream.AddSink(func(result []map[string]interface{}) {
				mu.Lock()
				defer mu.Unlock()
				results = append(results, result)
			})

			stream.Start()
			stream.Emit(tt.testData)

			time.Sleep(100 * time.Millisecond)

			mu.Lock()
			defer mu.Unlock()

			require.Len(t, results, 1)
			resultData := results[0].([]map[string]interface{})[0]

			// 验证期望的字段都存在
			for field, expected := range tt.expected {
				actual, exists := resultData[field]
				assert.True(t, exists, "Field %s should exist", field)
				assert.Equal(t, expected, actual, "Field %s value mismatch", field)
			}

			// 如果不是 "*"，验证没有额外的字段
			if len(tt.simpleFields) == 1 && tt.simpleFields[0] != "*" {
				assert.Len(t, resultData, len(tt.expected), "Should only have expected fields")
			}
		})
	}
}

// TestExpressionEvaluation 测试表达式计算
func TestExpressionEvaluation(t *testing.T) {
	tests := []struct {
		name       string
		expression types.FieldExpression
		testData   map[string]interface{}
		expected   interface{}
	}{
		{
			name: "String concatenation",
			expression: types.FieldExpression{
				Expression: "CONCAT(first_name, ' ', last_name)",
				Fields:     []string{"first_name", "last_name"},
			},
			testData: map[string]interface{}{
				"first_name": "John",
				"last_name":  "Doe",
			},
			expected: "John Doe",
		},
		{
			name: "Arithmetic operation",
			expression: types.FieldExpression{
				Expression: "price * quantity",
				Fields:     []string{"price", "quantity"},
			},
			testData: map[string]interface{}{
				"price":    10.5,
				"quantity": 3,
			},
			expected: 31.5,
		},
		{
			name: "String transformation",
			expression: types.FieldExpression{
				Expression: "UPPER(name)",
				Fields:     []string{"name"},
			},
			testData: map[string]interface{}{
				"name": "alice",
			},
			expected: "ALICE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := types.Config{
				NeedWindow: false,
				FieldExpressions: map[string]types.FieldExpression{
					"result": tt.expression,
				},
			}

			stream, err := NewStream(config)
			require.NoError(t, err)
			defer stream.Stop()

			var mu sync.Mutex
			var results []interface{}
			stream.AddSink(func(result []map[string]interface{}) {
				mu.Lock()
				defer mu.Unlock()
				results = append(results, result)
			})

			stream.Start()
			stream.Emit(tt.testData)

			time.Sleep(100 * time.Millisecond)

			mu.Lock()
			defer mu.Unlock()

			require.Len(t, results, 1)
			resultData := results[0].([]map[string]interface{})[0]

			actual, exists := resultData["result"]
			assert.True(t, exists, "Result field should exist")

			// 处理数值类型的比较
			if expectedFloat, ok := tt.expected.(float64); ok {
				if actualFloat, ok := actual.(float64); ok {
					assert.InEpsilon(t, expectedFloat, actualFloat, 0.0001)
				} else {
					t.Errorf("Expected float64, got %T", actual)
				}
			} else {
				assert.Equal(t, tt.expected, actual)
			}
		})
	}
}
