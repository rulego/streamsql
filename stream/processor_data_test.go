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

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataProcessor_NewDataProcessor 测试数据处理器创建
func TestDataProcessor_Constructor(t *testing.T) {
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

	processor := NewDataProcessor(stream)
	assert.NotNil(t, processor)
	assert.Equal(t, stream, processor.stream)
}

// TestDataProcessor_InitializeAggregator 测试聚合器初始化
func TestDataProcessor_InitializeAggregator(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"device", "temperature", "humidity"},
		NeedWindow:   true,
		GroupFields:  []string{"device"},
		SelectFields: map[string]aggregator.AggregateType{
			"temperature": aggregator.Avg,
			"humidity":    aggregator.Sum,
		},
		WindowConfig: types.WindowConfig{
			Type:   "tumbling",
			Params: map[string]interface{}{"size": 1 * time.Second},
		},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	processor := NewDataProcessor(stream)
	processor.initializeAggregator()

	assert.NotNil(t, stream.aggregator)
}

// TestDataProcessor_RegisterExpressionCalculator 测试表达式计算器注册
func TestDataProcessor_RegisterExpressionCalculator(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"device", "temperature"},
		NeedWindow:   true,
		GroupFields:  []string{"device"},
		SelectFields: map[string]aggregator.AggregateType{
			"temperature": aggregator.Avg,
		},
		FieldExpressions: map[string]types.FieldExpression{
			"temp_celsius": {
				Expression: "temperature * 1.8 + 32",
				Fields:     []string{"temperature"},
			},
		},
		WindowConfig: types.WindowConfig{
			Type:   "tumbling",
			Params: map[string]interface{}{"size": 1 * time.Second},
		},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	processor := NewDataProcessor(stream)
	processor.initializeAggregator()

	// 验证表达式计算器已注册
	assert.NotNil(t, stream.aggregator)
}

// TestDataProcessor_EvaluateExpressionForAggregation 测试聚合表达式计算
func TestDataProcessor_EvaluateExpressionForAggregation(t *testing.T) {
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

	processor := NewDataProcessor(stream)

	tests := []struct {
		name      string
		fieldExpr types.FieldExpression
		data      map[string]interface{}
		expected  interface{}
		hasError  bool
	}{
		{
			name: "Simple arithmetic expression",
			fieldExpr: types.FieldExpression{
				Expression: "temperature * 2",
				Fields:     []string{"temperature"},
			},
			data:     map[string]interface{}{"temperature": 25.0},
			expected: 50.0,
			hasError: false,
		},
		{
			name: "String concatenation",
			fieldExpr: types.FieldExpression{
				Expression: "name + '_suffix'",
				Fields:     []string{"name"},
			},
			data:     map[string]interface{}{"name": "test"},
			expected: "test_suffix",
			hasError: false,
		},
		{
			name: "Nested field expression",
			fieldExpr: types.FieldExpression{
				Expression: "device.id + 100",
				Fields:     []string{"device.id"},
			},
			data: map[string]interface{}{
				"device": map[string]interface{}{"id": 1},
			},
			expected: 101.0,
			hasError: false,
		},
		{
			name: "CASE expression",
			fieldExpr: types.FieldExpression{
				Expression: "CASE WHEN temperature > 30 THEN 'hot' ELSE 'cold' END",
				Fields:     []string{"temperature"},
			},
			data:     map[string]interface{}{"temperature": 35.0},
			expected: "hot",
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := processor.evaluateExpressionForAggregation(tt.fieldExpr, tt.data)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestDataProcessor_EvaluateNestedFieldExpression 测试嵌套字段表达式计算
func TestDataProcessor_EvaluateNestedFieldExpression(t *testing.T) {
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

	processor := NewDataProcessor(stream)

	tests := []struct {
		name       string
		expression string
		data       map[string]interface{}
		expected   interface{}
		hasError   bool
	}{
		{
			name:       "Simple nested field",
			expression: "device.id",
			data: map[string]interface{}{
				"device": map[string]interface{}{"id": 123},
			},
			expected: 123.0,
			hasError: false,
		},
		{
			name:       "Nested field arithmetic",
			expression: "device.temperature + 10",
			data: map[string]interface{}{
				"device": map[string]interface{}{"temperature": 25.5},
			},
			expected: 35.5,
			hasError: false,
		},
		{
			name:       "Deep nested field",
			expression: "sensor.data.value",
			data: map[string]interface{}{
				"sensor": map[string]interface{}{
					"data": map[string]interface{}{"value": 42.0},
				},
			},
			expected: 42.0,
			hasError: false,
		},
		{
			name:       "Nested field with backticks",
			expression: "`device`.`id`",
			data: map[string]interface{}{
				"device": map[string]interface{}{"id": 456},
			},
			expected: 456.0,
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := processor.evaluateNestedFieldExpression(tt.expression, tt.data)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestDataProcessor_EvaluateCaseExpression 测试CASE表达式计算
func TestDataProcessor_EvaluateCaseExpression(t *testing.T) {
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

	processor := NewDataProcessor(stream)

	tests := []struct {
		name       string
		expression string
		data       map[string]interface{}
		expected   interface{}
		hasError   bool
	}{
		{
			name:       "Simple CASE expression",
			expression: "CASE WHEN temperature > 30 THEN 'hot' ELSE 'cold' END",
			data:       map[string]interface{}{"temperature": 35.0},
			expected:   "hot",
			hasError:   false,
		},
		{
			name:       "CASE with multiple conditions",
			expression: "CASE WHEN temperature > 30 THEN 'hot' WHEN temperature > 20 THEN 'warm' ELSE 'cold' END",
			data:       map[string]interface{}{"temperature": 25.0},
			expected:   "warm",
			hasError:   false,
		},
		{
			name:       "CASE with numeric result",
			expression: "CASE WHEN status == 'active' THEN 1 ELSE 0 END",
			data:       map[string]interface{}{"status": "active"},
			expected:   1.0,
			hasError:   false,
		},
		{
			name:       "CASE with backtick identifiers",
			expression: "CASE WHEN `temperature` > 30 THEN 'hot' ELSE 'cold' END",
			data:       map[string]interface{}{"temperature": 35.0},
			expected:   "hot",
			hasError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := processor.evaluateCaseExpression(tt.expression, tt.data)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestDataProcessor_FallbackExpressionEvaluation 测试回退表达式计算
func TestDataProcessor_FallbackExpressionEvaluation(t *testing.T) {
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

	processor := NewDataProcessor(stream)

	tests := []struct {
		name       string
		expression string
		data       map[string]interface{}
		expected   interface{}
		hasError   bool
	}{
		{
			name:       "Simple arithmetic",
			expression: "value + 10",
			data:       map[string]interface{}{"value": 5.0},
			expected:   15.0,
			hasError:   false,
		},
		{
			name:       "String operation",
			expression: "name + '_test'",
			data:       map[string]interface{}{"name": "hello"},
			expected:   "hello_test",
			hasError:   false,
		},
		{
			name:       "Boolean expression",
			expression: "value > 10",
			data:       map[string]interface{}{"value": 15.0},
			expected:   true,
			hasError:   false,
		},
		{
			name:       "Expression with backticks",
			expression: "`value` * 2",
			data:       map[string]interface{}{"value": 7.0},
			expected:   14.0,
			hasError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := processor.fallbackExpressionEvaluation(tt.expression, tt.data)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestDataProcessor_ExpressionWithNullValues 测试包含NULL值的表达式计算
func TestDataProcessor_ExpressionWithNullValues(t *testing.T) {
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

	processor := NewDataProcessor(stream)

	// 测试NULL值处理
	data := map[string]interface{}{
		"value":    nil,
		"nonNull":  10.0,
		"nullStr":  nil,
		"validStr": "test",
	}

	// 测试嵌套字段NULL值
	result, err := processor.evaluateNestedFieldExpression("value + 5", data)
	assert.NoError(t, err)
	assert.Nil(t, result) // NULL + 5 应该返回 NULL

	// 测试CASE表达式NULL值
	result, err = processor.evaluateCaseExpression("CASE WHEN value IS NULL THEN 'null_value' ELSE 'not_null' END", data)
	assert.NoError(t, err)
	assert.Equal(t, "null_value", result)

	// 测试回退表达式NULL值
	result, err = processor.fallbackExpressionEvaluation("nonNull * 2", data)
	assert.NoError(t, err)
	assert.Equal(t, 20.0, result)
}

// TestDataProcessor_ExpandUnnestResults 测试 expandUnnestResults 函数的各种情况
func TestDataProcessor_ExpandUnnestResults(t *testing.T) {
	tests := []struct {
		name             string
		hasUnnestFunction bool
		result           map[string]interface{}
		originalData     map[string]interface{}
		expected         []map[string]interface{}
	}{
		{
			name:             "no unnest function - should return single result",
			hasUnnestFunction: false,
			result: map[string]interface{}{
				"name": "test",
				"age":  25,
			},
			originalData: map[string]interface{}{"id": 1},
			expected: []map[string]interface{}{
				{"name": "test", "age": 25},
			},
		},
		{
			name:             "empty result - should return single empty result",
			hasUnnestFunction: true,
			result:           map[string]interface{}{},
			originalData:     map[string]interface{}{"id": 1},
			expected: []map[string]interface{}{
				{},
			},
		},
		{
			name:             "no unnest result - should return single result",
			hasUnnestFunction: true,
			result: map[string]interface{}{
				"name": "test",
				"age":  25,
			},
			originalData: map[string]interface{}{"id": 1},
			expected: []map[string]interface{}{
				{"name": "test", "age": 25},
			},
		},
		{
			name:             "unnest result with simple values",
			hasUnnestFunction: true,
			result: map[string]interface{}{
				"name": "test",
				"items": []interface{}{
					map[string]interface{}{
						"__unnest_object__": true,
						"__data__":          "item1",
					},
					map[string]interface{}{
						"__unnest_object__": true,
						"__data__":          "item2",
					},
				},
			},
			originalData: map[string]interface{}{"id": 1},
			expected: []map[string]interface{}{
				{"name": "test", "items": "item1"},
				{"name": "test", "items": "item2"},
			},
		},
		{
			name:             "unnest result with object values",
			hasUnnestFunction: true,
			result: map[string]interface{}{
				"name": "test",
				"orders": []interface{}{
					map[string]interface{}{
						"__unnest_object__": true,
						"__data__": map[string]interface{}{
							"order_id": 1,
							"amount":   100,
						},
					},
					map[string]interface{}{
						"__unnest_object__": true,
						"__data__": map[string]interface{}{
							"order_id": 2,
							"amount":   200,
						},
					},
				},
			},
			originalData: map[string]interface{}{"id": 1},
			expected: []map[string]interface{}{
				{"name": "test", "order_id": 1, "amount": 100},
				{"name": "test", "order_id": 2, "amount": 200},
			},
		},
		{
			name:             "empty unnest result - should return empty array",
			hasUnnestFunction: true,
			result: map[string]interface{}{
				"name": "test",
				"items": []interface{}{
					map[string]interface{}{
						"__unnest_object__": true,
						"__empty_unnest__":  true,
					},
				},
			},
			originalData: map[string]interface{}{"id": 1},
			expected:     []map[string]interface{}{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建测试用的 stream 和 processor
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

			// 设置 hasUnnestFunction 标志
			stream.hasUnnestFunction = tt.hasUnnestFunction

			processor := NewDataProcessor(stream)

			// 调用被测试的函数
			result := processor.expandUnnestResults(tt.result, tt.originalData)

			// 验证结果
			assert.Equal(t, tt.expected, result)
		})
	}
}
