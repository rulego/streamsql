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
	"fmt"
	"testing"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStream_CompileFieldProcessInfo 测试字段处理信息编译
func TestStream_CompileFieldProcessInfo(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age:user_age", "`device_id`", "*"},
		FieldExpressions: map[string]types.FieldExpression{
			"full_name": {
				Expression: "first_name + ' ' + last_name",
				Fields:     []string{"first_name", "last_name"},
			},
		},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	stream.compileFieldProcessInfo()

	// 验证编译后的字段信息
	assert.NotNil(t, stream.compiledFieldInfo)
	assert.NotNil(t, stream.compiledExprInfo)

	// 验证简单字段编译
	assert.Contains(t, stream.compiledFieldInfo, "name")
	assert.Contains(t, stream.compiledFieldInfo, "age:user_age")
	assert.Contains(t, stream.compiledFieldInfo, "`device_id`")
	assert.Contains(t, stream.compiledFieldInfo, "*")
}

// TestStream_CompileSimpleFieldInfo 测试简单字段信息编译
func TestStream_CompileSimpleFieldInfo(t *testing.T) {
	config := types.Config{}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	tests := []struct {
		name              string
		fieldSpec         string
		expectedFieldName string
		expectedOutput    string
		expectedSelectAll bool
		expectedNested    bool
		expectedFunction  bool
		expectedLiteral   bool
		expectedString    string
	}{
		{
			name:              "Select all",
			fieldSpec:         "*",
			expectedFieldName: "",
			expectedOutput:    "*",
			expectedSelectAll: true,
			expectedNested:    false,
			expectedFunction:  false,
			expectedLiteral:   false,
		},
		{
			name:              "Simple field",
			fieldSpec:         "name",
			expectedFieldName: "name",
			expectedOutput:    "name",
			expectedSelectAll: false,
			expectedNested:    false,
			expectedFunction:  false,
			expectedLiteral:   false,
		},
		{
			name:              "Field with alias",
			fieldSpec:         "age:user_age",
			expectedFieldName: "age",
			expectedOutput:    "user_age",
			expectedSelectAll: false,
			expectedNested:    false,
			expectedFunction:  false,
			expectedLiteral:   false,
		},
		{
			name:              "Field with backticks",
			fieldSpec:         "`device_id`",
			expectedFieldName: "device_id",
			expectedOutput:    "device_id",
			expectedSelectAll: false,
			expectedNested:    false,
			expectedFunction:  false,
			expectedLiteral:   false,
		},
		{
			name:              "Field with backticks and alias",
			fieldSpec:         "`device_id`:`id`",
			expectedFieldName: "device_id",
			expectedOutput:    "id",
			expectedSelectAll: false,
			expectedNested:    false,
			expectedFunction:  false,
			expectedLiteral:   false,
		},
		{
			name:              "Nested field",
			fieldSpec:         "device.id",
			expectedFieldName: "device.id",
			expectedOutput:    "device.id",
			expectedSelectAll: false,
			expectedNested:    true,
			expectedFunction:  false,
			expectedLiteral:   false,
		},
		{
			name:              "Function call",
			fieldSpec:         "UPPER(name)",
			expectedFieldName: "UPPER(name)",
			expectedOutput:    "UPPER(name)",
			expectedSelectAll: false,
			expectedNested:    false,
			expectedFunction:  true,
			expectedLiteral:   false,
		},
		{
			name:              "String literal with single quotes",
			fieldSpec:         "'constant_value'",
			expectedFieldName: "'constant_value'",
			expectedOutput:    "'constant_value'",
			expectedSelectAll: false,
			expectedNested:    false,
			expectedFunction:  false,
			expectedLiteral:   true,
			expectedString:    "constant_value",
		},
		{
			name:              "String literal with double quotes",
			fieldSpec:         "\"test_string\"",
			expectedFieldName: "\"test_string\"",
			expectedOutput:    "\"test_string\"",
			expectedSelectAll: false,
			expectedNested:    false,
			expectedFunction:  false,
			expectedLiteral:   true,
			expectedString:    "test_string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := stream.compileSimpleFieldInfo(tt.fieldSpec)
			assert.NotNil(t, info)
			assert.Equal(t, tt.expectedFieldName, info.fieldName)
			assert.Equal(t, tt.expectedOutput, info.outputName)
			assert.Equal(t, tt.expectedSelectAll, info.isSelectAll)
			assert.Equal(t, tt.expectedNested, info.hasNestedField)
			assert.Equal(t, tt.expectedFunction, info.isFunctionCall)
			assert.Equal(t, tt.expectedLiteral, info.isStringLiteral)
			if tt.expectedLiteral {
				assert.Equal(t, tt.expectedString, info.stringValue)
			}
			assert.Equal(t, tt.expectedOutput, info.alias)
		})
	}
}

// TestStream_CompileExpressionInfo 测试表达式信息编译
func TestStream_CompileExpressionInfo(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
		FieldExpressions: map[string]types.FieldExpression{
			"simple_expr": {
				Expression: "value + 10",
				Fields:     []string{"value"},
			},
			"nested_expr": {
				Expression: "device.temperature * 1.8 + 32",
				Fields:     []string{"device.temperature"},
			},
			"function_expr": {
				Expression: "UPPER(name)",
				Fields:     []string{"name"},
			},
			"backtick_expr": {
				Expression: "`field_name` + 5",
				Fields:     []string{"field_name"},
			},
		},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	stream.compileExpressionInfo()

	// 验证表达式信息已编译
	assert.NotNil(t, stream.compiledExprInfo)
	assert.Len(t, stream.compiledExprInfo, 4)

	// 验证每个表达式的编译信息
	for exprName := range config.FieldExpressions {
		assert.Contains(t, stream.compiledExprInfo, exprName)
		info := stream.compiledExprInfo[exprName]
		assert.NotNil(t, info)
		assert.NotEmpty(t, info.originalExpr)
	}
}

// TestFieldProcessInfo_EdgeCases 测试字段处理信息边界情况
func TestFieldProcessInfo_EdgeCases(t *testing.T) {
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

	tests := []struct {
		name      string
		fieldSpec string
	}{
		{"Empty string", ""},
		{"Only backticks", "``"},
		{"Only quotes", "''"},
		{"Only double quotes", "\"\""},
		{"Malformed alias", "field::alias"},
		{"Complex nested", "a.b.c.d.e"},
		{"Function with nested", "FUNC(a.b.c)"},
		{"Mixed quotes", "'test\""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 应该不会panic，即使输入不规范
			assert.NotPanics(t, func() {
				info := stream.compileSimpleFieldInfo(tt.fieldSpec)
				assert.NotNil(t, info)
			})
		})
	}
}

// TestExpressionProcessInfo_Structure 测试表达式处理信息结构
func TestExpressionProcessInfo_Structure(t *testing.T) {
	// 测试expressionProcessInfo结构的基本功能
	info := &expressionProcessInfo{
		originalExpr:            "value + 10",
		processedExpr:           "value + 10",
		isFunctionCall:          false,
		hasNestedFields:         false,
		needsBacktickPreprocess: false,
	}

	assert.Equal(t, "value + 10", info.originalExpr)
	assert.Equal(t, "value + 10", info.processedExpr)
	assert.False(t, info.isFunctionCall)
	assert.False(t, info.hasNestedFields)
	assert.False(t, info.needsBacktickPreprocess)
	assert.Nil(t, info.compiledExpr)
}

// TestFieldProcessInfo_Structure 测试字段处理信息结构
func TestFieldProcessInfo_Structure(t *testing.T) {
	// 测试fieldProcessInfo结构的基本功能
	info := &fieldProcessInfo{
		fieldName:       "test_field",
		outputName:      "output_field",
		isFunctionCall:  false,
		hasNestedField:  false,
		isSelectAll:     false,
		isStringLiteral: true,
		stringValue:     "literal_value",
		alias:           "field_alias",
	}

	assert.Equal(t, "test_field", info.fieldName)
	assert.Equal(t, "output_field", info.outputName)
	assert.False(t, info.isFunctionCall)
	assert.False(t, info.hasNestedField)
	assert.False(t, info.isSelectAll)
	assert.True(t, info.isStringLiteral)
	assert.Equal(t, "literal_value", info.stringValue)
	assert.Equal(t, "field_alias", info.alias)
}

// TestStream_CompileFieldProcessInfo_Performance 测试字段处理信息编译性能
func TestStream_CompileFieldProcessInfo_Performance(t *testing.T) {
	// 创建大量字段的配置
	fields := make([]string, 100)
	expressions := make(map[string]types.FieldExpression)

	for i := 0; i < 100; i++ {
		fields[i] = fmt.Sprintf("field_%d", i)
		expressions[fmt.Sprintf("expr_%d", i)] = types.FieldExpression{
			Expression: fmt.Sprintf("field_%d + %d", i, i),
			Fields:     []string{fmt.Sprintf("field_%d", i)},
		}
	}

	config := types.Config{
		SimpleFields:     fields,
		FieldExpressions: expressions,
	}

	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	// 编译应该快速完成，不会超时
	assert.NotPanics(t, func() {
		stream.compileFieldProcessInfo()
	})

	// 验证编译结果
	assert.Len(t, stream.compiledFieldInfo, 100)
	assert.Len(t, stream.compiledExprInfo, 100)
}
