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

package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPrintTableFromSlice 测试表格打印功能
func TestPrintTableFromSlice(t *testing.T) {
	// 测试空数据
	assert.NotPanics(t, func() {
		PrintTableFromSlice([]map[string]interface{}{}, nil)
	}, "空数据不应该panic")

	// 测试正常数据
	data := []map[string]interface{}{
		{"name": "Alice", "age": 30, "city": "New York"},
		{"name": "Bob", "age": 25, "city": "Los Angeles"},
	}
	assert.NotPanics(t, func() {
		PrintTableFromSlice(data, nil)
	}, "正常数据不应该panic")

	// 测试带字段顺序的数据
	fieldOrder := []string{"name", "city", "age"}
	assert.NotPanics(t, func() {
		PrintTableFromSlice(data, fieldOrder)
	}, "带字段顺序的数据不应该panic")
}

// TestPrintTableBorder 测试边框打印功能
func TestPrintTableBorder(t *testing.T) {
	// 测试正常宽度
	assert.NotPanics(t, func() {
		colWidths := []int{5, 8, 6}
		PrintTableBorder(colWidths)
	}, "PrintTableBorder不应该panic")

	// 测试空宽度
	assert.NotPanics(t, func() {
		PrintTableBorder([]int{})
	}, "空宽度数组不应该panic")
}

// TestFormatTableData 测试数据格式化功能
func TestFormatTableData(t *testing.T) {
	// 测试切片数据
	sliceData := []map[string]interface{}{
		{"device": "sensor1", "temp": 25.5},
	}
	assert.NotPanics(t, func() {
		FormatTableData(sliceData, nil)
	}, "切片数据不应该panic")

	// 测试单个map数据
	mapData := map[string]interface{}{"device": "sensor1", "temp": 25.5}
	assert.NotPanics(t, func() {
		FormatTableData(mapData, nil)
	}, "map数据不应该panic")

	// 测试其他类型数据
	assert.NotPanics(t, func() {
		FormatTableData("string data", nil)
	}, "字符串数据不应该panic")

	// 测试空数据
	assert.NotPanics(t, func() {
		FormatTableData([]map[string]interface{}{}, nil)
	}, "空切片数据不应该panic")

	assert.NotPanics(t, func() {
		FormatTableData(map[string]interface{}{}, nil)
	}, "空map数据不应该panic")
}

// TestPrintTableFromSliceEdgeCases 测试边缘情况
func TestPrintTableFromSliceEdgeCases(t *testing.T) {
	// 测试字段顺序包含不存在的字段
	data := []map[string]interface{}{
		{"a": "1", "b": "2"},
	}
	fieldOrder := []string{"nonexistent", "a", "b", "another_nonexistent"}
	assert.NotPanics(t, func() {
		PrintTableFromSlice(data, fieldOrder)
	}, "字段顺序包含不存在字段不应该panic")

	// 测试数据行中某些字段缺失
	dataWithMissingFields := []map[string]interface{}{
		{"name": "Alice", "age": 30},
		{"name": "Bob", "city": "NYC"}, // 缺少age字段
		{"age": 25, "city": "LA"},     // 缺少name字段
	}
	assert.NotPanics(t, func() {
		PrintTableFromSlice(dataWithMissingFields, nil)
	}, "数据行字段缺失不应该panic")

	// 测试短字段名（测试最小宽度4的逻辑）
	shortFieldData := []map[string]interface{}{
		{"a": "1", "bb": "22", "ccc": "333"},
	}
	assert.NotPanics(t, func() {
		PrintTableFromSlice(shortFieldData, nil)
	}, "短字段名不应该panic")

	// 测试空值和nil值
	nilValueData := []map[string]interface{}{
		{"name": "Alice", "value": nil},
		{"name": "Bob", "value": ""},
	}
	assert.NotPanics(t, func() {
		PrintTableFromSlice(nilValueData, nil)
	}, "nil值不应该panic")
}