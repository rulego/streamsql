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

// TestPrintTableFromSlice tests the table printing function
func TestPrintTableFromSlice(t *testing.T) {
	// Test empty data
	assert.NotPanics(t, func() {
		PrintTableFromSlice([]map[string]any{}, nil)
	}, "空数据不应该panic")

	// Test normal data
	data := []map[string]any{
		{"name": "Alice", "age": 30, "city": "New York"},
		{"name": "Bob", "age": 25, "city": "Los Angeles"},
	}
	assert.NotPanics(t, func() {
		PrintTableFromSlice(data, nil)
	}, "正常数据不应该panic")

	// Test data with field order
	fieldOrder := []string{"name", "city", "age"}
	assert.NotPanics(t, func() {
		PrintTableFromSlice(data, fieldOrder)
	}, "带字段顺序的数据不应该panic")
}

// TestPrintTableBorder tests border printing functionality
func TestPrintTableBorder(t *testing.T) {
	// Test the normal width
	assert.NotPanics(t, func() {
		colWidths := []int{5, 8, 6}
		PrintTableBorder(colWidths)
	}, "PrintTableBorder不应该panic")

	// Test the empty width
	assert.NotPanics(t, func() {
		PrintTableBorder([]int{})
	}, "空宽度数组不应该panic")
}

// TestFormatTableData test data formatting function
func TestFormatTableData(t *testing.T) {
	// Test slicing data
	sliceData := []map[string]any{
		{"device": "sensor1", "temp": 25.5},
	}
	assert.NotPanics(t, func() {
		FormatTableData(sliceData, nil)
	}, "切片数据不应该panic")

	// Test individual map data
	mapData := map[string]any{"device": "sensor1", "temp": 25.5}
	assert.NotPanics(t, func() {
		FormatTableData(mapData, nil)
	}, "map数据不应该panic")

	// Testing other types of data
	assert.NotPanics(t, func() {
		FormatTableData("string data", nil)
	}, "字符串数据不应该panic")

	// Test empty data
	assert.NotPanics(t, func() {
		FormatTableData([]map[string]any{}, nil)
	}, "空切片数据不应该panic")

	assert.NotPanics(t, func() {
		FormatTableData(map[string]any{}, nil)
	}, "空map数据不应该panic")
}

// TestPrintTableFromSliceEdgeCases tests edge conditions
func TestPrintTableFromSliceEdgeCases(t *testing.T) {
	// The test field order includes fields that do not exist
	data := []map[string]any{
		{"a": "1", "b": "2"},
	}
	fieldOrder := []string{"nonexistent", "a", "b", "another_nonexistent"}
	assert.NotPanics(t, func() {
		PrintTableFromSlice(data, fieldOrder)
	}, "字段顺序包含不存在字段不应该panic")

	// Some fields in the test data row are missing
	dataWithMissingFields := []map[string]any{
		{"name": "Alice", "age": 30},
		{"name": "Bob", "city": "NYC"}, // The age field is missing
		{"age": 25, "city": "LA"},      // The name field is missing
	}
	assert.NotPanics(t, func() {
		PrintTableFromSlice(dataWithMissingFields, nil)
	}, "数据行字段缺失不应该panic")

	// Test short field name (test minimum width 4 logic)
	shortFieldData := []map[string]any{
		{"a": "1", "bb": "22", "ccc": "333"},
	}
	assert.NotPanics(t, func() {
		PrintTableFromSlice(shortFieldData, nil)
	}, "短字段名不应该panic")

	// Test null values and nil values
	nilValueData := []map[string]any{
		{"name": "Alice", "value": nil},
		{"name": "Bob", "value": ""},
	}
	assert.NotPanics(t, func() {
		PrintTableFromSlice(nilValueData, nil)
	}, "nil值不应该panic")
}
