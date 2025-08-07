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

package types

import (
	"testing"
	"time"
)

// TestRow 测试 Row 结构体的基本功能
func TestRow(t *testing.T) {
	testTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	testData := map[string]interface{}{
		"user_id": 123,
		"amount":  99.99,
		"status":  "active",
	}
	testSlot := &TimeSlot{
		Start: &testTime,
		End:   func() *time.Time { t := testTime.Add(time.Hour); return &t }(),
	}

	row := &Row{
		Timestamp: testTime,
		Data:      testData,
		Slot:      testSlot,
	}

	// 测试 GetTimestamp 方法
	if !row.GetTimestamp().Equal(testTime) {
		t.Errorf("Expected timestamp %v, got %v", testTime, row.GetTimestamp())
	}

	// 测试 Timestamp 字段
	if !row.Timestamp.Equal(testTime) {
		t.Errorf("Expected timestamp %v, got %v", testTime, row.Timestamp)
	}

	// 测试 Data 字段
	if row.Data == nil {
		t.Error("Expected Data to be non-nil")
	}

	dataMap, ok := row.Data.(map[string]interface{})
	if !ok {
		t.Error("Expected Data to be a map[string]interface{}")
	}

	if dataMap["user_id"] != 123 {
		t.Errorf("Expected user_id 123, got %v", dataMap["user_id"])
	}

	if dataMap["amount"] != 99.99 {
		t.Errorf("Expected amount 99.99, got %v", dataMap["amount"])
	}

	if dataMap["status"] != "active" {
		t.Errorf("Expected status 'active', got %v", dataMap["status"])
	}

	// 测试 Slot 字段
	if row.Slot == nil {
		t.Error("Expected Slot to be non-nil")
	}

	if !row.Slot.Start.Equal(testTime) {
		t.Errorf("Expected slot start %v, got %v", testTime, row.Slot.Start)
	}

	if !row.Slot.End.Equal(testTime.Add(time.Hour)) {
		t.Errorf("Expected slot end %v, got %v", testTime.Add(time.Hour), row.Slot.End)
	}
}

// TestRowWithNilData 测试 Row 结构体处理 nil 数据的情况
func TestRowWithNilData(t *testing.T) {
	testTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	row := &Row{
		Timestamp: testTime,
		Data:      nil,
		Slot:      nil,
	}

	// 测试 GetTimestamp 方法仍然正常工作
	if !row.GetTimestamp().Equal(testTime) {
		t.Errorf("Expected timestamp %v, got %v", testTime, row.GetTimestamp())
	}

	// 测试 nil 数据
	if row.Data != nil {
		t.Error("Expected Data to be nil")
	}

	// 测试 nil slot
	if row.Slot != nil {
		t.Error("Expected Slot to be nil")
	}
}

// TestRowWithDifferentDataTypes 测试 Row 结构体处理不同数据类型的情况
func TestRowWithDifferentDataTypes(t *testing.T) {
	testTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	// 测试字符串数据
	rowString := &Row{
		Timestamp: testTime,
		Data:      "test string data",
	}

	if rowString.Data != "test string data" {
		t.Errorf("Expected string data 'test string data', got %v", rowString.Data)
	}

	// 测试数字数据
	rowNumber := &Row{
		Timestamp: testTime,
		Data:      42,
	}

	if rowNumber.Data != 42 {
		t.Errorf("Expected number data 42, got %v", rowNumber.Data)
	}

	// 测试布尔数据
	rowBool := &Row{
		Timestamp: testTime,
		Data:      true,
	}

	if rowBool.Data != true {
		t.Errorf("Expected boolean data true, got %v", rowBool.Data)
	}

	// 测试切片数据
	sliceData := []string{"item1", "item2", "item3"}
	rowSlice := &Row{
		Timestamp: testTime,
		Data:      sliceData,
	}

	resultSlice, ok := rowSlice.Data.([]string)
	if !ok {
		t.Error("Expected Data to be a []string")
	}

	if len(resultSlice) != 3 {
		t.Errorf("Expected slice length 3, got %d", len(resultSlice))
	}

	if resultSlice[0] != "item1" {
		t.Errorf("Expected first item 'item1', got %v", resultSlice[0])
	}
}

// TestRowEventInterface 测试 Row 实现 RowEvent 接口
func TestRowEventInterface(t *testing.T) {
	testTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	row := &Row{
		Timestamp: testTime,
		Data:      "test data",
	}

	// 验证 Row 实现了 RowEvent 接口
	var rowEvent RowEvent = row

	if !rowEvent.GetTimestamp().Equal(testTime) {
		t.Errorf("Expected timestamp %v from RowEvent interface, got %v", testTime, rowEvent.GetTimestamp())
	}
}

// TestRowZeroTime 测试 Row 结构体处理零时间的情况
func TestRowZeroTime(t *testing.T) {
	zeroTime := time.Time{}

	row := &Row{
		Timestamp: zeroTime,
		Data:      "test data",
	}

	if !row.GetTimestamp().Equal(zeroTime) {
		t.Errorf("Expected zero timestamp %v, got %v", zeroTime, row.GetTimestamp())
	}

	if !row.GetTimestamp().IsZero() {
		t.Error("Expected timestamp to be zero")
	}
}

// TestRowConcurrentAccess 测试 Row 结构体的并发访问
func TestRowConcurrentAccess(t *testing.T) {
	testTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	row := &Row{
		Timestamp: testTime,
		Data:      "test data",
	}

	// 启动多个 goroutine 并发访问 GetTimestamp 方法
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				timestamp := row.GetTimestamp()
				if !timestamp.Equal(testTime) {
					t.Errorf("Concurrent access failed: expected %v, got %v", testTime, timestamp)
				}
			}
			done <- true
		}()
	}

	// 等待所有 goroutine 完成
	for i := 0; i < 10; i++ {
		<-done
	}
}
