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

// TestNewTimeSlot 测试 NewTimeSlot 构造函数
func TestNewTimeSlot(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)

	ts := NewTimeSlot(&start, &end)

	if ts == nil {
		t.Error("Expected TimeSlot to be non-nil")
	}

	if ts.Start == nil {
		t.Error("Expected Start to be non-nil")
	}

	if ts.End == nil {
		t.Error("Expected End to be non-nil")
	}

	if !ts.Start.Equal(start) {
		t.Errorf("Expected start time %v, got %v", start, *ts.Start)
	}

	if !ts.End.Equal(end) {
		t.Errorf("Expected end time %v, got %v", end, *ts.End)
	}
}

// TestNewTimeSlotWithNil 测试 NewTimeSlot 处理 nil 参数的情况
func TestNewTimeSlotWithNil(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	// 测试 end 为 nil
	ts1 := NewTimeSlot(&start, nil)
	if ts1.Start == nil {
		t.Error("Expected Start to be non-nil")
	}
	if ts1.End != nil {
		t.Error("Expected End to be nil")
	}

	// 测试 start 为 nil
	ts2 := NewTimeSlot(nil, &start)
	if ts2.Start != nil {
		t.Error("Expected Start to be nil")
	}
	if ts2.End == nil {
		t.Error("Expected End to be non-nil")
	}

	// 测试两者都为 nil
	ts3 := NewTimeSlot(nil, nil)
	if ts3.Start != nil {
		t.Error("Expected Start to be nil")
	}
	if ts3.End != nil {
		t.Error("Expected End to be nil")
	}
}

// TestTimeSlotHash 测试 Hash 方法
func TestTimeSlotHash(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)

	ts1 := NewTimeSlot(&start, &end)
	ts2 := NewTimeSlot(&start, &end)

	hash1 := ts1.Hash()
	hash2 := ts2.Hash()

	// 相同的时间槽应该产生相同的哈希值
	if hash1 != hash2 {
		t.Errorf("Expected same hash for identical time slots, got %d and %d", hash1, hash2)
	}

	// 不同的时间槽应该产生不同的哈希值
	differentEnd := time.Date(2024, 1, 1, 14, 0, 0, 0, time.UTC)
	ts3 := NewTimeSlot(&start, &differentEnd)
	hash3 := ts3.Hash()

	if hash1 == hash3 {
		t.Errorf("Expected different hash for different time slots, got same hash %d", hash1)
	}
}

// TestTimeSlotContains 测试 Contains 方法
func TestTimeSlotContains(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)
	ts := NewTimeSlot(&start, &end)

	// 测试边界情况
	if !ts.Contains(start) {
		t.Error("Expected Contains to return true for start time")
	}

	if ts.Contains(end) {
		t.Error("Expected Contains to return false for end time (exclusive)")
	}

	// 测试范围内的时间
	midTime := time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC)
	if !ts.Contains(midTime) {
		t.Error("Expected Contains to return true for time within range")
	}

	// 测试范围外的时间
	beforeStart := time.Date(2024, 1, 1, 11, 59, 59, 0, time.UTC)
	if ts.Contains(beforeStart) {
		t.Error("Expected Contains to return false for time before start")
	}

	afterEnd := time.Date(2024, 1, 1, 13, 0, 1, 0, time.UTC)
	if ts.Contains(afterEnd) {
		t.Error("Expected Contains to return false for time after end")
	}
}

// TestGetStartTime 测试 GetStartTime 方法
func TestGetStartTime(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)
	ts := NewTimeSlot(&start, &end)

	startTime := ts.GetStartTime()
	if startTime == nil {
		t.Error("Expected GetStartTime to return non-nil")
	}

	if !startTime.Equal(start) {
		t.Errorf("Expected start time %v, got %v", start, *startTime)
	}

	// 测试 nil TimeSlot
	var nilTS *TimeSlot
	nilStartTime := nilTS.GetStartTime()
	if nilStartTime != nil {
		t.Error("Expected GetStartTime to return nil for nil TimeSlot")
	}

	// 测试 Start 为 nil 的情况
	tsWithNilStart := NewTimeSlot(nil, &end)
	nilStart := tsWithNilStart.GetStartTime()
	if nilStart != nil {
		t.Error("Expected GetStartTime to return nil when Start is nil")
	}
}

// TestGetEndTime 测试 GetEndTime 方法
func TestGetEndTime(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)
	ts := NewTimeSlot(&start, &end)

	endTime := ts.GetEndTime()
	if endTime == nil {
		t.Error("Expected GetEndTime to return non-nil")
	}

	if !endTime.Equal(end) {
		t.Errorf("Expected end time %v, got %v", end, *endTime)
	}

	// 测试 nil TimeSlot
	var nilTS *TimeSlot
	nilEndTime := nilTS.GetEndTime()
	if nilEndTime != nil {
		t.Error("Expected GetEndTime to return nil for nil TimeSlot")
	}

	// 测试 End 为 nil 的情况
	tsWithNilEnd := NewTimeSlot(&start, nil)
	nilEnd := tsWithNilEnd.GetEndTime()
	if nilEnd != nil {
		t.Error("Expected GetEndTime to return nil when End is nil")
	}
}

// TestWindowStart 测试 WindowStart 方法
func TestWindowStart(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)
	ts := NewTimeSlot(&start, &end)

	windowStart := ts.WindowStart()
	expectedStart := start.UnixNano()

	if windowStart != expectedStart {
		t.Errorf("Expected window start %d, got %d", expectedStart, windowStart)
	}

	// 测试 nil TimeSlot
	var nilTS *TimeSlot
	nilWindowStart := nilTS.WindowStart()
	if nilWindowStart != 0 {
		t.Errorf("Expected WindowStart to return 0 for nil TimeSlot, got %d", nilWindowStart)
	}

	// 测试 Start 为 nil 的情况
	tsWithNilStart := NewTimeSlot(nil, &end)
	nilStart := tsWithNilStart.WindowStart()
	if nilStart != 0 {
		t.Errorf("Expected WindowStart to return 0 when Start is nil, got %d", nilStart)
	}
}

// TestWindowEnd 测试 WindowEnd 方法
func TestWindowEnd(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)
	ts := NewTimeSlot(&start, &end)

	windowEnd := ts.WindowEnd()
	expectedEnd := end.UnixNano()

	if windowEnd != expectedEnd {
		t.Errorf("Expected window end %d, got %d", expectedEnd, windowEnd)
	}

	// 测试 nil TimeSlot
	var nilTS *TimeSlot
	nilWindowEnd := nilTS.WindowEnd()
	if nilWindowEnd != 0 {
		t.Errorf("Expected WindowEnd to return 0 for nil TimeSlot, got %d", nilWindowEnd)
	}

	// 测试 End 为 nil 的情况
	tsWithNilEnd := NewTimeSlot(&start, nil)
	nilEnd := tsWithNilEnd.WindowEnd()
	if nilEnd != 0 {
		t.Errorf("Expected WindowEnd to return 0 when End is nil, got %d", nilEnd)
	}
}

// TestTimeSlotEdgeCases 测试 TimeSlot 的边界情况
func TestTimeSlotEdgeCases(t *testing.T) {
	// 测试相同的开始和结束时间
	sameTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	ts := NewTimeSlot(&sameTime, &sameTime)

	if ts.Contains(sameTime) {
		t.Error("Expected Contains to return false when start equals end")
	}

	// 测试开始时间晚于结束时间的情况
	start := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	invalidTS := NewTimeSlot(&start, &end)

	midTime := time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC)
	if invalidTS.Contains(midTime) {
		t.Error("Expected Contains to return false for invalid time slot (start > end)")
	}
}

// TestTimeSlotConcurrentAccess 测试 TimeSlot 的并发访问
func TestTimeSlotConcurrentAccess(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)
	ts := NewTimeSlot(&start, &end)

	// 启动多个 goroutine 并发访问 TimeSlot 方法
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				// 测试各种方法的并发访问
				_ = ts.Hash()
				_ = ts.Contains(start)
				_ = ts.GetStartTime()
				_ = ts.GetEndTime()
				_ = ts.WindowStart()
				_ = ts.WindowEnd()
			}
			done <- true
		}()
	}

	// 等待所有 goroutine 完成
	for i := 0; i < 10; i++ {
		<-done
	}
}