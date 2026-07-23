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

// TestNewTimeSlot Tests the NewTimeSlot constructor
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

// TestNewTimeSlotWithNil tests how NewTimeSlot handles nil parameters
func TestNewTimeSlotWithNil(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	// Test end is nil
	ts1 := NewTimeSlot(&start, nil)
	if ts1.Start == nil {
		t.Error("Expected Start to be non-nil")
	}
	if ts1.End != nil {
		t.Error("Expected End to be nil")
	}

	// Test start is nil
	ts2 := NewTimeSlot(nil, &start)
	if ts2.Start != nil {
		t.Error("Expected Start to be nil")
	}
	if ts2.End == nil {
		t.Error("Expected End to be non-nil")
	}

	// Both tests are nil
	ts3 := NewTimeSlot(nil, nil)
	if ts3.Start != nil {
		t.Error("Expected Start to be nil")
	}
	if ts3.End != nil {
		t.Error("Expected End to be nil")
	}
}

// TestTimeSlotHash Testing the hash method
func TestTimeSlotHash(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)

	ts1 := NewTimeSlot(&start, &end)
	ts2 := NewTimeSlot(&start, &end)

	hash1 := ts1.Hash()
	hash2 := ts2.Hash()

	// The same time slots should produce the same hash value
	if hash1 != hash2 {
		t.Errorf("Expected same hash for identical time slots, got %d and %d", hash1, hash2)
	}

	// Different time slots should produce different hash values
	differentEnd := time.Date(2024, 1, 1, 14, 0, 0, 0, time.UTC)
	ts3 := NewTimeSlot(&start, &differentEnd)
	hash3 := ts3.Hash()

	if hash1 == hash3 {
		t.Errorf("Expected different hash for different time slots, got same hash %d", hash1)
	}
}

// TestTimeSlotContains Test Contains method
func TestTimeSlotContains(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)
	ts := NewTimeSlot(&start, &end)

	// Test boundary conditions
	if !ts.Contains(start) {
		t.Error("Expected Contains to return true for start time")
	}

	if ts.Contains(end) {
		t.Error("Expected Contains to return false for end time (exclusive)")
	}

	// Time within the test range
	midTime := time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC)
	if !ts.Contains(midTime) {
		t.Error("Expected Contains to return true for time within range")
	}

	// Testing time beyond the scope of the test
	beforeStart := time.Date(2024, 1, 1, 11, 59, 59, 0, time.UTC)
	if ts.Contains(beforeStart) {
		t.Error("Expected Contains to return false for time before start")
	}

	afterEnd := time.Date(2024, 1, 1, 13, 0, 1, 0, time.UTC)
	if ts.Contains(afterEnd) {
		t.Error("Expected Contains to return false for time after end")
	}
}

// TestGetStartTime Tests the GetStartTime method
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

	// Test nil TimeSlot
	var nilTS *TimeSlot
	nilStartTime := nilTS.GetStartTime()
	if nilStartTime != nil {
		t.Error("Expected GetStartTime to return nil for nil TimeSlot")
	}

	// Test the case where Start is nil
	tsWithNilStart := NewTimeSlot(nil, &end)
	nilStart := tsWithNilStart.GetStartTime()
	if nilStart != nil {
		t.Error("Expected GetStartTime to return nil when Start is nil")
	}
}

// TestGetEndTime Tests the GetEndTime method
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

	// Test nil TimeSlot
	var nilTS *TimeSlot
	nilEndTime := nilTS.GetEndTime()
	if nilEndTime != nil {
		t.Error("Expected GetEndTime to return nil for nil TimeSlot")
	}

	// Test the case where End is nil
	tsWithNilEnd := NewTimeSlot(&start, nil)
	nilEnd := tsWithNilEnd.GetEndTime()
	if nilEnd != nil {
		t.Error("Expected GetEndTime to return nil when End is nil")
	}
}

// TestWindowStart tests the WindowStart method
func TestWindowStart(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)
	ts := NewTimeSlot(&start, &end)

	windowStart := ts.WindowStart()
	expectedStart := start.UnixNano()

	if windowStart != expectedStart {
		t.Errorf("Expected window start %d, got %d", expectedStart, windowStart)
	}

	// Test nil TimeSlot
	var nilTS *TimeSlot
	nilWindowStart := nilTS.WindowStart()
	if nilWindowStart != 0 {
		t.Errorf("Expected WindowStart to return 0 for nil TimeSlot, got %d", nilWindowStart)
	}

	// Test the case where Start is nil
	tsWithNilStart := NewTimeSlot(nil, &end)
	nilStart := tsWithNilStart.WindowStart()
	if nilStart != 0 {
		t.Errorf("Expected WindowStart to return 0 when Start is nil, got %d", nilStart)
	}
}

// TestWindowEnd tests the WindowEnd method
func TestWindowEnd(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)
	ts := NewTimeSlot(&start, &end)

	windowEnd := ts.WindowEnd()
	expectedEnd := end.UnixNano()

	if windowEnd != expectedEnd {
		t.Errorf("Expected window end %d, got %d", expectedEnd, windowEnd)
	}

	// Test nil TimeSlot
	var nilTS *TimeSlot
	nilWindowEnd := nilTS.WindowEnd()
	if nilWindowEnd != 0 {
		t.Errorf("Expected WindowEnd to return 0 for nil TimeSlot, got %d", nilWindowEnd)
	}

	// Test the case where End is nil
	tsWithNilEnd := NewTimeSlot(&start, nil)
	nilEnd := tsWithNilEnd.WindowEnd()
	if nilEnd != 0 {
		t.Errorf("Expected WindowEnd to return 0 when End is nil, got %d", nilEnd)
	}
}

// TestTimeSlotEdgeCases Tests the boundaries of the TimeSlot
func TestTimeSlotEdgeCases(t *testing.T) {
	// Test the same start and end times
	sameTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	ts := NewTimeSlot(&sameTime, &sameTime)

	if ts.Contains(sameTime) {
		t.Error("Expected Contains to return false when start equals end")
	}

	// Cases where the test starts later than the end time
	start := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	invalidTS := NewTimeSlot(&start, &end)

	midTime := time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC)
	if invalidTS.Contains(midTime) {
		t.Error("Expected Contains to return false for invalid time slot (start > end)")
	}
}

// TestTimeSlotConcurrentAccess Tests for concurrent access to the TimeSlot
func TestTimeSlotConcurrentAccess(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)
	ts := NewTimeSlot(&start, &end)

	// Start multiple goroutines to concurrently access TimeSlot methods
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				// Testing concurrent access for various methods
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

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}
