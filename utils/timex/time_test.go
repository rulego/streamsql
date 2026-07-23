// Copyright 2021 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package timex

import (
	"testing"
	"time"
)

func TestAlignTimeToWindow(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		size     time.Duration
		expected time.Time
	}{
		{
			name:     "对齐到1分钟窗口",
			input:    time.Date(2024, 1, 1, 12, 35, 56, 789000000, time.UTC),
			size:     3 * time.Minute,
			expected: time.Date(2024, 1, 1, 12, 33, 0, 0, time.UTC),
		},
		{
			name:     "对齐到5分钟窗口",
			input:    time.Date(2024, 1, 1, 12, 37, 56, 789000000, time.UTC),
			size:     5 * time.Minute,
			expected: time.Date(2024, 1, 1, 12, 35, 0, 0, time.UTC),
		},
		{
			name:     "对齐到1小时窗口",
			input:    time.Date(2024, 1, 1, 12, 34, 56, 789000000, time.UTC),
			size:     time.Hour,
			expected: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		},
		{
			name:     "对齐到1天窗口",
			input:    time.Date(2024, 1, 1, 12, 34, 56, 789000000, time.UTC),
			size:     24 * time.Hour,
			expected: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "零时刻对齐测试",
			input:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			size:     time.Hour,
			expected: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AlignTimeToWindow(tt.input, tt.size)
			if !got.Equal(tt.expected) {
				t.Errorf("AlignTimeToWindow() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestAlignTime tests the AlignTime function
func TestAlignTime(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		timeUnit time.Duration
		roundUp  bool
		expected time.Time
	}{
		{
			name:     "向下对齐到分钟",
			input:    time.Date(2024, 1, 1, 12, 35, 45, 0, time.UTC),
			timeUnit: time.Minute,
			roundUp:  false,
			expected: time.Date(2024, 1, 1, 12, 35, 0, 0, time.UTC),
		},
		{
			name:     "向上对齐到分钟",
			input:    time.Date(2024, 1, 1, 12, 35, 45, 0, time.UTC),
			timeUnit: time.Minute,
			roundUp:  true,
			expected: time.Date(2024, 1, 1, 12, 36, 0, 0, time.UTC),
		},
		{
			name:     "向下对齐到小时",
			input:    time.Date(2024, 1, 1, 12, 35, 45, 0, time.UTC),
			timeUnit: time.Hour,
			roundUp:  false,
			expected: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		},
		{
			name:     "向上对齐到小时",
			input:    time.Date(2024, 1, 1, 12, 35, 45, 0, time.UTC),
			timeUnit: time.Hour,
			roundUp:  true,
			expected: time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC),
		},
		{
			name:     "向下对齐到秒",
			input:    time.Date(2024, 1, 1, 12, 35, 45, 500000000, time.UTC),
			timeUnit: time.Second,
			roundUp:  false,
			expected: time.Date(2024, 1, 1, 12, 35, 45, 0, time.UTC),
		},
		{
			name:     "向上对齐到秒",
			input:    time.Date(2024, 1, 1, 12, 35, 45, 500000000, time.UTC),
			timeUnit: time.Second,
			roundUp:  true,
			expected: time.Date(2024, 1, 1, 12, 35, 46, 0, time.UTC),
		},
		{
			name:     "精确对齐时间向下",
			input:    time.Date(2024, 1, 1, 12, 35, 0, 0, time.UTC),
			timeUnit: time.Minute,
			roundUp:  false,
			expected: time.Date(2024, 1, 1, 12, 35, 0, 0, time.UTC),
		},
		{
			name:     "精确对齐时间向上",
			input:    time.Date(2024, 1, 1, 12, 35, 0, 0, time.UTC),
			timeUnit: time.Minute,
			roundUp:  true,
			expected: time.Date(2024, 1, 1, 12, 35, 0, 0, time.UTC),
		},
		{
			name:     "向下对齐到天",
			input:    time.Date(2024, 1, 1, 12, 35, 45, 0, time.UTC),
			timeUnit: 24 * time.Hour,
			roundUp:  false,
			expected: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "向上对齐到天",
			input:    time.Date(2024, 1, 1, 12, 35, 45, 0, time.UTC),
			timeUnit: 24 * time.Hour,
			roundUp:  true,
			expected: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AlignTime(tt.input, tt.timeUnit, tt.roundUp)
			if !got.Equal(tt.expected) {
				t.Errorf("AlignTime() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestAlignTimeEdgeCases tests the boundary state of the AlignTime function
func TestAlignTimeEdgeCases(t *testing.T) {
	// Test zero time
	zeroTime := time.Time{}
	result := AlignTime(zeroTime, time.Minute, true)
	expected := zeroTime.Truncate(time.Minute)
	if !result.Equal(expected) {
		t.Errorf("AlignTime with zero time failed: expected %v, got %v", expected, result)
	}

	// Testing is a very small unit of time
	testTime := time.Date(2024, 1, 1, 12, 35, 45, 123456789, time.UTC)
	result = AlignTime(testTime, time.Nanosecond, true)
	expected = testTime.Truncate(time.Nanosecond)
	if !result.Equal(expected) {
		t.Errorf("AlignTime with nanosecond failed: expected %v, got %v", expected, result)
	}

	// Testing is a very large unit of time
	result = AlignTime(testTime, 365*24*time.Hour, false) // One year
	expected = testTime.Truncate(365 * 24 * time.Hour)
	if !result.Equal(expected) {
		t.Errorf("AlignTime with year unit failed: expected %v, got %v", expected, result)
	}
}

// TestAlignTimeToWindowEdgeCases Tests the boundary status of the AlignTimeToWindow function
func TestAlignTimeToWindowEdgeCases(t *testing.T) {
	// Test zero time
	zeroTime := time.Time{}
	result := AlignTimeToWindow(zeroTime, time.Minute)
	if !result.Equal(zeroTime) {
		t.Errorf("AlignTimeToWindow with zero time failed: expected %v, got %v", zeroTime, result)
	}

	// Test with very small window sizes
	testTime := time.Date(2024, 1, 1, 12, 35, 45, 123456789, time.UTC)
	result = AlignTimeToWindow(testTime, time.Nanosecond)
	expected := testTime.Add(time.Duration(-testTime.UnixNano() % int64(time.Nanosecond)))
	if !result.Equal(expected) {
		t.Errorf("AlignTimeToWindow with nanosecond failed: expected %v, got %v", expected, result)
	}

	// Test window size with a size of 1 second
	result = AlignTimeToWindow(testTime, time.Second)
	expectedNano := testTime.UnixNano() - (testTime.UnixNano() % int64(time.Second))
	expected = time.Unix(0, expectedNano)
	if !result.Equal(expected) {
		t.Errorf("AlignTimeToWindow with second failed: expected %v, got %v", expected, result)
	}
}

// TestTimeFunctionsConcurrency: Tests the concurrency security of the time function
func TestTimeFunctionsConcurrency(t *testing.T) {
	testTime := time.Date(2024, 1, 1, 12, 35, 45, 123456789, time.UTC)

	// Start multiple goroutines to call the time function concurrently
	done := make(chan bool, 20)
	for i := 0; i < 20; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				// Test AlignTimeToWindow
				result1 := AlignTimeToWindow(testTime, time.Minute)
				expected1 := testTime.Add(time.Duration(-testTime.UnixNano() % int64(time.Minute)))
				if !result1.Equal(expected1) {
					t.Errorf("Concurrent AlignTimeToWindow failed: expected %v, got %v", expected1, result1)
					return
				}

				// Test AlignTime
				result2 := AlignTime(testTime, time.Minute, true)
				expected2 := testTime.Truncate(time.Minute).Add(time.Minute)
				if !result2.Equal(expected2) {
					t.Errorf("Concurrent AlignTime failed: expected %v, got %v", expected2, result2)
					return
				}
			}
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 20; i++ {
		<-done
	}
}
