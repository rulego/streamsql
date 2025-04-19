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
