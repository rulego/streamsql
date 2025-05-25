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
	"time"
)

type TimeSlot struct {
	Start *time.Time
	End   *time.Time
}

func NewTimeSlot(start, end *time.Time) *TimeSlot {
	return &TimeSlot{
		Start: start,
		End:   end,
	}
}

// Hash 生成槽位的哈希值
func (ts TimeSlot) Hash() uint64 {
	// 将开始时间和结束时间转换为 Unix 时间戳（纳秒级）
	startNano := ts.Start.UnixNano()
	endNano := ts.End.UnixNano()

	// 使用简单但高效的哈希算法
	// 将两个时间戳组合成一个唯一的哈希值
	hash := uint64(startNano)
	hash = (hash << 32) | (hash >> 32)
	hash = hash ^ uint64(endNano)

	return hash
}

// Contains 检查给定时间是否在槽位范围内
func (ts TimeSlot) Contains(t time.Time) bool {
	return (t.Equal(*ts.Start) || t.After(*ts.Start)) &&
		t.Before(*ts.End)
}

func (ts *TimeSlot) GetStartTime() *time.Time {
	if ts == nil || ts.Start == nil {
		return nil
	}
	return ts.Start
}

func (ts *TimeSlot) GetEndTime() *time.Time {
	if ts == nil || ts.End == nil {
		return nil
	}
	return ts.End
}

func (ts *TimeSlot) WindowStart() int64 {
	if ts == nil || ts.Start == nil {
		return 0
	}
	return ts.Start.UnixNano()
}

func (ts *TimeSlot) WindowEnd() int64 {
	if ts == nil || ts.End == nil {
		return 0
	}
	return ts.End.UnixNano()
}
