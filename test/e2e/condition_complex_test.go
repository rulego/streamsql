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

package streamsql

import (
	"testing"
)

// TestComplexConditions 测试复杂条件和组合条件的支持
func TestComplexConditions(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		data        map[string]interface{}
		expectMatch bool
		description string
	}{
		{
			name: "简单AND条件",
			sql:  "SELECT * FROM stream WHERE temperature > 20 AND humidity < 80",
			data: map[string]interface{}{
				"temperature": 25.0,
				"humidity":    70.0,
			},
			expectMatch: true,
			description: "两个条件都满足",
		},
		{
			name: "简单OR条件",
			sql:  "SELECT * FROM stream WHERE temperature > 30 OR humidity > 90",
			data: map[string]interface{}{
				"temperature": 25.0,
				"humidity":    95.0,
			},
			expectMatch: true,
			description: "其中一个条件满足",
		},
		{
			name: "复杂组合条件 - 括号优先级",
			sql:  "SELECT * FROM stream WHERE (temperature > 20 AND humidity < 80) OR status == 'active'",
			data: map[string]interface{}{
				"temperature": 15.0,
				"humidity":    70.0,
				"status":      "active",
			},
			expectMatch: true,
			description: "第一组条件不满足，但status条件满足",
		},
		{
			name: "多重AND条件",
			sql:  "SELECT * FROM stream WHERE temperature > 20 AND humidity < 80 AND pressure > 1000",
			data: map[string]interface{}{
				"temperature": 25.0,
				"humidity":    70.0,
				"pressure":    1050.0,
			},
			expectMatch: true,
			description: "三个条件都满足",
		},
		{
			name: "多重OR条件",
			sql:  "SELECT * FROM stream WHERE temperature > 40 OR humidity > 90 OR pressure < 900",
			data: map[string]interface{}{
				"temperature": 25.0,
				"humidity":    70.0,
				"pressure":    850.0,
			},
			expectMatch: true,
			description: "第三个条件满足",
		},
		{
			name: "复杂嵌套条件",
			sql:  "SELECT * FROM stream WHERE (temperature > 20 AND humidity < 80) OR (pressure > 1000 AND status == 'normal')",
			data: map[string]interface{}{
				"temperature": 15.0,
				"humidity":    85.0,
				"pressure":    1100.0,
				"status":      "normal",
			},
			expectMatch: true,
			description: "第一组条件不满足，第二组条件满足",
		},
		{
			name: "字符串条件组合",
			sql:  "SELECT * FROM stream WHERE deviceId == 'sensor001' AND location == 'room1'",
			data: map[string]interface{}{
				"deviceId": "sensor001",
				"location": "room1",
			},
			expectMatch: true,
			description: "字符串相等条件组合",
		},
		{
			name: "混合类型条件",
			sql:  "SELECT * FROM stream WHERE temperature > 20 AND deviceId == 'sensor001' AND active == true",
			data: map[string]interface{}{
				"temperature": 25.0,
				"deviceId":    "sensor001",
				"active":      true,
			},
			expectMatch: true,
			description: "数字、字符串、布尔值混合条件",
		},
		{
			name: "NOT条件组合",
			sql:  "SELECT * FROM stream WHERE temperature >= 20 AND humidity > 50",
			data: map[string]interface{}{
				"temperature": 25.0,
				"humidity":    60.0,
			},
			expectMatch: true,
			description: "NOT条件与其他条件组合",
		},
		{
			name: "条件不满足的情况",
			sql:  "SELECT * FROM stream WHERE temperature > 30 AND humidity < 50",
			data: map[string]interface{}{
				"temperature": 25.0,
				"humidity":    60.0,
			},
			expectMatch: false,
			description: "两个条件都不满足",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ssql := New()
			err := ssql.Execute(tt.sql)
			if err != nil {
				t.Fatalf("Execute() failed: %v", err)
			}

			// 使用EmitSync进行同步测试
			result, err := ssql.EmitSync(tt.data)
			if err != nil {
				t.Fatalf("EmitSync() failed: %v", err)
			}

			// 检查结果是否符合预期
			if tt.expectMatch {
				if result == nil {
					t.Errorf("Expected match but got nil result. %s", tt.description)
				}
			} else {
				if result != nil {
					t.Errorf("Expected no match but got result: %v. %s", result, tt.description)
				}
			}

			// 清理资源
			ssql.Stop()
		})
	}
}

// TestComplexConditionsWithLike 测试LIKE条件与其他条件的组合
func TestComplexConditionsWithLike(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		data        map[string]interface{}
		expectMatch bool
	}{
		{
			name: "LIKE与AND条件组合",
			sql:  "SELECT * FROM stream WHERE deviceId LIKE 'sensor%' AND temperature > 20",
			data: map[string]interface{}{
				"deviceId":    "sensor001",
				"temperature": 25.0,
			},
			expectMatch: true,
		},
		{
			name: "LIKE与OR条件组合",
			sql:  "SELECT * FROM stream WHERE deviceId LIKE 'temp%' OR location LIKE '%room%'",
			data: map[string]interface{}{
				"deviceId": "sensor001",
				"location": "meeting_room_1",
			},
			expectMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ssql := New()
			err := ssql.Execute(tt.sql)
			if err != nil {
				t.Fatalf("Execute() failed: %v", err)
			}

			result, err := ssql.EmitSync(tt.data)
			if err != nil {
				t.Fatalf("EmitSync() failed: %v", err)
			}

			if tt.expectMatch {
				if result == nil {
					t.Errorf("Expected match but got nil result")
				}
			} else {
				if result != nil {
					t.Errorf("Expected no match but got result: %v", result)
				}
			}

			ssql.Stop()
		})
	}
}

// TestComplexConditionsWithNullChecks 测试NULL检查与其他条件的组合
func TestComplexConditionsWithNullChecks(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		data        map[string]interface{}
		expectMatch bool
	}{
		{
			name: "IS NULL与AND条件组合",
			sql:  "SELECT * FROM stream WHERE description IS NULL AND temperature > 20",
			data: map[string]interface{}{
				"temperature": 25.0,
				// description字段缺失，应该被视为null
			},
			expectMatch: true,
		},
		{
			name: "IS NOT NULL与OR条件组合",
			sql:  "SELECT * FROM stream WHERE description IS NOT NULL OR temperature > 30",
			data: map[string]interface{}{
				"temperature": 35.0,
				// description字段缺失，但temperature条件满足
			},
			expectMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ssql := New()
			err := ssql.Execute(tt.sql)
			if err != nil {
				t.Fatalf("Execute() failed: %v", err)
			}

			result, err := ssql.EmitSync(tt.data)
			if err != nil {
				t.Fatalf("EmitSync() failed: %v", err)
			}

			if tt.expectMatch {
				if result == nil {
					t.Errorf("Expected match but got nil result")
				}
			} else {
				if result != nil {
					t.Errorf("Expected no match but got result: %v", result)
				}
			}

			ssql.Stop()
		})
	}
}
