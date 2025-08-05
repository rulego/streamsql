package rsql

import (
	"strings"
	"testing"
	"time"

	"github.com/rulego/streamsql/window"
)

// TestSelectStatement_ToStreamConfig 测试 SelectStatement 转换为 Stream 配置
func TestSelectStatement_ToStreamConfig(t *testing.T) {
	tests := []struct {
		name      string
		stmt      *SelectStatement
		wantErr   bool
		errMsg    string
		checkFunc func(*testing.T, *SelectStatement)
	}{
		{
			name: "基本 SELECT 语句",
			stmt: &SelectStatement{
				Fields: []Field{
					{Expression: "temperature", Alias: "temp"},
					{Expression: "humidity", Alias: ""},
				},
				Source: "sensor_data",
			},
			wantErr: false,
			checkFunc: func(t *testing.T, stmt *SelectStatement) {
				config, condition, err := stmt.ToStreamConfig()
				if err != nil {
					t.Errorf("ToStreamConfig() error = %v", err)
					return
				}
				if config == nil {
					t.Error("ToStreamConfig() returned nil config")
					return
				}
				if condition != "" {
					t.Errorf("Expected empty condition, got %s", condition)
				}
				if len(config.SimpleFields) != 2 {
					t.Errorf("Expected 2 simple fields, got %d", len(config.SimpleFields))
				}
			},
		},
		{
			name: "SELECT * 语句",
			stmt: &SelectStatement{
				SelectAll: true,
				Source:    "sensor_data",
			},
			wantErr: false,
			checkFunc: func(t *testing.T, stmt *SelectStatement) {
				config, _, err := stmt.ToStreamConfig()
				if err != nil {
					t.Errorf("ToStreamConfig() error = %v", err)
					return
				}
				if len(config.SimpleFields) != 1 || config.SimpleFields[0] != "*" {
					t.Errorf("Expected SimpleFields to contain '*', got %v", config.SimpleFields)
				}
			},
		},
		{
			name: "带聚合函数的语句",
			stmt: &SelectStatement{
				Fields: []Field{
					{Expression: "AVG(temperature)", Alias: "avg_temp"},
					{Expression: "COUNT(*)", Alias: "count"},
				},
				Source: "sensor_data",
				Window: WindowDefinition{
					Type:   "TUMBLINGWINDOW",
					Params: []interface{}{"10s"},
				},
			},
			wantErr: false,
			checkFunc: func(t *testing.T, stmt *SelectStatement) {
				config, _, err := stmt.ToStreamConfig()
				if err != nil {
					t.Errorf("ToStreamConfig() error = %v", err)
					return
				}
				if config.WindowConfig.Type != window.TypeTumbling {
					t.Errorf("Expected tumbling window, got %v", config.WindowConfig.Type)
				}
				if !config.NeedWindow {
					t.Error("Expected NeedWindow to be true")
				}
			},
		},
		{
			name: "缺少 FROM 子句",
			stmt: &SelectStatement{
				Fields: []Field{
					{Expression: "temperature"},
				},
			},
			wantErr: true,
			errMsg:  "missing FROM clause",
		},
		{
			name: "带 DISTINCT 的语句",
			stmt: &SelectStatement{
				Fields: []Field{
					{Expression: "category"},
				},
				Distinct: true,
				Source:   "products",
			},
			wantErr: false,
			checkFunc: func(t *testing.T, stmt *SelectStatement) {
				config, _, err := stmt.ToStreamConfig()
				if err != nil {
					t.Errorf("ToStreamConfig() error = %v", err)
					return
				}
				if !config.Distinct {
					t.Error("Expected Distinct to be true")
				}
			},
		},
		{
			name: "带 LIMIT 的语句",
			stmt: &SelectStatement{
				Fields: []Field{
					{Expression: "name"},
				},
				Source: "users",
				Limit:  100,
			},
			wantErr: false,
			checkFunc: func(t *testing.T, stmt *SelectStatement) {
				config, _, err := stmt.ToStreamConfig()
				if err != nil {
					t.Errorf("ToStreamConfig() error = %v", err)
					return
				}
				if config.Limit != 100 {
					t.Errorf("Expected Limit to be 100, got %d", config.Limit)
				}
			},
		},
		{
			name: "带 GROUP BY 的语句",
			stmt: &SelectStatement{
				Fields: []Field{
					{Expression: "category"},
					{Expression: "COUNT(*)", Alias: "count"},
				},
				Source:  "products",
				GroupBy: []string{"category"},
			},
			wantErr: false,
			checkFunc: func(t *testing.T, stmt *SelectStatement) {
				config, _, err := stmt.ToStreamConfig()
				if err != nil {
					t.Errorf("ToStreamConfig() error = %v", err)
					return
				}
				if len(config.GroupFields) != 1 || config.GroupFields[0] != "category" {
					t.Errorf("Expected GroupFields to contain 'category', got %v", config.GroupFields)
				}
			},
		},
		{
			name: "带 HAVING 的语句",
			stmt: &SelectStatement{
				Fields: []Field{
					{Expression: "category"},
					{Expression: "COUNT(*)", Alias: "count"},
				},
				Source:  "products",
				GroupBy: []string{"category"},
				Having:  "COUNT(*) > 10",
			},
			wantErr: false,
			checkFunc: func(t *testing.T, stmt *SelectStatement) {
				config, _, err := stmt.ToStreamConfig()
				if err != nil {
					t.Errorf("ToStreamConfig() error = %v", err)
					return
				}
				if config.Having != "COUNT(*) > 10" {
					t.Errorf("Expected Having to be 'COUNT(*) > 10', got %s", config.Having)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantErr {
				_, _, err := tt.stmt.ToStreamConfig()
				if err == nil {
					t.Error("ToStreamConfig() expected error but got none")
					return
				}
				if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ToStreamConfig() error = %v, expected to contain %s", err, tt.errMsg)
				}
			} else {
				if tt.checkFunc != nil {
					tt.checkFunc(t, tt.stmt)
				}
			}
		})
	}
}

// TestField 测试 Field 结构体
func TestField(t *testing.T) {
	field := Field{
		Expression: "temperature",
		Alias:      "temp",
		AggType:    "AVG",
	}

	if field.Expression != "temperature" {
		t.Errorf("Expected Expression to be 'temperature', got %s", field.Expression)
	}
	if field.Alias != "temp" {
		t.Errorf("Expected Alias to be 'temp', got %s", field.Alias)
	}
	if field.AggType != "AVG" {
		t.Errorf("Expected AggType to be 'AVG', got %s", field.AggType)
	}
}

// TestWindowDefinition 测试 WindowDefinition 结构体
func TestWindowDefinition(t *testing.T) {
	wd := WindowDefinition{
		Type:     "TUMBLINGWINDOW",
		Params:   []interface{}{"10s", "5s"},
		TsProp:   "timestamp",
		TimeUnit: time.Second,
	}

	if wd.Type != "TUMBLINGWINDOW" {
		t.Errorf("Expected Type to be 'TUMBLINGWINDOW', got %s", wd.Type)
	}
	if len(wd.Params) != 2 {
		t.Errorf("Expected 2 params, got %d", len(wd.Params))
	}
	if wd.TsProp != "timestamp" {
		t.Errorf("Expected TsProp to be 'timestamp', got %s", wd.TsProp)
	}
	if wd.TimeUnit != time.Second {
		t.Errorf("Expected TimeUnit to be Second, got %v", wd.TimeUnit)
	}
}

// TestIsAggregationFunction 测试聚合函数检测
func TestIsAggregationFunction(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		expected bool
	}{
		{
			name:     "简单字段",
			expr:     "temperature",
			expected: false,
		},
		{
			name:     "COUNT 函数",
			expr:     "COUNT(*)",
			expected: true,
		},
		{
			name:     "AVG 函数",
			expr:     "AVG(temperature)",
			expected: true,
		},
		{
			name:     "SUM 函数",
			expr:     "SUM(value)",
			expected: true,
		},
		{
			name:     "MAX 函数",
			expr:     "MAX(score)",
			expected: true,
		},
		{
			name:     "MIN 函数",
			expr:     "MIN(price)",
			expected: true,
		},
		{
			name:     "空表达式",
			expr:     "",
			expected: false,
		},
		{
			name:     "包含括号但非函数",
			expr:     "(temperature + humidity)",
			expected: false, // 算术表达式，非聚合函数
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isAggregationFunction(tt.expr)
			if result != tt.expected {
				t.Errorf("isAggregationFunction(%s) = %v, expected %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// TestExtractFieldOrder 测试字段顺序提取
func TestExtractFieldOrder(t *testing.T) {
	fields := []Field{
		{Expression: "temperature", Alias: "temp"},
		{Expression: "humidity", Alias: ""},
		{Expression: "'sensor_id'", Alias: "id"},
		{Expression: "COUNT(*)", Alias: "count"},
	}

	fieldOrder := extractFieldOrder(fields)
	expected := []string{"temp", "humidity", "id", "count"}

	if len(fieldOrder) != len(expected) {
		t.Errorf("Expected %d fields, got %d", len(expected), len(fieldOrder))
		return
	}

	for i, field := range fieldOrder {
		if field != expected[i] {
			t.Errorf("Expected field %d to be %s, got %s", i, expected[i], field)
		}
	}
}

// TestExtractGroupFields 测试 GROUP BY 字段提取
func TestExtractGroupFields(t *testing.T) {
	stmt := &SelectStatement{
		GroupBy: []string{"category", "region", "COUNT(*)", "status"},
	}

	groupFields := extractGroupFields(stmt)
	expected := []string{"category", "region", "status"}

	if len(groupFields) != len(expected) {
		t.Errorf("Expected %d group fields, got %d", len(expected), len(groupFields))
		return
	}

	for i, field := range groupFields {
		if field != expected[i] {
			t.Errorf("Expected group field %d to be %s, got %s", i, expected[i], field)
		}
	}
}

// TestBuildSelectFields 测试构建选择字段
func TestBuildSelectFields(t *testing.T) {
	fields := []Field{
		{Expression: "AVG(temperature)", Alias: "avg_temp"},
		{Expression: "COUNT(*)", Alias: "count"},
		{Expression: "category", Alias: "cat"},
	}

	aggMap, fieldMap := buildSelectFields(fields)

	// 检查聚合映射
	if len(aggMap) == 0 {
		t.Error("Expected aggregation map to have entries")
	}

	// 检查字段映射
	if len(fieldMap) == 0 {
		t.Error("Expected field map to have entries")
	}

	// 验证别名映射
	if _, exists := fieldMap["avg_temp"]; !exists {
		t.Error("Expected field map to contain 'avg_temp'")
	}
	if _, exists := fieldMap["count"]; !exists {
		t.Error("Expected field map to contain 'count'")
	}
}

// TestSelectStatementEdgeCases 测试边界情况
func TestSelectStatementEdgeCases(t *testing.T) {
	// 测试空字段列表
	stmt := &SelectStatement{
		Fields: []Field{},
		Source: "test_table",
	}

	config, condition, err := stmt.ToStreamConfig()
	if err != nil {
		t.Errorf("ToStreamConfig() with empty fields error = %v", err)
		return
	}
	if config == nil {
		t.Error("ToStreamConfig() returned nil config")
		return
	}
	if condition != "" {
		t.Errorf("Expected empty condition, got %s", condition)
	}

	// 测试复杂窗口类型
	stmt2 := &SelectStatement{
		Fields: []Field{
			{Expression: "COUNT(*)", Alias: "count"},
		},
		Source: "test_table",
		Window: WindowDefinition{
			Type:   "SESSIONWINDOW",
			Params: []interface{}{"30s"},
		},
		GroupBy: []string{"user_id"},
	}

	config2, _, err := stmt2.ToStreamConfig()
	if err != nil {
		t.Errorf("ToStreamConfig() with session window error = %v", err)
		return
	}
	if config2.WindowConfig.Type != window.TypeSession {
		t.Errorf("Expected session window, got %v", config2.WindowConfig.Type)
	}
	if config2.WindowConfig.GroupByKey != "user_id" {
		t.Errorf("Expected GroupByKey to be 'user_id', got %s", config2.WindowConfig.GroupByKey)
	}
}

// TestSelectStatementConcurrency 测试并发安全性
func TestSelectStatementConcurrency(t *testing.T) {
	stmt := &SelectStatement{
		Fields: []Field{
			{Expression: "temperature", Alias: "temp"},
			{Expression: "COUNT(*)", Alias: "count"},
		},
		Source: "sensor_data",
		Window: WindowDefinition{
			Type:   "TUMBLINGWINDOW",
			Params: []interface{}{"10s"},
		},
	}

	// 启动多个 goroutine 并发调用 ToStreamConfig
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				config, condition, err := stmt.ToStreamConfig()
				if err != nil {
					t.Errorf("Concurrent ToStreamConfig() error = %v", err)
					return
				}
				if config == nil {
					t.Error("Concurrent ToStreamConfig() returned nil config")
					return
				}
				if condition != "" {
					t.Errorf("Concurrent ToStreamConfig() expected empty condition, got %s", condition)
					return
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