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

	"github.com/rulego/streamsql/aggregator"
	"github.com/stretchr/testify/assert"
)

// TestConfig 测试 Config 结构体的基本功能
func TestConfig(t *testing.T) {
	config := &Config{
		WindowConfig: WindowConfig{
			Type:       "tumbling",
			Params:     map[string]interface{}{"size": "1m"},
			TsProp:     "timestamp",
			TimeUnit:   time.Minute,
			GroupByKey: "user_id",
		},
		GroupFields:  []string{"user_id", "category"},
		SelectFields: map[string]aggregator.AggregateType{"count": aggregator.Count, "sum": aggregator.Sum},
		FieldAlias:   map[string]string{"user_id": "uid", "category": "cat"},
		SimpleFields: []string{"name", "email"},
		FieldExpressions: map[string]FieldExpression{
			"total": {
				Field:      "amount",
				Expression: "amount * 1.1",
				Fields:     []string{"amount"},
			},
		},
		FieldOrder: []string{"user_id", "category", "count", "sum"},
		Where:      "amount > 100",
		Having:     "count > 5",
		NeedWindow: true,
		Distinct:   false,
		Limit:      1000,
		Projections: []Projection{
			{
				SourceType: SourceGroupKey,
				InputName:  "user_id",
				OutputName: "uid",
			},
		},
		PerformanceConfig: PerformanceConfig{
			BufferConfig: BufferConfig{
				DataChannelSize:   100,
				ResultChannelSize: 100,
				WindowOutputSize:  50,
			},
			MonitoringConfig: MonitoringConfig{
				EnableMonitoring: true,
			},
			WorkerConfig: WorkerConfig{
				SinkWorkerCount: 4,
			},
		},
	}

	// 验证基本字段
	if config.WindowConfig.Type != "tumbling" {
		t.Errorf("Expected window type 'tumbling', got '%s'", config.WindowConfig.Type)
	}

	if len(config.GroupFields) != 2 {
		t.Errorf("Expected 2 group fields, got %d", len(config.GroupFields))
	}

	if config.GroupFields[0] != "user_id" || config.GroupFields[1] != "category" {
		t.Errorf("Group fields mismatch: %v", config.GroupFields)
	}

	if len(config.SelectFields) != 2 {
		t.Errorf("Expected 2 select fields, got %d", len(config.SelectFields))
	}

	if config.SelectFields["count"] != aggregator.Count {
		t.Errorf("Expected Count aggregator for 'count' field")
	}

	if config.SelectFields["sum"] != aggregator.Sum {
		t.Errorf("Expected Sum aggregator for 'sum' field")
	}

	if config.FieldAlias["user_id"] != "uid" {
		t.Errorf("Expected alias 'uid' for 'user_id', got '%s'", config.FieldAlias["user_id"])
	}

	if !config.NeedWindow {
		t.Error("Expected NeedWindow to be true")
	}

	if config.Distinct {
		t.Error("Expected Distinct to be false")
	}

	if config.Limit != 1000 {
		t.Errorf("Expected limit 1000, got %d", config.Limit)
	}

	if config.Where != "amount > 100" {
		t.Errorf("Expected where clause 'amount > 100', got '%s'", config.Where)
	}

	if config.Having != "count > 5" {
		t.Errorf("Expected having clause 'count > 5', got '%s'", config.Having)
	}
}

// TestWindowConfig 测试 WindowConfig 结构体
func TestWindowConfig(t *testing.T) {
	windowConfig := WindowConfig{
		Type:       "sliding",
		Params:     map[string]interface{}{"size": "5m", "interval": "1m"},
		TsProp:     "event_time",
		TimeUnit:   time.Minute,
		GroupByKey: "session_id",
	}

	if windowConfig.Type != "sliding" {
		t.Errorf("Expected window type 'sliding', got '%s'", windowConfig.Type)
	}

	if windowConfig.TsProp != "event_time" {
		t.Errorf("Expected timestamp property 'event_time', got '%s'", windowConfig.TsProp)
	}

	if windowConfig.TimeUnit != time.Minute {
		t.Errorf("Expected time unit 'Minute', got '%v'", windowConfig.TimeUnit)
	}

	if windowConfig.GroupByKey != "session_id" {
		t.Errorf("Expected group by key 'session_id', got '%s'", windowConfig.GroupByKey)
	}

	if len(windowConfig.Params) != 2 {
		t.Errorf("Expected 2 parameters, got %d", len(windowConfig.Params))
	}

	if windowConfig.Params["size"] != "5m" {
		t.Errorf("Expected size parameter '5m', got '%v'", windowConfig.Params["size"])
	}

	if windowConfig.Params["interval"] != "1m" {
		t.Errorf("Expected interval parameter '1m', got '%v'", windowConfig.Params["interval"])
	}
}

// TestFieldExpression 测试 FieldExpression 结构体
func TestFieldExpression(t *testing.T) {
	fieldExpr := FieldExpression{
		Field:      "total_amount",
		Expression: "price * quantity + tax",
		Fields:     []string{"price", "quantity", "tax"},
	}

	if fieldExpr.Field != "total_amount" {
		t.Errorf("Expected field 'total_amount', got '%s'", fieldExpr.Field)
	}

	if fieldExpr.Expression != "price * quantity + tax" {
		t.Errorf("Expected expression 'price * quantity + tax', got '%s'", fieldExpr.Expression)
	}

	if len(fieldExpr.Fields) != 3 {
		t.Errorf("Expected 3 fields, got %d", len(fieldExpr.Fields))
	}

	expectedFields := []string{"price", "quantity", "tax"}
	for i, field := range fieldExpr.Fields {
		if field != expectedFields[i] {
			t.Errorf("Expected field '%s' at index %d, got '%s'", expectedFields[i], i, field)
		}
	}
}

// TestProjection 测试 Projection 结构体
func TestProjection(t *testing.T) {
	projection := Projection{
		SourceType: SourceGroupKey,
		InputName:  "user_name",
		OutputName: "name",
	}

	if projection.SourceType != SourceGroupKey {
		t.Errorf("Expected projection type '%v', got '%v'", SourceGroupKey, projection.SourceType)
	}

	if projection.InputName != "user_name" {
		t.Errorf("Expected input name 'user_name', got '%s'", projection.InputName)
	}

	if projection.OutputName != "name" {
		t.Errorf("Expected output name 'name', got '%s'", projection.OutputName)
	}
}

// TestPerformanceConfig 测试 PerformanceConfig 结构体
func TestPerformanceConfig(t *testing.T) {
	perfConfig := PerformanceConfig{
		BufferConfig: BufferConfig{
			DataChannelSize:   500,
			ResultChannelSize: 500,
			WindowOutputSize:  100,
		},
		MonitoringConfig: MonitoringConfig{
			EnableMonitoring:    true,
			StatsUpdateInterval: time.Second * 10,
		},
		WorkerConfig: WorkerConfig{
			SinkWorkerCount: 8,
		},
	}

	if perfConfig.BufferConfig.DataChannelSize != 500 {
		t.Errorf("Expected data channel size 500, got %d", perfConfig.BufferConfig.DataChannelSize)
	}

	if perfConfig.MonitoringConfig.StatsUpdateInterval != time.Second*10 {
		t.Errorf("Expected stats update interval 10s, got %v", perfConfig.MonitoringConfig.StatsUpdateInterval)
	}

	if perfConfig.BufferConfig.ResultChannelSize != 500 {
		t.Errorf("Expected result channel size 500, got %d", perfConfig.BufferConfig.ResultChannelSize)
	}

	if !perfConfig.MonitoringConfig.EnableMonitoring {
		t.Error("Expected EnableMonitoring to be true")
	}

	if perfConfig.WorkerConfig.SinkWorkerCount != 8 {
		t.Errorf("Expected sink worker count 8, got %d", perfConfig.WorkerConfig.SinkWorkerCount)
	}
}

// TestConfigDefaults 测试 Config 结构体的默认值
func TestConfigDefaults(t *testing.T) {
	config := &Config{}

	// 验证默认值
	if config.NeedWindow {
		t.Error("Expected NeedWindow default to be false")
	}

	if config.Distinct {
		t.Error("Expected Distinct default to be false")
	}

	if config.Limit != 0 {
		t.Errorf("Expected Limit default to be 0, got %d", config.Limit)
	}

	if len(config.GroupFields) != 0 {
		t.Errorf("Expected empty GroupFields, got %v", config.GroupFields)
	}

	if len(config.SelectFields) != 0 {
		t.Errorf("Expected empty SelectFields, got %v", config.SelectFields)
	}

	if len(config.FieldAlias) != 0 {
		t.Errorf("Expected empty FieldAlias, got %v", config.FieldAlias)
	}
}

// TestNewConfig 测试NewConfig函数
func TestNewConfig(t *testing.T) {
	config := NewConfig()

	// 验证默认值
	assert.False(t, config.NeedWindow)
	assert.False(t, config.Distinct)
	assert.Equal(t, 0, config.Limit)
	assert.Empty(t, config.GroupFields)
	assert.Empty(t, config.SelectFields)
	assert.Empty(t, config.FieldAlias)
	assert.Empty(t, config.SimpleFields)
	assert.Empty(t, config.FieldExpressions)
	assert.Empty(t, config.FieldOrder)
	assert.Empty(t, config.Where)
	assert.Empty(t, config.Having)
	assert.Empty(t, config.Projections)
}

// TestNewConfigWithPerformance 测试NewConfigWithPerformance函数
func TestNewConfigWithPerformance(t *testing.T) {
	perfConfig := PerformanceConfig{
		BufferConfig: BufferConfig{
			DataChannelSize:   200,
			ResultChannelSize: 150,
		},
		MonitoringConfig: MonitoringConfig{
			EnableMonitoring: true,
		},
	}

	config := NewConfigWithPerformance(perfConfig)

	// 验证性能配置已设置
	assert.Equal(t, 200, config.PerformanceConfig.BufferConfig.DataChannelSize)
	assert.Equal(t, 150, config.PerformanceConfig.BufferConfig.ResultChannelSize)
	assert.True(t, config.PerformanceConfig.MonitoringConfig.EnableMonitoring)

	// 验证其他字段为默认值
	assert.False(t, config.NeedWindow)
	assert.False(t, config.Distinct)
}

// TestDefaultPerformanceConfig 测试DefaultPerformanceConfig函数
func TestDefaultPerformanceConfig(t *testing.T) {
	config := DefaultPerformanceConfig()

	// 验证缓冲区配置
	assert.Equal(t, 1000, config.BufferConfig.DataChannelSize)
	assert.Equal(t, 100, config.BufferConfig.ResultChannelSize)
	assert.Equal(t, 50, config.BufferConfig.WindowOutputSize)
	assert.False(t, config.BufferConfig.EnableDynamicResize)
	assert.Equal(t, 10000, config.BufferConfig.MaxBufferSize)
	assert.Equal(t, 0.8, config.BufferConfig.UsageThreshold)

	// 验证溢出配置
	assert.Equal(t, "drop", config.OverflowConfig.Strategy)
	assert.Equal(t, 5*time.Second, config.OverflowConfig.BlockTimeout)
	assert.True(t, config.OverflowConfig.AllowDataLoss)

	// 验证工作器配置
	assert.Equal(t, 4, config.WorkerConfig.SinkPoolSize)
	assert.Equal(t, 2, config.WorkerConfig.SinkWorkerCount)
	assert.Equal(t, 10, config.WorkerConfig.MaxRetryRoutines)

	// 验证监控配置
	assert.False(t, config.MonitoringConfig.EnableMonitoring)
	assert.Equal(t, 30*time.Second, config.MonitoringConfig.StatsUpdateInterval)
	assert.False(t, config.MonitoringConfig.EnableDetailedStats)
}

// TestHighPerformanceConfig 测试HighPerformanceConfig函数
func TestHighPerformanceConfig(t *testing.T) {
	config := HighPerformanceConfig()

	// 验证高性能配置
	assert.Equal(t, 5000, config.BufferConfig.DataChannelSize)
	assert.Equal(t, 500, config.BufferConfig.ResultChannelSize)
	assert.Equal(t, 200, config.BufferConfig.WindowOutputSize)
	assert.Equal(t, "expand", config.OverflowConfig.Strategy)
	assert.Equal(t, 8, config.WorkerConfig.SinkPoolSize)
	assert.Equal(t, 4, config.WorkerConfig.SinkWorkerCount)
	assert.True(t, config.MonitoringConfig.EnableMonitoring)
}

// TestLowLatencyConfig 测试LowLatencyConfig函数
func TestLowLatencyConfig(t *testing.T) {
	config := LowLatencyConfig()

	// 验证低延迟配置
	assert.Equal(t, 100, config.BufferConfig.DataChannelSize)
	assert.Equal(t, 50, config.BufferConfig.ResultChannelSize)
	assert.Equal(t, 20, config.BufferConfig.WindowOutputSize)
	assert.Equal(t, "block", config.OverflowConfig.Strategy)
	assert.Equal(t, 1*time.Second, config.OverflowConfig.BlockTimeout)
	assert.True(t, config.MonitoringConfig.EnableMonitoring)
	assert.Equal(t, 1*time.Second, config.MonitoringConfig.StatsUpdateInterval)
}

// TestBufferConfig 测试BufferConfig结构体
func TestBufferConfig(t *testing.T) {
	config := BufferConfig{
		DataChannelSize:     1000,
		ResultChannelSize:   100,
		WindowOutputSize:    50,
		EnableDynamicResize: true,
		MaxBufferSize:       5000,
		UsageThreshold:      0.75,
	}

	assert.Equal(t, 1000, config.DataChannelSize)
	assert.Equal(t, 100, config.ResultChannelSize)
	assert.Equal(t, 50, config.WindowOutputSize)
	assert.True(t, config.EnableDynamicResize)
	assert.Equal(t, 5000, config.MaxBufferSize)
	assert.Equal(t, 0.75, config.UsageThreshold)
}

// TestWorkerConfig 测试WorkerConfig结构体
func TestWorkerConfig(t *testing.T) {
	config := WorkerConfig{
		SinkPoolSize:     8,
		SinkWorkerCount:  4,
		MaxRetryRoutines: 20,
	}

	assert.Equal(t, 8, config.SinkPoolSize)
	assert.Equal(t, 4, config.SinkWorkerCount)
	assert.Equal(t, 20, config.MaxRetryRoutines)
}

// TestMonitoringConfig 测试MonitoringConfig结构体
func TestMonitoringConfig(t *testing.T) {
	warningThresholds := WarningThresholds{
		DropRateWarning:     0.01,
		DropRateCritical:    0.05,
		BufferUsageWarning:  0.8,
		BufferUsageCritical: 0.95,
	}

	config := MonitoringConfig{
		EnableMonitoring:    true,
		StatsUpdateInterval: 15 * time.Second,
		EnableDetailedStats: true,
		WarningThresholds:   warningThresholds,
	}

	assert.True(t, config.EnableMonitoring)
	assert.Equal(t, 15*time.Second, config.StatsUpdateInterval)
	assert.True(t, config.EnableDetailedStats)
	assert.Equal(t, 0.01, config.WarningThresholds.DropRateWarning)
	assert.Equal(t, 0.05, config.WarningThresholds.DropRateCritical)
	assert.Equal(t, 0.8, config.WarningThresholds.BufferUsageWarning)
	assert.Equal(t, 0.95, config.WarningThresholds.BufferUsageCritical)
}

// TestProjectionSourceType 测试ProjectionSourceType枚举
func TestProjectionSourceType(t *testing.T) {
	assert.Equal(t, ProjectionSourceType(0), SourceGroupKey)
	assert.Equal(t, ProjectionSourceType(1), SourceAggregateResult)
	assert.Equal(t, ProjectionSourceType(2), SourceWindowProperty)
}

// TestComplexConfig 测试复杂配置组合
func TestComplexConfig(t *testing.T) {
	config := Config{
		WindowConfig: WindowConfig{
			Type:       "sliding",
			Params:     map[string]interface{}{"size": "5m", "slide": "1m"},
			TsProp:     "event_time",
			TimeUnit:   time.Minute,
			GroupByKey: "session_id",
		},
		GroupFields: []string{"user_id", "product_category", "region"},
		SelectFields: map[string]aggregator.AggregateType{
			"total_amount": aggregator.Sum,
			"order_count":  aggregator.Count,
			"avg_price":    aggregator.Avg,
			"max_price":    aggregator.Max,
			"min_price":    aggregator.Min,
		},
		FieldAlias: map[string]string{
			"user_id":          "uid",
			"product_category": "category",
			"total_amount":     "total",
		},
		SimpleFields: []string{"user_name", "email", "phone"},
		FieldExpressions: map[string]FieldExpression{
			"discounted_total": {
				Field:      "total_amount",
				Expression: "total_amount * 0.9",
				Fields:     []string{"total_amount"},
			},
			"price_per_item": {
				Field:      "avg_price",
				Expression: "total_amount / order_count",
				Fields:     []string{"total_amount", "order_count"},
			},
		},
		FieldOrder: []string{"uid", "category", "region", "total", "order_count"},
		Where:      "total_amount > 100 AND region IN ('US', 'EU')",
		Having:     "order_count >= 5 AND avg_price > 50",
		NeedWindow: true,
		Distinct:   true,
		Limit:      5000,
		Projections: []Projection{
			{
				SourceType: SourceGroupKey,
				InputName:  "user_id",
				OutputName: "uid",
			},
			{
				SourceType: SourceAggregateResult,
				InputName:  "total_amount",
				OutputName: "total",
			},
			{
				SourceType: SourceWindowProperty,
				InputName:  "window_start",
				OutputName: "start_time",
			},
		},
		PerformanceConfig: HighPerformanceConfig(),
	}

	// 验证复杂配置
	assert.Equal(t, "sliding", config.WindowConfig.Type)
	assert.Len(t, config.GroupFields, 3)
	assert.Len(t, config.SelectFields, 5)
	assert.Len(t, config.FieldAlias, 3)
	assert.Len(t, config.SimpleFields, 3)
	assert.Len(t, config.FieldExpressions, 2)
	assert.Len(t, config.FieldOrder, 5)
	assert.True(t, config.NeedWindow)
	assert.True(t, config.Distinct)
	assert.Equal(t, 5000, config.Limit)
	assert.Len(t, config.Projections, 3)

	// 验证字段表达式
	discountedExpr := config.FieldExpressions["discounted_total"]
	assert.Equal(t, "total_amount", discountedExpr.Field)
	assert.Equal(t, "total_amount * 0.9", discountedExpr.Expression)
	assert.Equal(t, []string{"total_amount"}, discountedExpr.Fields)

	pricePerItemExpr := config.FieldExpressions["price_per_item"]
	assert.Equal(t, "avg_price", pricePerItemExpr.Field)
	assert.Equal(t, "total_amount / order_count", pricePerItemExpr.Expression)
	assert.Equal(t, []string{"total_amount", "order_count"}, pricePerItemExpr.Fields)

	// 验证投影配置
	assert.Equal(t, SourceGroupKey, config.Projections[0].SourceType)
	assert.Equal(t, SourceAggregateResult, config.Projections[1].SourceType)
	assert.Equal(t, SourceWindowProperty, config.Projections[2].SourceType)
}
