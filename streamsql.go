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
	"fmt"

	"github.com/rulego/streamsql/rsql"
	"github.com/rulego/streamsql/stream"
	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/utils/table"
)

// Streamsql 是StreamSQL流处理引擎的主要接口。
// 它封装了SQL解析、流处理、窗口管理等核心功能。
//
// 使用示例:
//
//	ssql := streamsql.New()
//	err := ssql.Execute("SELECT AVG(temperature) FROM stream GROUP BY TumblingWindow('5s')")
//	ssql.Emit(map[string]interface{}{"temperature": 25.5})
type Streamsql struct {
	stream *stream.Stream

	// 性能配置模式
	performanceMode string // "default", "high_performance", "low_latency", "zero_data_loss", "custom"
	customConfig    *types.PerformanceConfig

	// 新增：同步处理模式配置
	enableSyncMode bool // 是否启用同步模式（用于非聚合查询）

	// 保存原始SELECT字段顺序，用于表格输出时保持字段顺序
	fieldOrder []string
}

// New 创建一个新的StreamSQL实例。
// 支持通过可选的Option参数进行配置。
//
// 参数:
//   - options: 可变长度的配置选项，用于自定义StreamSQL行为
//
// 返回值:
//   - *Streamsql: 新创建的StreamSQL实例
//
// 示例:
//
//	// 创建默认实例
//	ssql := streamsql.New()
//
//	// 创建高性能实例
//	ssql := streamsql.New(streamsql.WithHighPerformance())
//
//	// 创建零数据丢失实例
//	ssql := streamsql.New(streamsql.WithZeroDataLoss())
func New(options ...Option) *Streamsql {
	s := &Streamsql{
		performanceMode: "default", // 默认使用标准性能配置
	}

	// 应用所有配置选项
	for _, option := range options {
		option(s)
	}

	return s
}

// Execute 解析并执行SQL查询，创建对应的流处理管道。
// 这是StreamSQL的核心方法，负责将SQL转换为实际的流处理逻辑。
//
// 支持的SQL语法:
//   - SELECT 子句: 选择字段和聚合函数
//   - FROM 子句: 指定数据源（通常为'stream'）
//   - WHERE 子句: 数据过滤条件
//   - GROUP BY 子句: 分组字段和窗口函数
//   - HAVING 子句: 聚合结果过滤
//   - LIMIT 子句: 限制结果数量
//   - DISTINCT: 结果去重
//
// 窗口函数:
//   - TumblingWindow('5s'): 滚动窗口
//   - SlidingWindow('30s', '10s'): 滑动窗口
//   - CountingWindow(100): 计数窗口
//   - SessionWindow('5m'): 会话窗口
//
// 参数:
//   - sql: 要执行的SQL查询语句
//
// 返回值:
//   - error: 如果SQL解析或执行失败，返回相应错误
//
// 示例:
//
//	// 基本聚合查询
//	err := ssql.Execute("SELECT deviceId, AVG(temperature) FROM stream GROUP BY deviceId, TumblingWindow('5s')")
//
//	// 带过滤条件的查询
//	err := ssql.Execute("SELECT * FROM stream WHERE temperature > 30")
//
//	// 复杂的窗口聚合
//	err := ssql.Execute(`
//	    SELECT deviceId,
//	           AVG(temperature) as avg_temp,
//	           MAX(humidity) as max_humidity
//	    FROM stream
//	    WHERE deviceId != 'test'
//	    GROUP BY deviceId, SlidingWindow('1m', '30s')
//	    HAVING avg_temp > 25
//	    LIMIT 100
//	`)
func (s *Streamsql) Execute(sql string) error {
	// 解析SQL语句
	config, condition, err := rsql.Parse(sql)
	if err != nil {
		return fmt.Errorf("SQL解析失败: %w", err)
	}

	// 从解析结果中获取字段顺序信息
	s.fieldOrder = config.FieldOrder

	// 根据性能模式创建流处理器
	var streamInstance *stream.Stream

	switch s.performanceMode {
	case "high_performance":
		streamInstance, err = stream.NewStreamWithHighPerformance(*config)
	case "low_latency":
		streamInstance, err = stream.NewStreamWithLowLatency(*config)
	case "zero_data_loss":
		streamInstance, err = stream.NewStreamWithZeroDataLoss(*config)
	case "custom":
		if s.customConfig != nil {
			streamInstance, err = stream.NewStreamWithCustomPerformance(*config, *s.customConfig)
		} else {
			streamInstance, err = stream.NewStream(*config)
		}
	default: // "default"
		streamInstance, err = stream.NewStream(*config)
	}

	if err != nil {
		return fmt.Errorf("创建流处理器失败: %w", err)
	}

	s.stream = streamInstance

	// 注册过滤条件
	if err = s.stream.RegisterFilter(condition); err != nil {
		return fmt.Errorf("注册过滤条件失败: %w", err)
	}

	// 启动流处理
	s.stream.Start()
	return nil
}

// Emit 添加数据到流处理管道。
// 接受类型安全的map[string]interface{}格式数据。
//
// 参数:
//   - data: 要添加的数据，必须是map[string]interface{}类型
//
// 示例:
//
//	// 添加设备数据
//	ssql.Emit(map[string]interface{}{
//	    "deviceId": "sensor001",
//	    "temperature": 25.5,
//	    "humidity": 60.0,
//	    "timestamp": time.Now(),
//	})
//
//	// 添加用户行为数据
//	ssql.Emit(map[string]interface{}{
//	    "userId": "user123",
//	    "action": "click",
//	    "page": "/home",
//	})
func (s *Streamsql) Emit(data map[string]interface{}) {
	if s.stream != nil {
		s.stream.Emit(data)
	}
}

// EmitSync 同步处理数据，立即返回处理结果。
// 仅适用于非聚合查询，聚合查询会返回错误。
// 接受类型安全的map[string]interface{}格式数据。
//
// 参数:
//   - data: 要处理的数据，必须是map[string]interface{}类型
//
// 返回值:
//   - map[string]interface{}: 处理后的结果数据，如果不匹配过滤条件返回nil
//   - error: 处理错误
//
// 示例:
//
//	result, err := ssql.EmitSync(map[string]interface{}{
//	    "deviceId": "sensor001",
//	    "temperature": 25.5,
//	})
//	if err != nil {
//	    log.Printf("处理错误: %v", err)
//	} else if result != nil {
//	    // 立即使用处理结果（result是map[string]interface{}类型）
//	    fmt.Printf("处理结果: %v\n", result)
//	}
func (s *Streamsql) EmitSync(data map[string]interface{}) (map[string]interface{}, error) {
	if s.stream == nil {
		return nil, fmt.Errorf("stream未初始化")
	}

	// 检查是否为非聚合查询
	if s.stream.IsAggregationQuery() {
		return nil, fmt.Errorf("同步模式仅支持非聚合查询，聚合查询请使用Emit()方法")
	}

	return s.stream.ProcessSync(data)
}

// IsAggregationQuery 检查当前查询是否为聚合查询
func (s *Streamsql) IsAggregationQuery() bool {
	if s.stream == nil {
		return false
	}
	return s.stream.IsAggregationQuery()
}

// Stream 返回底层的流处理器实例。
// 通过此方法可以访问更底层的流处理功能。
//
// 返回值:
//   - *stream.Stream: 底层流处理器实例，如果未执行SQL则返回nil
//
// 常用场景:
//   - 添加结果处理回调
//   - 获取结果通道
//   - 手动控制流处理生命周期
//
// 示例:
//
//	// 添加结果处理回调
//	ssql.Stream().AddSink(func(results []map[string]interface{}) {
//	    fmt.Printf("处理结果: %v\n", results)
//	})
//
//	// 获取结果通道
//	resultChan := ssql.Stream().GetResultsChan()
//	go func() {
//	    for result := range resultChan {
//	        // 处理结果
//	    }
//	}()
func (s *Streamsql) Stream() *stream.Stream {
	return s.stream
}

// GetStats 获取流处理统计信息
func (s *Streamsql) GetStats() map[string]int64 {
	if s.stream != nil {
		return s.stream.GetStats()
	}
	return make(map[string]int64)
}

// GetDetailedStats 获取详细的性能统计信息
func (s *Streamsql) GetDetailedStats() map[string]interface{} {
	if s.stream != nil {
		return s.stream.GetDetailedStats()
	}
	return make(map[string]interface{})
}

// Stop 停止流处理器，释放相关资源。
// 调用此方法后，流处理器将停止接收和处理新数据。
//
// 建议在应用程序退出前调用此方法进行清理:
//
//	defer ssql.Stop()
//
// 注意: 停止后的StreamSQL实例不能重新启动，需要创建新实例。
func (s *Streamsql) Stop() {
	if s.stream != nil {
		s.stream.Stop()
	}
}

// AddSink 直接添加结果处理回调函数。
// 这是对 Stream().AddSink() 的便捷封装，使API调用更简洁。
//
// 参数:
//   - sink: 结果处理函数，接收[]map[string]interface{}类型的结果数据
//
// 示例:
//
//	// 直接添加结果处理
//	ssql.AddSink(func(results []map[string]interface{}) {
//	    fmt.Printf("处理结果: %v\n", results)
//	})
//
//	// 添加多个处理器
//	ssql.AddSink(func(results []map[string]interface{}) {
//	    // 保存到数据库
//	    saveToDatabase(results)
//	})
//	ssql.AddSink(func(results []map[string]interface{}) {
//	    // 发送到消息队列
//	    sendToQueue(results)
//	})
func (s *Streamsql) AddSink(sink func([]map[string]interface{})) {
	if s.stream != nil {
		s.stream.AddSink(sink)
	}
}

// PrintTable 以表格形式打印结果到控制台，类似数据库输出格式。
// 首先显示列名，然后逐行显示数据。
//
// 支持的数据格式:
//   - []map[string]interface{}: 多行记录
//   - map[string]interface{}: 单行记录
//   - 其他类型: 直接打印
//
// 示例:
//
//	// 表格式打印结果
//	ssql.PrintTable()
//
//	// 输出格式:
//	// +--------+----------+
//	// | device | max_temp |
//	// +--------+----------+
//	// | aa     | 30.0     |
//	// | bb     | 22.0     |
//	// +--------+----------+
func (s *Streamsql) PrintTable() {
	s.AddSink(func(results []map[string]interface{}) {
		s.printTableFormat(results)
	})
}

// printTableFormat 格式化打印表格数据
// 参数:
//   - results: []map[string]interface{}类型的结果数据
func (s *Streamsql) printTableFormat(results []map[string]interface{}) {
	table.FormatTableData(results, s.fieldOrder)
}

// ToChannel 返回结果通道，用于异步获取处理结果。
// 通过此通道可以以非阻塞方式获取流处理结果。
//
// 返回值:
//   - <-chan interface{}: 只读的结果通道，如果未执行SQL则返回nil
//
// 示例:
//
//	// 获取结果通道
//	resultChan := ssql.ToChannel()
//	if resultChan != nil {
//	    go func() {
//	        for result := range resultChan {
//	            fmt.Printf("异步结果: %v\n", result)
//	        }
//	    }()
//	}

// ToChannel 将查询结果转换为通道输出
// 返回一个只读通道，用于接收查询结果
//
// 注意:
//   - 必须有消费者持续从通道读取数据，否则可能导致流处理阻塞
//   - 返回的通道传输批量结果数据
func (s *Streamsql) ToChannel() <-chan []map[string]interface{} {
	if s.stream != nil {
		return s.stream.GetResultsChan()
	}
	return nil
}
