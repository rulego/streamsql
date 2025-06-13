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
	"time"

	"github.com/rulego/streamsql/rsql"
	"github.com/rulego/streamsql/stream"
)

// Streamsql 是StreamSQL流处理引擎的主要接口。
// 它封装了SQL解析、流处理、窗口管理等核心功能。
//
// 使用示例:
//
//	ssql := streamsql.New()
//	err := ssql.Execute("SELECT AVG(temperature) FROM stream GROUP BY TumblingWindow('5s')")
//	ssql.AddData(map[string]interface{}{"temperature": 25.5})
type Streamsql struct {
	stream *stream.Stream
	// 缓冲区配置
	dataBufSize   int  // 数据通道缓冲区大小
	resultBufSize int  // 结果通道缓冲区大小
	sinkPoolSize  int  // Sink工作池大小
	highPerf      bool // 是否使用高性能配置

	// 数据丢失策略配置
	overflowStrategy string        // 溢出策略: "expand"(默认), "drop", "block", "persist"
	blockingTimeout  time.Duration // 阻塞超时时间

	// 持久化配置
	persistDataDir       string        // 持久化数据目录
	persistMaxFileSize   int64         // 持久化文件最大大小
	persistFlushInterval time.Duration // 持久化刷新间隔
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
//	// 创建带日志配置的实例
//	ssql := streamsql.New(
//	    streamsql.WithLogLevel(logger.DEBUG),
//	    streamsql.WithDiscardLog(),
//	)
func New(options ...Option) *Streamsql {
	s := &Streamsql{
		// 设置默认缓冲区配置（优化的标准场景配置）
		dataBufSize:   20000, // 默认2万数据缓冲，经测试验证的性能最优点
		resultBufSize: 20000, // 默认2万结果缓冲，平衡性能和内存使用
		sinkPoolSize:  800,   // 默认800个sink工作池，与缓冲区比例优化
		highPerf:      false, // 默认不启用超高性能模式

		// 设置默认策略配置（零数据丢失的expand策略）
		overflowStrategy: "expand", // 默认动态扩容策略，保证零数据丢失
		blockingTimeout:  0,        // 默认无超时限制

		// 设置默认持久化配置
		persistDataDir:       "./streamsql_overflow_data", // 默认持久化目录
		persistMaxFileSize:   10 * 1024 * 1024,            // 默认10MB文件大小
		persistFlushInterval: 5 * time.Second,             // 默认5秒刷新间隔
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

	// 根据配置创建流处理器
	if s.highPerf {
		// 使用高性能配置
		s.stream, err = stream.NewHighPerformanceStream(*config)
	} else {
		// 使用配置的策略创建流处理器，传递持久化配置
		s.stream, err = stream.NewStreamWithLossPolicyAndPersistence(*config,
			s.dataBufSize, s.resultBufSize, s.sinkPoolSize,
			s.overflowStrategy, s.blockingTimeout,
			s.persistDataDir, s.persistMaxFileSize, s.persistFlushInterval)
	}

	if err != nil {
		return fmt.Errorf("创建流处理器失败: %w", err)
	}

	// 注册过滤条件
	if err = s.stream.RegisterFilter(condition); err != nil {
		return fmt.Errorf("注册过滤条件失败: %w", err)
	}

	// 启动流处理
	s.stream.Start()
	return nil
}

// AddData 向流中添加一条数据记录。
// 数据会根据已配置的SQL查询进行处理和聚合。
//
// 支持的数据格式:
//   - map[string]interface{}: 最常用的键值对格式
//   - 结构体: 会自动转换为map格式处理
//
// 参数:
//   - data: 要添加的数据，通常是map[string]interface{}或结构体
//
// 示例:
//
//	// 添加设备数据
//	ssql.AddData(map[string]interface{}{
//	    "deviceId": "sensor001",
//	    "temperature": 25.5,
//	    "humidity": 60.0,
//	    "timestamp": time.Now(),
//	})
//
//	// 添加用户行为数据
//	ssql.AddData(map[string]interface{}{
//	    "userId": "user123",
//	    "action": "click",
//	    "page": "/home",
//	})
func (s *Streamsql) AddData(data interface{}) {
	if s.stream != nil {
		s.stream.AddData(data)
	}
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
//	ssql.Stream().AddSink(func(result interface{}) {
//	    fmt.Printf("处理结果: %v\n", result)
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
