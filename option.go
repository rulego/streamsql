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
	"io"
	"time"

	"github.com/rulego/streamsql/logger"
)

// Option 表示对StreamSQL默认行为的修改配置。
// 通过函数式选项模式，用户可以灵活地配置StreamSQL的各种行为。
type Option func(*Streamsql)

// WithLogger 设置自定义日志记录器。
// 允许用户提供自己的日志实现，支持不同的日志后端和格式。
//
// 参数:
//   - log: 实现了logger.Logger接口的日志记录器
//
// 示例:
//
//	// 使用自定义日志记录器
//	customLogger := logger.NewLogger(logger.DEBUG, os.Stderr)
//	ssql := streamsql.New(WithLogger(customLogger))
func WithLogger(log logger.Logger) Option {
	return func(s *Streamsql) {
		logger.SetDefault(log)
	}
}

// WithLogLevel 设置日志级别。
// 这是设置日志级别的便捷方法，使用默认的日志输出目标。
//
// 参数:
//   - level: 日志级别，可选值：DEBUG, INFO, WARN, ERROR, OFF
//
// 示例:
//
//	// 设置为调试级别
//	ssql := streamsql.New(WithLogLevel(logger.DEBUG))
//
//	// 关闭日志
//	ssql := streamsql.New(WithLogLevel(logger.OFF))
func WithLogLevel(level logger.Level) Option {
	return func(s *Streamsql) {
		logger.GetDefault().SetLevel(level)
	}
}

// WithLogOutput 设置日志输出目标。
// 允许用户指定日志输出到文件、标准输出或其他io.Writer。
//
// 参数:
//   - output: 日志输出目标，如os.Stdout、os.Stderr或文件
//   - level: 日志级别
//
// 示例:
//
//	// 输出到文件
//	logFile, _ := os.OpenFile("streamsql.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
//	ssql := streamsql.New(WithLogOutput(logFile, logger.INFO))
//
//	// 输出到标准错误
//	ssql := streamsql.New(WithLogOutput(os.Stderr, logger.WARN))
func WithLogOutput(output io.Writer, level logger.Level) Option {
	return func(s *Streamsql) {
		customLogger := logger.NewLogger(level, output)
		logger.SetDefault(customLogger)
	}
}

// WithDiscardLog 禁用所有日志输出。
// 这是完全关闭日志的便捷方法，适用于性能敏感的生产环境。
//
// 示例:
//
//	// 完全禁用日志
//	ssql := streamsql.New(WithDiscardLog())
func WithDiscardLog() Option {
	return func(s *Streamsql) {
		logger.SetDefault(logger.NewDiscardLogger())
	}
}

//// WithLocation overrides the timezone of the cron instance.
//func WithLocation(loc *time.Location) Option {
//	return func(s *Streamsql) {
//	}
//}

// WithBufferSizes 设置自定义缓冲区大小。
// 允许用户精确控制各个缓冲区的大小以适应不同的负载需求。
// 注意：默认配置已经是标准场景配置（10K缓冲区），使用expand策略动态扩容。
//
// 参数:
//   - dataBufSize: 数据通道缓冲区大小，影响输入数据的缓存能力
//   - resultBufSize: 结果通道缓冲区大小，影响输出结果的缓存能力
//   - sinkPoolSize: Sink工作池大小，影响异步sink处理的并发度
//
// 示例:
//
//	// 轻量场景配置（资源受限环境）
//	ssql := streamsql.New(WithBufferSizes(5000, 5000, 250))
//
//	// 重负载配置（极高并发场景）
//	ssql := streamsql.New(WithBufferSizes(50000, 50000, 1500))
func WithBufferSizes(dataBufSize, resultBufSize, sinkPoolSize int) Option {
	return func(s *Streamsql) {
		s.dataBufSize = dataBufSize
		s.resultBufSize = resultBufSize
		s.sinkPoolSize = sinkPoolSize
	}
}

// WithHighPerformance 启用高性能配置。
// 使用预设的超大缓冲区配置，适用于极高吞吐量场景。
// 配置：50K数据缓冲，50K结果缓冲，1K sink池。
//
// 示例:
//
//	// 启用高性能模式
//	ssql := streamsql.New(WithHighPerformance())
func WithHighPerformance() Option {
	return func(s *Streamsql) {
		s.highPerf = true
	}
}

// WithDataBufferSize 仅设置数据通道缓冲区大小。
// 这是只调整输入缓冲的便捷方法。默认为10K。
//
// 参数:
//   - size: 数据通道缓冲区大小
//
// 示例:
//
//	// 减小到轻量级配置
//	ssql := streamsql.New(WithDataBufferSize(5000))
//
//	// 增大到超高性能配置
//	ssql := streamsql.New(WithDataBufferSize(50000))
func WithDataBufferSize(size int) Option {
	return func(s *Streamsql) {
		s.dataBufSize = size
	}
}

// WithResultBufferSize 仅设置结果通道缓冲区大小。
// 这是只调整输出缓冲的便捷方法。默认为10K。
//
// 参数:
//   - size: 结果通道缓冲区大小
//
// 示例:
//
//	// 减小到轻量级配置
//	ssql := streamsql.New(WithResultBufferSize(5000))
//
//	// 增大到超高性能配置
//	ssql := streamsql.New(WithResultBufferSize(50000))
func WithResultBufferSize(size int) Option {
	return func(s *Streamsql) {
		s.resultBufSize = size
	}
}

// WithSinkPoolSize 仅设置Sink工作池大小。
// 这是只调整sink并发度的便捷方法。默认为400。
//
// 参数:
//   - size: Sink工作池大小
//
// 示例:
//
//	// 减小到轻量级配置
//	ssql := streamsql.New(WithSinkPoolSize(250))
//
//	// 增大到超高性能配置
//	ssql := streamsql.New(WithSinkPoolSize(1500))
func WithSinkPoolSize(size int) Option {
	return func(s *Streamsql) {
		s.sinkPoolSize = size
	}
}

// ============ 数据丢失策略配置 ============

// WithOverflowStrategy 设置数据溢出处理策略。
// 当缓冲区满时采用的处理方式，影响数据完整性和系统性能。
// 默认策略为"expand"（动态扩容），确保零数据丢失。
//
// 策略选项:
//   - "expand": 动态扩容缓冲区（默认，推荐）- 零数据丢失，自动调节性能
//   - "drop": 丢弃数据 - 最高性能，但可能丢失数据
//   - "block": 阻塞等待 - 零数据丢失，但可能影响吞吐量
//   - "persist": 持久化到磁盘 - 零数据丢失，支持数据恢复
//
// 参数:
//   - strategy: 溢出处理策略
//   - timeout: 阻塞模式的超时时间（仅在"block"策略下有效）
//
// 示例:
//
//	// 使用丢弃策略（最高性能）
//	ssql := streamsql.New(WithOverflowStrategy("drop", 0))
//
//	// 使用阻塞策略（零丢失，有超时）
//	ssql := streamsql.New(WithOverflowStrategy("block", 5*time.Second))
//
//	// 使用持久化策略（零丢失，可恢复）
//	ssql := streamsql.New(WithOverflowStrategy("persist", 0))
//
//	// 使用动态扩容策略（默认，推荐）
//	ssql := streamsql.New(WithOverflowStrategy("expand", 0))
func WithOverflowStrategy(strategy string, timeout time.Duration) Option {
	return func(s *Streamsql) {
		s.overflowStrategy = strategy
		s.blockingTimeout = timeout
	}
}

// WithDropStrategy 设置为丢弃策略的便捷方法。
// 当缓冲区满时直接丢弃新数据，保证最高性能但可能丢失数据。
// 适用于对性能要求极高、可容忍少量数据丢失的场景。
//
// 示例:
//
//	// 启用丢弃策略
//	ssql := streamsql.New(WithDropStrategy())
func WithDropStrategy() Option {
	return func(s *Streamsql) {
		s.overflowStrategy = "drop"
		s.blockingTimeout = 0
	}
}

// WithBlockStrategy 设置为阻塞策略的便捷方法。
// 当缓冲区满时阻塞等待，直到有空间或超时。
// 保证零数据丢失，但可能影响系统吞吐量。
//
// 参数:
//   - timeout: 阻塞超时时间，0表示无限等待
//
// 示例:
//
//	// 启用阻塞策略，5秒超时
//	ssql := streamsql.New(WithBlockStrategy(5*time.Second))
//
//	// 启用阻塞策略，无限等待
//	ssql := streamsql.New(WithBlockStrategy(0))
func WithBlockStrategy(timeout time.Duration) Option {
	return func(s *Streamsql) {
		s.overflowStrategy = "block"
		s.blockingTimeout = timeout
	}
}

// WithExpandStrategy 设置为动态扩容策略的便捷方法。
// 当缓冲区满时自动扩大缓冲区容量，实现零数据丢失和性能平衡。
// 这是默认策略，推荐在大多数生产环境中使用。
//
// 示例:
//
//	// 启用动态扩容策略（默认已启用）
//	ssql := streamsql.New(WithExpandStrategy())
func WithExpandStrategy() Option {
	return func(s *Streamsql) {
		s.overflowStrategy = "expand"
		s.blockingTimeout = 0
	}
}

// WithPersistStrategy 设置为持久化策略的便捷方法。
// 当缓冲区满时将数据持久化到磁盘，支持数据恢复。
// 适用于对数据完整性要求极高的关键业务场景。
//
// 参数:
//   - dataDir: 持久化数据目录，为空则使用默认目录"./streamsql_overflow_data"
//
// 示例:
//
//	// 启用持久化策略，使用默认目录
//	ssql := streamsql.New(WithPersistStrategy(""))
//
//	// 启用持久化策略，使用自定义目录
//	ssql := streamsql.New(WithPersistStrategy("/data/streamsql"))
func WithPersistStrategy(dataDir string) Option {
	return func(s *Streamsql) {
		s.overflowStrategy = "persist"
		s.blockingTimeout = 0
		if dataDir != "" {
			s.persistDataDir = dataDir
		}
	}
}

// ============ 持久化配置 ============

// WithPersistenceConfig 设置持久化配置参数。
// 用于配置数据持久化的详细行为。
//
// 参数:
//   - dataDir: 数据存储目录
//   - maxFileSize: 单个文件最大大小（字节）
//   - flushInterval: 数据刷新到磁盘的间隔时间
//
// 示例:
//
//	// 配置持久化参数
//	ssql := streamsql.New(
//	    WithPersistStrategy(""),
//	    WithPersistenceConfig("/data/backup", 50*1024*1024, 10*time.Second),
//	)
func WithPersistenceConfig(dataDir string, maxFileSize int64, flushInterval time.Duration) Option {
	return func(s *Streamsql) {
		s.persistDataDir = dataDir
		s.persistMaxFileSize = maxFileSize
		s.persistFlushInterval = flushInterval
	}
}

// WithPersistDataDir 设置持久化数据目录。
//
// 参数:
//   - dataDir: 数据存储目录路径
//
// 示例:
//
//	ssql := streamsql.New(WithPersistDataDir("/var/lib/streamsql"))
func WithPersistDataDir(dataDir string) Option {
	return func(s *Streamsql) {
		s.persistDataDir = dataDir
	}
}

// WithPersistMaxFileSize 设置持久化文件最大大小。
//
// 参数:
//   - maxSize: 单个文件最大大小（字节）
//
// 示例:
//
//	// 设置最大文件大小为100MB
//	ssql := streamsql.New(WithPersistMaxFileSize(100 * 1024 * 1024))
func WithPersistMaxFileSize(maxSize int64) Option {
	return func(s *Streamsql) {
		s.persistMaxFileSize = maxSize
	}
}

// WithPersistFlushInterval 设置持久化刷新间隔。
//
// 参数:
//   - interval: 数据刷新到磁盘的间隔时间
//
// 示例:
//
//	// 每30秒刷新一次
//	ssql := streamsql.New(WithPersistFlushInterval(30 * time.Second))
func WithPersistFlushInterval(interval time.Duration) Option {
	return func(s *Streamsql) {
		s.persistFlushInterval = interval
	}
}

// ============ 预设配置组合 ============

// WithLightweightConfig 轻量级配置组合。
// 适用于资源受限环境、开发测试、或低负载场景。
// 配置：5K数据缓冲，5K结果缓冲，250 sink池，expand策略。
//
// 特点:
//   - 内存占用低（约1-2MB）
//   - 动态扩容保证数据完整性
//   - 适合日常开发和轻量级生产环境
//
// 示例:
//
//	ssql := streamsql.New(WithLightweightConfig())
func WithLightweightConfig() Option {
	return func(s *Streamsql) {
		s.dataBufSize = 5000
		s.resultBufSize = 5000
		s.sinkPoolSize = 250
		s.overflowStrategy = "expand"
		s.blockingTimeout = 0
	}
}

// WithProductionConfig 生产环境配置组合。
// 适用于中等到高负载的生产环境。
// 配置：20K数据缓冲，20K结果缓冲，800 sink池，expand策略。
//
// 特点:
//   - 性能与资源平衡
//   - 零数据丢失保证
//   - 适合大多数生产环境
//
// 示例:
//
//	ssql := streamsql.New(WithProductionConfig())
func WithProductionConfig() Option {
	return func(s *Streamsql) {
		s.dataBufSize = 20000
		s.resultBufSize = 20000
		s.sinkPoolSize = 800
		s.overflowStrategy = "expand"
		s.blockingTimeout = 0
	}
}

// WithExtremePerformanceConfig 极限性能配置组合。
// 适用于超高并发、超大吞吐量的极端场景。
// 配置：100K数据缓冲，100K结果缓冲，2K sink池，expand策略。
//
// 特点:
//   - 最高性能表现
//   - 内存占用较大（约25MB）
//   - 适合关键业务和极高负载场景
//
// 示例:
//
//	ssql := streamsql.New(WithExtremePerformanceConfig())
func WithExtremePerformanceConfig() Option {
	return func(s *Streamsql) {
		s.dataBufSize = 100000
		s.resultBufSize = 100000
		s.sinkPoolSize = 2000
		s.overflowStrategy = "expand"
		s.blockingTimeout = 0
	}
}

// WithMissionCriticalConfig 关键任务配置组合。
// 适用于对数据完整性要求极高的关键业务场景。
// 配置：20K数据缓冲，20K结果缓冲，800 sink池，持久化策略。
//
// 特点:
//   - 零数据丢失保证
//   - 支持数据恢复
//   - 适合金融、医疗等关键业务
//
// 示例:
//
//	ssql := streamsql.New(WithMissionCriticalConfig("/backup/streamsql"))
func WithMissionCriticalConfig(dataDir string) Option {
	return func(s *Streamsql) {
		s.dataBufSize = 20000
		s.resultBufSize = 20000
		s.sinkPoolSize = 800
		s.overflowStrategy = "persist"
		s.blockingTimeout = 0
		if dataDir != "" {
			s.persistDataDir = dataDir
		}
	}
}
