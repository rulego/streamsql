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
