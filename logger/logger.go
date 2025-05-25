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

// Package logger 提供StreamSQL的日志记录功能。
// 支持不同日志级别和可配置的日志输出后端。
package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

// Level 定义日志级别
type Level int

const (
	// DEBUG 调试级别，显示详细的调试信息
	DEBUG Level = iota
	// INFO 信息级别，显示一般信息
	INFO
	// WARN 警告级别，显示警告信息
	WARN
	// ERROR 错误级别，仅显示错误信息
	ERROR
	// OFF 关闭日志
	OFF
)

// String 返回日志级别的字符串表示
func (l Level) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case OFF:
		return "OFF"
	default:
		return "UNKNOWN"
	}
}

// Logger 接口定义了日志记录的基本方法
type Logger interface {
	// Debug 记录调试级别的日志
	Debug(format string, args ...interface{})
	// Info 记录信息级别的日志
	Info(format string, args ...interface{})
	// Warn 记录警告级别的日志
	Warn(format string, args ...interface{})
	// Error 记录错误级别的日志
	Error(format string, args ...interface{})
	// SetLevel 设置日志级别
	SetLevel(level Level)
}

// defaultLogger 是默认的日志实现
type defaultLogger struct {
	level  Level
	logger *log.Logger
}

// NewLogger 创建一个新的日志记录器
// 参数:
//   - level: 日志级别
//   - output: 输出目标，如os.Stdout、os.Stderr或文件
//
// 返回值:
//   - Logger: 日志记录器实例
//
// 示例:
//
//	logger := NewLogger(INFO, os.Stdout)
//	logger.Info("应用程序启动")
func NewLogger(level Level, output io.Writer) Logger {
	return &defaultLogger{
		level:  level,
		logger: log.New(output, "", 0), // 使用自定义格式，不使用标准库的前缀
	}
}

// Debug 记录调试级别的日志
func (l *defaultLogger) Debug(format string, args ...interface{}) {
	if l.level <= DEBUG {
		l.log(DEBUG, format, args...)
	}
}

// Info 记录信息级别的日志
func (l *defaultLogger) Info(format string, args ...interface{}) {
	if l.level <= INFO {
		l.log(INFO, format, args...)
	}
}

// Warn 记录警告级别的日志
func (l *defaultLogger) Warn(format string, args ...interface{}) {
	if l.level <= WARN {
		l.log(WARN, format, args...)
	}
}

// Error 记录错误级别的日志
func (l *defaultLogger) Error(format string, args ...interface{}) {
	if l.level <= ERROR {
		l.log(ERROR, format, args...)
	}
}

// SetLevel 设置日志级别
func (l *defaultLogger) SetLevel(level Level) {
	l.level = level
}

// log 内部日志记录方法，格式化输出日志信息
func (l *defaultLogger) log(level Level, format string, args ...interface{}) {
	if l.level == OFF {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	message := fmt.Sprintf(format, args...)
	logLine := fmt.Sprintf("[%s] [%s] %s", timestamp, level.String(), message)
	l.logger.Println(logLine)
}

// discardLogger 是一个丢弃所有日志输出的记录器
type discardLogger struct{}

// NewDiscardLogger 创建一个丢弃所有日志的记录器
// 用于在不需要日志输出的场景中使用
func NewDiscardLogger() Logger {
	return &discardLogger{}
}

func (d *discardLogger) Debug(format string, args ...interface{}) {}
func (d *discardLogger) Info(format string, args ...interface{})  {}
func (d *discardLogger) Warn(format string, args ...interface{})  {}
func (d *discardLogger) Error(format string, args ...interface{}) {}
func (d *discardLogger) SetLevel(level Level)                     {}

// 全局默认日志记录器
var defaultInstance Logger = NewLogger(INFO, os.Stdout)

// SetDefault 设置全局默认日志记录器
func SetDefault(logger Logger) {
	defaultInstance = logger
}

// GetDefault 获取全局默认日志记录器
func GetDefault() Logger {
	return defaultInstance
}

// 便捷的全局日志方法

// Debug 使用默认日志记录器记录调试信息
func Debug(format string, args ...interface{}) {
	defaultInstance.Debug(format, args...)
}

// Info 使用默认日志记录器记录信息
func Info(format string, args ...interface{}) {
	defaultInstance.Info(format, args...)
}

// Warn 使用默认日志记录器记录警告
func Warn(format string, args ...interface{}) {
	defaultInstance.Warn(format, args...)
}

// Error 使用默认日志记录器记录错误
func Error(format string, args ...interface{}) {
	defaultInstance.Error(format, args...)
}
