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

// Package logger provides logging functionality for StreamSQL.
// Supports different log levels and configurable log output backends.
package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

// Level defines log levels
type Level int

const (
	// DEBUG debug level, displays detailed debug information
	DEBUG Level = iota
	// INFO info level, displays general information
	INFO
	// WARN warning level, displays warning information
	WARN
	// ERROR error level, only displays error information
	ERROR
	// OFF disables logging
	OFF
)

// String returns string representation of log level
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

// Logger interface defines basic methods for logging
type Logger interface {
	// Debug records debug level logs
	Debug(format string, args ...interface{})
	// Info records info level logs
	Info(format string, args ...interface{})
	// Warn records warning level logs
	Warn(format string, args ...interface{})
	// Error records error level logs
	Error(format string, args ...interface{})
	// SetLevel sets the log level
	SetLevel(level Level)
}

// defaultLogger is the default log implementation
type defaultLogger struct {
	level  Level
	logger *log.Logger
}

// NewLogger creates a new logger
// Parameters:
//   - level: log level
//   - output: output destination, such as os.Stdout, os.Stderr, or file
//
// Returns:
//   - Logger: logger instance
//
// Example:
//
//	logger := NewLogger(INFO, os.Stdout)
//	logger.Info("Application started")
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

// log internal logging method, formats and outputs log information
func (l *defaultLogger) log(level Level, format string, args ...interface{}) {
	if l.level == OFF {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	message := fmt.Sprintf(format, args...)
	logLine := fmt.Sprintf("[%s] [%s] %s", timestamp, level.String(), message)
	l.logger.Println(logLine)
}

// discardLogger is a logger that discards all log output
type discardLogger struct{}

// NewDiscardLogger creates a logger that discards all logs
// Used in scenarios where log output is not needed
func NewDiscardLogger() Logger {
	return &discardLogger{}
}

func (d *discardLogger) Debug(format string, args ...interface{}) {}
func (d *discardLogger) Info(format string, args ...interface{})  {}
func (d *discardLogger) Warn(format string, args ...interface{})  {}
func (d *discardLogger) Error(format string, args ...interface{}) {}
func (d *discardLogger) SetLevel(level Level)                     {}

// Global default logger
var defaultInstance Logger = NewLogger(INFO, os.Stdout)

// SetDefault sets the global default logger
func SetDefault(logger Logger) {
	defaultInstance = logger
}

// GetDefault gets the global default logger
func GetDefault() Logger {
	return defaultInstance
}

// 便捷的全局日志方法

// Debug uses the default logger to record debug information
func Debug(format string, args ...interface{}) {
	defaultInstance.Debug(format, args...)
}

// Info uses the default logger to record information
func Info(format string, args ...interface{}) {
	defaultInstance.Info(format, args...)
}

// Warn uses the default logger to record warnings
func Warn(format string, args ...interface{}) {
	defaultInstance.Warn(format, args...)
}

// Error uses the default logger to record errors
func Error(format string, args ...interface{}) {
	defaultInstance.Error(format, args...)
}
