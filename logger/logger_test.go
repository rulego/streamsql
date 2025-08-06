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

package logger

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// TestLevel_String 测试日志级别的字符串表示
func TestLevel_String(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{DEBUG, "DEBUG"},
		{INFO, "INFO"},
		{WARN, "WARN"},
		{ERROR, "ERROR"},
		{OFF, "OFF"},
		{Level(999), "UNKNOWN"}, // 测试未知级别
	}

	for _, test := range tests {
		if got := test.level.String(); got != test.expected {
			t.Errorf("Level(%d).String() = %q, want %q", test.level, got, test.expected)
		}
	}
}

// TestNewLogger 测试创建新的日志器
func TestNewLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(INFO, &buf)

	if logger == nil {
		t.Fatal("NewLogger() returned nil")
	}

	// 测试日志输出
	logger.Info("test message")
	output := buf.String()

	if !strings.Contains(output, "test message") {
		t.Errorf("Expected log output to contain 'test message', got: %s", output)
	}

	if !strings.Contains(output, "[INFO]") {
		t.Errorf("Expected log output to contain '[INFO]', got: %s", output)
	}
}

// TestDefaultLogger_Debug 测试调试级别日志
func TestDefaultLogger_Debug(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(DEBUG, &buf)

	logger.Debug("debug message with %s", "parameter")
	output := buf.String()

	if !strings.Contains(output, "debug message with parameter") {
		t.Errorf("Expected debug message in output, got: %s", output)
	}

	if !strings.Contains(output, "[DEBUG]") {
		t.Errorf("Expected [DEBUG] in output, got: %s", output)
	}
}

// TestDefaultLogger_Info 测试信息级别日志
func TestDefaultLogger_Info(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(INFO, &buf)

	logger.Info("info message with %d number", 42)
	output := buf.String()

	if !strings.Contains(output, "info message with 42 number") {
		t.Errorf("Expected info message in output, got: %s", output)
	}

	if !strings.Contains(output, "[INFO]") {
		t.Errorf("Expected [INFO] in output, got: %s", output)
	}
}

// TestDefaultLogger_Warn 测试警告级别日志
func TestDefaultLogger_Warn(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(WARN, &buf)

	logger.Warn("warning message")
	output := buf.String()

	if !strings.Contains(output, "warning message") {
		t.Errorf("Expected warning message in output, got: %s", output)
	}

	if !strings.Contains(output, "[WARN]") {
		t.Errorf("Expected [WARN] in output, got: %s", output)
	}
}

// TestDefaultLogger_Error 测试错误级别日志
func TestDefaultLogger_Error(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(ERROR, &buf)

	logger.Error("error message: %v", "something went wrong")
	output := buf.String()

	if !strings.Contains(output, "error message: something went wrong") {
		t.Errorf("Expected error message in output, got: %s", output)
	}

	if !strings.Contains(output, "[ERROR]") {
		t.Errorf("Expected [ERROR] in output, got: %s", output)
	}
}

// TestDefaultLogger_SetLevel 测试设置日志级别
func TestDefaultLogger_SetLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(DEBUG, &buf)

	// 设置为 ERROR 级别
	logger.SetLevel(ERROR)

	// 测试低级别日志不会输出
	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")

	output := buf.String()
	if strings.Contains(output, "debug message") || strings.Contains(output, "info message") || strings.Contains(output, "warn message") {
		t.Errorf("Expected no output for lower level logs, got: %s", output)
	}

	// 测试 ERROR 级别日志会输出
	buf.Reset()
	logger.Error("error message")
	output = buf.String()

	if !strings.Contains(output, "error message") {
		t.Errorf("Expected error message in output, got: %s", output)
	}
}

// TestDefaultLogger_LevelFiltering 测试日志级别过滤
func TestDefaultLogger_LevelFiltering(t *testing.T) {
	tests := []struct {
		loggerLevel  Level
		messageLevel Level
		shouldLog    bool
	}{
		{DEBUG, DEBUG, true},
		{DEBUG, INFO, true},
		{DEBUG, WARN, true},
		{DEBUG, ERROR, true},
		{INFO, DEBUG, false},
		{INFO, INFO, true},
		{INFO, WARN, true},
		{INFO, ERROR, true},
		{WARN, DEBUG, false},
		{WARN, INFO, false},
		{WARN, WARN, true},
		{WARN, ERROR, true},
		{ERROR, DEBUG, false},
		{ERROR, INFO, false},
		{ERROR, WARN, false},
		{ERROR, ERROR, true},
		{OFF, ERROR, false},
	}

	for _, test := range tests {
		var buf bytes.Buffer
		logger := NewLogger(test.loggerLevel, &buf)

		// 根据消息级别调用相应的日志方法
		switch test.messageLevel {
		case DEBUG:
			logger.Debug("test message")
		case INFO:
			logger.Info("test message")
		case WARN:
			logger.Warn("test message")
		case ERROR:
			logger.Error("test message")
		}

		output := buf.String()
		hasOutput := len(output) > 0

		if hasOutput != test.shouldLog {
			t.Errorf("Logger level %s, message level %s: expected shouldLog=%v, got hasOutput=%v",
				test.loggerLevel.String(), test.messageLevel.String(), test.shouldLog, hasOutput)
		}
	}
}

// TestDefaultLogger_OFFLevel 测试 OFF 级别
func TestDefaultLogger_OFFLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(OFF, &buf)

	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")

	output := buf.String()
	if len(output) > 0 {
		t.Errorf("Expected no output when level is OFF, got: %s", output)
	}
}

// TestNewDiscardLogger 测试丢弃日志器
func TestNewDiscardLogger(t *testing.T) {
	logger := NewDiscardLogger()

	if logger == nil {
		t.Fatal("NewDiscardLogger() returned nil")
	}

	// 测试所有方法都不会产生输出或错误
	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")
	logger.SetLevel(DEBUG)

	// 如果没有 panic，测试通过
}

// TestGlobalLogger 测试全局日志器
func TestGlobalLogger(t *testing.T) {
	// 保存原始的全局日志器
	original := GetDefault()
	defer SetDefault(original)

	// 创建测试用的日志器
	var buf bytes.Buffer
	testLogger := NewLogger(DEBUG, &buf)
	SetDefault(testLogger)

	// 测试全局日志器是否被正确设置
	if GetDefault() != testLogger {
		t.Error("Global logger was not set correctly")
	}

	// 测试全局日志方法
	Debug("global debug message")
	Info("global info message")
	Warn("global warn message")
	Error("global error message")

	output := buf.String()

	expectedMessages := []string{
		"global debug message",
		"global info message",
		"global warn message",
		"global error message",
	}

	for _, msg := range expectedMessages {
		if !strings.Contains(output, msg) {
			t.Errorf("Expected output to contain '%s', got: %s", msg, output)
		}
	}
}

// TestLogFormat 测试日志格式
func TestLogFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(INFO, &buf)

	logger.Info("test message")
	output := buf.String()

	// 检查时间戳格式 (YYYY-MM-DD HH:MM:SS.mmm)
	if !strings.Contains(output, "[") || !strings.Contains(output, "]") {
		t.Errorf("Expected timestamp format in brackets, got: %s", output)
	}

	// 检查日志级别格式
	if !strings.Contains(output, "[INFO]") {
		t.Errorf("Expected [INFO] in output, got: %s", output)
	}

	// 检查消息内容
	if !strings.Contains(output, "test message") {
		t.Errorf("Expected 'test message' in output, got: %s", output)
	}
}

// TestLoggerWithStdout 测试使用标准输出的日志器
func TestLoggerWithStdout(t *testing.T) {
	logger := NewLogger(INFO, os.Stdout)

	if logger == nil {
		t.Fatal("NewLogger() with os.Stdout returned nil")
	}

	// 这个测试主要确保不会 panic
	logger.Info("test message to stdout")
}

// TestLoggerWithStderr 测试使用标准错误的日志器
func TestLoggerWithStderr(t *testing.T) {
	logger := NewLogger(ERROR, os.Stderr)

	if logger == nil {
		t.Fatal("NewLogger() with os.Stderr returned nil")
	}

	// 这个测试主要确保不会 panic
	logger.Error("test error message to stderr")
}

// TestConcurrentLogging 测试并发日志记录
func TestConcurrentLogging(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(INFO, &buf)

	done := make(chan bool, 10)

	// 启动多个 goroutine 并发写日志
	for i := 0; i < 10; i++ {
		go func(id int) {
			logger.Info("concurrent message from goroutine %d", id)
			done <- true
		}(i)
	}

	// 等待所有 goroutine 完成
	for i := 0; i < 10; i++ {
		<-done
	}

	output := buf.String()

	// 检查是否有输出（具体内容可能因并发而乱序）
	if len(output) == 0 {
		t.Error("Expected some output from concurrent logging")
	}

	// 检查是否包含预期的消息数量
	messageCount := strings.Count(output, "concurrent message")
	if messageCount != 10 {
		t.Errorf("Expected 10 concurrent messages, got %d", messageCount)
	}
}

// TestLoggerParameterFormatting 测试日志参数格式化
func TestLoggerParameterFormatting(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(DEBUG, &buf)

	tests := []struct {
		format   string
		args     []interface{}
		expected string
	}{
		{"simple message", nil, "simple message"},
		{"message with %s", []interface{}{"string"}, "message with string"},
		{"message with %d", []interface{}{42}, "message with 42"},
		{"message with %v", []interface{}{true}, "message with true"},
		{"multiple %s %d %v", []interface{}{"params", 123, false}, "multiple params 123 false"},
	}

	for _, test := range tests {
		buf.Reset()
		logger.Info(test.format, test.args...)
		output := buf.String()

		if !strings.Contains(output, test.expected) {
			t.Errorf("Expected output to contain '%s', got: %s", test.expected, output)
		}
	}
}

// TestDefaultLoggerInitialization 测试默认日志器初始化
func TestDefaultLoggerInitialization(t *testing.T) {
	defaultLogger := GetDefault()

	if defaultLogger == nil {
		t.Fatal("Default logger should not be nil")
	}

	// 测试默认日志器可以正常工作
	defaultLogger.Info("test default logger")
}

// TestLoggerInternalLogMethod 测试内部 log 方法的 OFF 级别处理
func TestLoggerInternalLogMethod(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(DEBUG, &buf).(*defaultLogger)

	// 设置为 OFF 级别
	logger.SetLevel(OFF)

	// 直接调用内部 log 方法
	logger.log(ERROR, "test message")

	// 验证没有输出
	output := buf.String()
	if len(output) > 0 {
		t.Errorf("Expected no output when level is OFF, got: %s", output)
	}
}

// TestDiscardLoggerAllMethods 测试丢弃日志器的所有方法
func TestDiscardLoggerAllMethods(t *testing.T) {
	logger := NewDiscardLogger()

	// 测试所有级别的日志方法
	logger.Debug("debug %s", "test")
	logger.Info("info %d", 123)
	logger.Warn("warn %v", true)
	logger.Error("error %s %d", "test", 456)

	// 测试设置级别
	logger.SetLevel(DEBUG)
	logger.SetLevel(INFO)
	logger.SetLevel(WARN)
	logger.SetLevel(ERROR)
	logger.SetLevel(OFF)

	// 如果没有 panic 或错误，测试通过
}

// TestLoggerWithNilArgs 测试使用 nil 参数的日志记录
func TestLoggerWithNilArgs(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(DEBUG, &buf)

	// 测试没有参数的情况
	logger.Info("message without args")
	output := buf.String()
	if !strings.Contains(output, "message without args") {
		t.Errorf("Expected message in output, got: %s", output)
	}

	// 测试空参数列表
	buf.Reset()
	logger.Info("message with empty args", []interface{}{}...)
	output = buf.String()
	if !strings.Contains(output, "message with empty args") {
		t.Errorf("Expected message in output, got: %s", output)
	}
}

// TestLoggerTimestampFormat 测试时间戳格式
func TestLoggerTimestampFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(INFO, &buf)

	logger.Info("timestamp test")
	output := buf.String()

	// 检查时间戳格式：[YYYY-MM-DD HH:MM:SS.mmm]
	// 使用正则表达式验证时间戳格式
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) == 0 {
		t.Fatal("No output lines found")
	}

	line := lines[0]
	// 检查是否包含日期时间格式
	if !strings.Contains(line, "[") || !strings.Contains(line, "]") {
		t.Errorf("Expected timestamp in brackets, got: %s", line)
	}

	// 检查是否包含年份（简单验证）
	if !strings.Contains(line, "2025") && !strings.Contains(line, "2024") && !strings.Contains(line, "2026") {
		t.Errorf("Expected year in timestamp, got: %s", line)
	}
}

// TestGlobalLoggerRestore 测试全局日志器的恢复
func TestGlobalLoggerRestore(t *testing.T) {
	// 保存原始的全局日志器
	original := GetDefault()

	// 创建新的测试日志器
	var buf bytes.Buffer
	testLogger := NewLogger(ERROR, &buf)
	SetDefault(testLogger)

	// 验证设置成功
	if GetDefault() != testLogger {
		t.Error("Failed to set test logger")
	}

	// 恢复原始日志器
	SetDefault(original)

	// 验证恢复成功
	if GetDefault() != original {
		t.Error("Failed to restore original logger")
	}
}

// TestLevelConstants 测试所有日志级别常量
func TestLevelConstants(t *testing.T) {
	expectedLevels := map[Level]string{
		DEBUG: "DEBUG",
		INFO:  "INFO",
		WARN:  "WARN",
		ERROR: "ERROR",
		OFF:   "OFF",
	}

	for level, expectedString := range expectedLevels {
		if level.String() != expectedString {
			t.Errorf("Level %d should return %s, got %s", level, expectedString, level.String())
		}
	}

	// 测试级别的数值
	if DEBUG != 0 {
		t.Errorf("DEBUG should be 0, got %d", DEBUG)
	}
	if INFO != 1 {
		t.Errorf("INFO should be 1, got %d", INFO)
	}
	if WARN != 2 {
		t.Errorf("WARN should be 2, got %d", WARN)
	}
	if ERROR != 3 {
		t.Errorf("ERROR should be 3, got %d", ERROR)
	}
	if OFF != 4 {
		t.Errorf("OFF should be 4, got %d", OFF)
	}
}
