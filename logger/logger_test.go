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

// TestLevel_String String representation at the test log level
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
		{Level(999), "UNKNOWN"}, // Testing unknown levels
	}

	for _, test := range tests {
		if got := test.level.String(); got != test.expected {
			t.Errorf("Level(%d).String() = %q, want %q", test.level, got, test.expected)
		}
	}
}

// TestNewLogger tests the creation of a new logger
func TestNewLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(INFO, &buf)

	if logger == nil {
		t.Fatal("NewLogger() returned nil")
	}

	// Test log output
	logger.Info("test message")
	output := buf.String()

	if !strings.Contains(output, "test message") {
		t.Errorf("Expected log output to contain 'test message', got: %s", output)
	}

	if !strings.Contains(output, "[INFO]") {
		t.Errorf("Expected log output to contain '[INFO]', got: %s", output)
	}
}

// TestDefaultLogger_Debug Test debug level logs
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

// TestDefaultLogger_Info Test information level log
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

// TestDefaultLogger_Warn Test warning level logs
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

// TestDefaultLogger_Error Test error level logs
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

// TestDefaultLogger_SetLevel Test the log level
func TestDefaultLogger_SetLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(DEBUG, &buf)

	// Set to ERROR level
	logger.SetLevel(ERROR)

	// Low-level test logs do not output
	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")

	output := buf.String()
	if strings.Contains(output, "debug message") || strings.Contains(output, "info message") || strings.Contains(output, "warn message") {
		t.Errorf("Expected no output for lower level logs, got: %s", output)
	}

	// Test ERROR level logs will be output
	buf.Reset()
	logger.Error("error message")
	output = buf.String()

	if !strings.Contains(output, "error message") {
		t.Errorf("Expected error message in output, got: %s", output)
	}
}

// TestDefaultLogger_LevelFiltering Test log-level filtering
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

		// Call the corresponding logging method according to the message level
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

// TestDefaultLogger_OFFLevel Test OFF level
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

// TestNewDiscardLogger tests the drop logger
func TestNewDiscardLogger(t *testing.T) {
	logger := NewDiscardLogger()

	if logger == nil {
		t.Fatal("NewDiscardLogger() returned nil")
	}

	// Testing all methods does not produce output or errors
	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")
	logger.SetLevel(DEBUG)

	// If there is no panic, the test passes
}

// TestGlobalLogger tests the global logger
func TestGlobalLogger(t *testing.T) {
	// Preserves the original global logger
	original := GetDefault()
	defer SetDefault(original)

	// Create a test logger
	var buf bytes.Buffer
	testLogger := NewLogger(DEBUG, &buf)
	SetDefault(testLogger)

	// Test whether the global logger is set correctly
	if GetDefault() != testLogger {
		t.Error("Global logger was not set correctly")
	}

	// Test the global logging method
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

// TestLogFormat: Test log format
func TestLogFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(INFO, &buf)

	logger.Info("test message")
	output := buf.String()

	// Check the timestamp format (YYYY-MM-DD HH:MM:SS.mmm)
	if !strings.Contains(output, "[") || !strings.Contains(output, "]") {
		t.Errorf("Expected timestamp format in brackets, got: %s", output)
	}

	// Check the log-level format
	if !strings.Contains(output, "[INFO]") {
		t.Errorf("Expected [INFO] in output, got: %s", output)
	}

	// Check the message content
	if !strings.Contains(output, "test message") {
		t.Errorf("Expected 'test message' in output, got: %s", output)
	}
}

// TestLoggerWithStdout tests loggers using standard output
func TestLoggerWithStdout(t *testing.T) {
	logger := NewLogger(INFO, os.Stdout)

	if logger == nil {
		t.Fatal("NewLogger() with os.Stdout returned nil")
	}

	// This test mainly ensures there is no panic
	logger.Info("test message to stdout")
}

// TestLoggerWithStderr tests using a standard error logger
func TestLoggerWithStderr(t *testing.T) {
	logger := NewLogger(ERROR, os.Stderr)

	if logger == nil {
		t.Fatal("NewLogger() with os.Stderr returned nil")
	}

	// This test mainly ensures there is no panic
	logger.Error("test error message to stderr")
}

// TestConcurrentLogging tests concurrent logging
func TestConcurrentLogging(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(INFO, &buf)

	done := make(chan bool, 10)

	// Start multiple goroutines and write logs
	for i := 0; i < 10; i++ {
		go func(id int) {
			logger.Info("concurrent message from goroutine %d", id)
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	output := buf.String()

	// Check for outputs (specific content may be disordered due to concurrency)
	if len(output) == 0 {
		t.Error("Expected some output from concurrent logging")
	}

	// Check whether the expected number of messages is included
	messageCount := strings.Count(output, "concurrent message")
	if messageCount != 10 {
		t.Errorf("Expected 10 concurrent messages, got %d", messageCount)
	}
}

// TestLoggerParameterFormatting: Formatting test log parameters
func TestLoggerParameterFormatting(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(DEBUG, &buf)

	tests := []struct {
		format   string
		args     []any
		expected string
	}{
		{"simple message", nil, "simple message"},
		{"message with %s", []any{"string"}, "message with string"},
		{"message with %d", []any{42}, "message with 42"},
		{"message with %v", []any{true}, "message with true"},
		{"multiple %s %d %v", []any{"params", 123, false}, "multiple params 123 false"},
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

// TestDefaultLoggerInitialization Tests default logger initialization
func TestDefaultLoggerInitialization(t *testing.T) {
	defaultLogger := GetDefault()

	if defaultLogger == nil {
		t.Fatal("Default logger should not be nil")
	}

	// Test that the default logger works properly
	defaultLogger.Info("test default logger")
}

// TestLoggerInternalLogMethod tests the OFF level handling of internal log methods
func TestLoggerInternalLogMethod(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(DEBUG, &buf).(*defaultLogger)

	// Set to OFF level
	logger.SetLevel(OFF)

	// Directly call the internal log method
	logger.log(ERROR, "test message")

	// Verification has no output
	output := buf.String()
	if len(output) > 0 {
		t.Errorf("Expected no output when level is OFF, got: %s", output)
	}
}

// TestDiscardLoggerAllMethods tests all methods for discarding loggers
func TestDiscardLoggerAllMethods(t *testing.T) {
	logger := NewDiscardLogger()

	// Test logging methods at all levels
	logger.Debug("debug %s", "test")
	logger.Info("info %d", 123)
	logger.Warn("warn %v", true)
	logger.Error("error %s %d", "test", 456)

	// Test the level settings
	logger.SetLevel(DEBUG)
	logger.SetLevel(INFO)
	logger.SetLevel(WARN)
	logger.SetLevel(ERROR)
	logger.SetLevel(OFF)

	// If there is no panic or error, the test passes
}

// TestLoggerWithNilArgs tests logging using nil parameters
func TestLoggerWithNilArgs(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(DEBUG, &buf)

	// Testing without parameters
	logger.Info("message without args")
	output := buf.String()
	if !strings.Contains(output, "message without args") {
		t.Errorf("Expected message in output, got: %s", output)
	}

	// Test null parameter list
	buf.Reset()
	logger.Info("message with empty args")
	output = buf.String()
	if !strings.Contains(output, "message with empty args") {
		t.Errorf("Expected message in output, got: %s", output)
	}
}

// TestLoggerTimestampFormat: Test the timestamp format
func TestLoggerTimestampFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(INFO, &buf)

	logger.Info("timestamp test")
	output := buf.String()

	// Check timestamp format: [YYYY-MM-DD HH:MM:SS.mmm]
	// Use regular expressions to verify the timestamp format
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) == 0 {
		t.Fatal("No output lines found")
	}

	line := lines[0]
	// Check whether the date and time format is included
	if !strings.Contains(line, "[") || !strings.Contains(line, "]") {
		t.Errorf("Expected timestamp in brackets, got: %s", line)
	}

	// Check if the year is included (simple verification)
	if !strings.Contains(line, "2025") && !strings.Contains(line, "2024") && !strings.Contains(line, "2026") {
		t.Errorf("Expected year in timestamp, got: %s", line)
	}
}

// TestGlobalLoggerRestore tests the recovery of the global logger
func TestGlobalLoggerRestore(t *testing.T) {
	// Preserves the original global logger
	original := GetDefault()

	// Create a new test logger
	var buf bytes.Buffer
	testLogger := NewLogger(ERROR, &buf)
	SetDefault(testLogger)

	// Verify that the setup is successful
	if GetDefault() != testLogger {
		t.Error("Failed to set test logger")
	}

	// Restore the original logger
	SetDefault(original)

	// Verification of successful recovery
	if GetDefault() != original {
		t.Error("Failed to restore original logger")
	}
}

// TestLevelConstants tests all log-level constants
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

	// Test level values
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
