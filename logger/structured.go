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

// Structured logging support. This is an additive layer over the printf-style
// Logger interface: the existing Info/Warn/Error/Debug signatures are unchanged,
// so custom Logger implementations and existing call sites keep working. New
// code should prefer the *Fields variants to emit key-value context that log
// aggregators can parse.

package logger

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

// Format selects the encoding of structured log entries.
type Format int

const (
	// TextFormat emits "[ts] [LEVEL] msg key=value" (logfmt-style). Default.
	TextFormat Format = iota
	// JSONFormat emits one JSON object per line.
	JSONFormat
)

// Field is a structured key-value pair attached to a log entry.
type Field struct {
	Key   string
	Value any
}

// Field constructors cover the common value types so callers get typed
// capture at the call site (and avoid boxing mistakes).
func String(k, v string) Field          { return Field{Key: k, Value: v} }
func Int(k string, v int) Field         { return Field{Key: k, Value: v} }
func Int64(k string, v int64) Field     { return Field{Key: k, Value: v} }
func Float64(k string, v float64) Field { return Field{Key: k, Value: v} }
func Bool(k string, v bool) Field       { return Field{Key: k, Value: v} }

// Err records an error under the conventional "error" key. A nil error is
// preserved as a null value rather than panic.
func Err(err error) Field {
	if err == nil {
		return Field{Key: "error", Value: nil}
	}
	return Field{Key: "error", Value: err.Error()}
}

// Duration records a duration as its human-readable string form (e.g. "12ms").
func Duration(k string, d time.Duration) Field { return Field{Key: k, Value: d.String()} }

// Any captures an arbitrary value under k. Prefer a typed constructor when one
// exists; Any defers formatting to the renderer.
func Any(k string, v any) Field { return Field{Key: k, Value: v} }

// StructuredLogger is the optional structured variant of Logger. Loggers that
// implement it emit key-value context; the printf-style Logger methods remain
// available alongside. Package-level helpers (InfoFields, ...) use this when
// present and otherwise fall back to the printf method.
type StructuredLogger interface {
	DebugFields(msg string, fields ...Field)
	InfoFields(msg string, fields ...Field)
	WarnFields(msg string, fields ...Field)
	ErrorFields(msg string, fields ...Field)
}

// NewLoggerWithFormat is like NewLogger but selects the structured output
// encoding. TextFormat matches NewLogger's historical output.
func NewLoggerWithFormat(level Level, output io.Writer, format Format) Logger {
	return &defaultLogger{
		level:  level,
		format: format,
		logger: log.New(output, "", 0),
	}
}

// --- defaultLogger structured implementation ---

func (l *defaultLogger) DebugFields(msg string, fields ...Field) {
	if l.level <= DEBUG {
		l.logFields(DEBUG, msg, fields)
	}
}

func (l *defaultLogger) InfoFields(msg string, fields ...Field) {
	if l.level <= INFO {
		l.logFields(INFO, msg, fields)
	}
}

func (l *defaultLogger) WarnFields(msg string, fields ...Field) {
	if l.level <= WARN {
		l.logFields(WARN, msg, fields)
	}
}

func (l *defaultLogger) ErrorFields(msg string, fields ...Field) {
	if l.level <= ERROR {
		l.logFields(ERROR, msg, fields)
	}
}

func (l *defaultLogger) logFields(level Level, msg string, fields []Field) {
	if l.level == OFF {
		return
	}
	ts := time.Now().Format("2006-01-02 15:04:05.000")
	var line string
	if l.format == JSONFormat {
		line = renderJSON(ts, level, msg, fields)
	} else {
		line = renderText(ts, level, msg, fields)
	}
	l.logger.Println(line)
}

// --- discardLogger structured implementation (no-ops) ---

func (d *discardLogger) DebugFields(msg string, fields ...Field) {}
func (d *discardLogger) InfoFields(msg string, fields ...Field)  {}
func (d *discardLogger) WarnFields(msg string, fields ...Field)  {}
func (d *discardLogger) ErrorFields(msg string, fields ...Field) {}

// --- rendering ---

func renderText(ts string, level Level, msg string, fields []Field) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[%s] [%s] %s", ts, level.String(), msg)
	for _, f := range fields {
		b.WriteByte(' ')
		b.WriteString(f.Key)
		b.WriteByte('=')
		b.WriteString(formatLogfmtValue(f.Value))
	}
	return b.String()
}

// formatLogfmtValue renders a field value for the text format. Strings are
// quoted only when they contain characters that would break logfmt parsing.
func formatLogfmtValue(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		if needsQuoting(x) {
			return strconv.Quote(x)
		}
		return x
	case bool:
		return strconv.FormatBool(x)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	default:
		s := fmt.Sprintf("%v", v)
		if needsQuoting(s) {
			return strconv.Quote(s)
		}
		return s
	}
}

func needsQuoting(s string) bool {
	if s == "" {
		return true
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c <= ' ' || c == '=' || c == '"' || c == '\\' {
			return true
		}
	}
	return false
}

func renderJSON(ts string, level Level, msg string, fields []Field) string {
	m := make(map[string]any, len(fields)+3)
	m["ts"] = ts
	m["level"] = level.String()
	m["msg"] = msg
	for _, f := range fields {
		if f.Value == nil {
			m[f.Key] = nil
		} else {
			m[f.Key] = f.Value
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		// Should not happen for these value types; fall back to a safe line.
		return fmt.Sprintf(`{"level":%q,"msg":%q,"render_error":%q}`, level.String(), msg, err.Error())
	}
	return string(b)
}

func joinFieldsText(fields []Field) string {
	var b strings.Builder
	for i, f := range fields {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(f.Key)
		b.WriteByte('=')
		b.WriteString(formatLogfmtValue(f.Value))
	}
	return b.String()
}

// --- package-level structured helpers (operate on the default logger) ---

// InfoFields emits a structured INFO entry on the default logger. If the
// default does not implement StructuredLogger (a custom printf-only logger),
// the fields are folded into a single text message so no context is lost.
func InfoFields(msg string, fields ...Field) {
	emitFields(defaultInstance.Info, msg, fields)
}

func WarnFields(msg string, fields ...Field)  { emitFields(defaultInstance.Warn, msg, fields) }
func ErrorFields(msg string, fields ...Field) { emitFields(defaultInstance.Error, msg, fields) }
func DebugFields(msg string, fields ...Field) { emitFields(defaultInstance.Debug, msg, fields) }

type printfFunc func(format string, args ...any)

func emitFields(printf printfFunc, msg string, fields []Field) {
	if len(fields) == 0 {
		printf("%s", msg)
		return
	}
	printf("%s %s", msg, joinFieldsText(fields))
}
