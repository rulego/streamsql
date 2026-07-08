package logger

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureLogger builds a defaultLogger writing to a buffer for inspection.
func captureLogger(level Level, format Format) (*defaultLogger, *bytes.Buffer) {
	var buf bytes.Buffer
	return NewLoggerWithFormat(level, &buf, format).(*defaultLogger), &buf
}

func TestStructured_TextFormat(t *testing.T) {
	l, buf := captureLogger(INFO, TextFormat)
	l.InfoFields("window fired", String("deviceId", "d1"), Int("rows", 42))
	line := buf.String()
	assert.Contains(t, line, "[INFO] window fired")
	assert.Contains(t, line, "deviceId=d1")
	assert.Contains(t, line, "rows=42")
	assert.NotContains(t, line, `"rows"`, "int value must not be quoted")
}

func TestStructured_TextFormatQuotesSpaces(t *testing.T) {
	l, buf := captureLogger(INFO, TextFormat)
	l.InfoFields("msg", String("loc", "room A"))
	assert.Contains(t, buf.String(), `loc="room A"`, "value with space must be quoted")
}

func TestStructured_JSONFormat(t *testing.T) {
	l, buf := captureLogger(INFO, JSONFormat)
	l.InfoFields("window fired", String("deviceId", "d1"), Int("rows", 42))
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &parsed))
	assert.Equal(t, "window fired", parsed["msg"])
	assert.Equal(t, "INFO", parsed["level"])
	assert.Equal(t, "d1", parsed["deviceId"])
	assert.EqualValues(t, 42, parsed["rows"])
}

func TestStructured_LevelFiltering(t *testing.T) {
	l, buf := captureLogger(WARN, TextFormat) // WARN suppresses INFO/DEBUG
	l.InfoFields("should not appear", String("k", "v"))
	l.WarnFields("should appear", String("k", "v"))
	out := buf.String()
	assert.NotContains(t, out, "should not appear")
	assert.Contains(t, out, "should appear")
}

func TestStructured_ErrFieldNilSafe(t *testing.T) {
	l, buf := captureLogger(INFO, JSONFormat)
	l.ErrorFields("bad row", Err(nil)) // must not panic
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &parsed))
	assert.Contains(t, parsed, "error")

	l2, buf2 := captureLogger(INFO, TextFormat)
	l2.ErrorFields("bad row", Err(errors.New("connection refused")))
	assert.Contains(t, buf2.String(), `error="connection refused"`, "value with space must be quoted")
}

func TestStructured_DurationField(t *testing.T) {
	l, buf := captureLogger(INFO, TextFormat)
	l.InfoFields("lat", Duration("proc", 12*time.Millisecond))
	assert.Contains(t, buf.String(), "proc=12ms")
}

func TestStructured_DiscardLoggerNoOp(t *testing.T) {
	d := NewDiscardLogger().(StructuredLogger)
	assert.NotPanics(t, func() {
		d.DebugFields("x", String("k", "v"))
		d.InfoFields("x")
		d.WarnFields("x")
		d.ErrorFields("x")
	})
}

// printfOnly is a Logger that does NOT implement StructuredLogger, exercising
// the package-level fallback path.
type printfOnly struct{ b bytes.Buffer }

func (p *printfOnly) Debug(format string, a ...any) { p.b.WriteString("D ") }
func (p *printfOnly) Info(format string, a ...any)  { p.b.WriteString("I ") }
func (p *printfOnly) Warn(format string, a ...any)  { p.b.WriteString("W ") }
func (p *printfOnly) Error(format string, a ...any) { p.b.WriteString("E ") }
func (p *printfOnly) SetLevel(Level)                {}

func TestStructured_PackageLevelWithStructuredDefault(t *testing.T) {
	var buf bytes.Buffer
	prev := GetDefault()
	defer SetDefault(prev)
	SetDefault(NewLoggerWithFormat(INFO, &buf, TextFormat))

	InfoFields("hello", String("who", "world"))
	out := buf.String()
	assert.Contains(t, out, "hello")
	assert.Contains(t, out, "who=world")
}

func TestStructured_PackageLevelFallbackForPrintfOnly(t *testing.T) {
	prev := GetDefault()
	defer SetDefault(prev)
	p := &printfOnly{}
	SetDefault(p) // printf-only, no StructuredLogger

	// Fields fold into the message; Info receives them (no context lost).
	InfoFields("hello", String("who", "world"))
	assert.True(t, strings.HasPrefix(p.b.String(), "I "), "printf fallback must still call Info")
}

func TestStructured_DefaultNewLoggerIsTextFormat(t *testing.T) {
	// NewLogger (unchanged historical ctor) must default to TextFormat, so
	// existing behavior is preserved.
	var buf bytes.Buffer
	l := NewLogger(INFO, &buf).(*defaultLogger)
	assert.Equal(t, TextFormat, l.format)
	l.InfoFields("m", String("k", "v"))
	assert.Contains(t, buf.String(), "k=v")
}
