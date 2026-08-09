package functions

import (
	"testing"
)

// Benchmarks isolating ExprBridge per-row expression evaluation. These exercise
// the expr-lang fallback path used by SELECT field expressions, where (before
// M15) every call recompiled the expression under a global write lock.

func benchEval(b *testing.B, expr string, data map[string]any) {
	bridge := GetExprBridge()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bridge.EvaluateExpression(expr, data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExprBridge_Arithmetic(b *testing.B) {
	benchEval(b, "temperature * 2 + humidity", map[string]any{
		"temperature": 25.7,
		"humidity":    65.0,
	})
}

func BenchmarkExprBridge_FunctionCall(b *testing.B) {
	benchEval(b, "abs(temperature - 100)", map[string]any{
		"temperature": 25.7,
	})
}

func BenchmarkExprBridge_StringConcat(b *testing.B) {
	benchEval(b, "device + '-' + location", map[string]any{
		"device":   "sensor01",
		"location": "room_a",
	})
}

func BenchmarkExprBridge_Field(b *testing.B) {
	benchEval(b, "temperature", map[string]any{
		"temperature": 25.7,
	})
}

// usesExprFunction runs on every row but its verdict depends only on the
// expression text. Before caching, its regex dominated CPU on the computed-field
// path (~26% of samples). These isolate that check.
func BenchmarkExprBridge_UsesExprFunction(b *testing.B) {
	bridge := GetExprBridge()
	const expression = "temperature * 2 + humidity - offset / scale"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bridge.usesExprFunction(expression)
	}
}

// Uncached baseline for comparison: the raw regex the cache replaces.
func BenchmarkExprBridge_UsesExprFunctionRegex(b *testing.B) {
	const expression = "temperature * 2 + humidity - offset / scale"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exprCallPattern.MatchString(expression)
	}
}

// Isolate the per-call environment construction cost.
func BenchmarkExprBridge_CreateEnv(b *testing.B) {
	bridge := GetExprBridge()
	data := map[string]any{"temperature": 25.7, "humidity": 65.0}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bridge.CreateEnhancedExprEnvironment(data)
	}
}

// Isolate ListAll (registry snapshot) cost.
func BenchmarkExprBridge_ListAll(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ListAll()
	}
}
