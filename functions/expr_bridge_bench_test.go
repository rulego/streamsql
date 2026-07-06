package functions

import (
	"testing"
)

// Benchmarks isolating ExprBridge per-row expression evaluation. These exercise
// the expr-lang fallback path used by SELECT field expressions, where (before
// M15) every call recompiled the expression under a global write lock.

func benchEval(b *testing.B, expr string, data map[string]interface{}) {
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
	benchEval(b, "temperature * 2 + humidity", map[string]interface{}{
		"temperature": 25.7,
		"humidity":    65.0,
	})
}

func BenchmarkExprBridge_FunctionCall(b *testing.B) {
	benchEval(b, "abs(temperature - 100)", map[string]interface{}{
		"temperature": 25.7,
	})
}

func BenchmarkExprBridge_StringConcat(b *testing.B) {
	benchEval(b, "device + '-' + location", map[string]interface{}{
		"device":   "sensor01",
		"location": "room_a",
	})
}

func BenchmarkExprBridge_Field(b *testing.B) {
	benchEval(b, "temperature", map[string]interface{}{
		"temperature": 25.7,
	})
}

// Isolate the per-call environment construction cost.
func BenchmarkExprBridge_CreateEnv(b *testing.B) {
	bridge := GetExprBridge()
	data := map[string]interface{}{"temperature": 25.7, "humidity": 65.0}
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

