package streamsql

import (
	"testing"
)

// Integration benchmarks exercising the full main path with realistic RSQL.
// EmitSync processes each row synchronously end-to-end (the same path users
// call), so ns/op is the true per-row latency through ProcessData -> field
// evaluation -> result building. Aggregation queries are exercised separately
// via the Emit-based benchmarks.

func benchEmitSync(b *testing.B, sql string, row map[string]interface{}) {
	b.Helper()
	ssql := New()
	defer ssql.Stop()
	if err := ssql.Execute(sql); err != nil {
		b.Fatalf("Execute: %v", err)
	}

	// Warm up compile/preprocess caches (do not measure).
	if _, err := ssql.EmitSync(row); err != nil {
		b.Fatalf("warmup EmitSync: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ssql.EmitSync(row); err != nil {
			b.Fatalf("EmitSync: %v", err)
		}
	}
	b.StopTimer()
}

func BenchmarkMainPath_FilterProject(b *testing.B) {
	benchEmitSync(b,
		"SELECT deviceId, temperature FROM stream WHERE temperature > 20",
		map[string]interface{}{"deviceId": "d1", "temperature": 25.5, "humidity": 60.0},
	)
}

func BenchmarkMainPath_MultiFieldFilter(b *testing.B) {
	benchEmitSync(b,
		"SELECT deviceId, temperature, humidity FROM stream WHERE temperature > 20 AND humidity < 80",
		map[string]interface{}{"deviceId": "d1", "temperature": 25.5, "humidity": 60.0},
	)
}

func BenchmarkMainPath_ComputedFields(b *testing.B) {
	benchEmitSync(b,
		"SELECT deviceId, temperature * 2 + humidity AS score, abs(temperature - 100) AS dev FROM stream WHERE temperature > 20",
		map[string]interface{}{"deviceId": "d1", "temperature": 25.5, "humidity": 60.0},
	)
}

func BenchmarkMainPath_StringConcat(b *testing.B) {
	benchEmitSync(b,
		"SELECT deviceId + '-' + location AS id FROM stream",
		map[string]interface{}{"deviceId": "d1", "location": "roomA"},
	)
}

func BenchmarkMainPath_NoFilter(b *testing.B) {
	benchEmitSync(b,
		"SELECT deviceId, temperature, humidity FROM stream",
		map[string]interface{}{"deviceId": "d1", "temperature": 25.5, "humidity": 60.0},
	)
}
