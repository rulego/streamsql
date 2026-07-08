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

package schema

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInferType(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name string
		in   any
		want DataType
	}{
		{"nil", nil, TypeAny},
		{"int", int(1), TypeInt},
		{"int64", int64(1), TypeInt64},
		{"float32", float32(1), TypeFloat},
		{"float64", float64(1), TypeFloat},
		{"bool", true, TypeBool},
		{"string", "x", TypeString},
		{"time", now, TypeTime},
		{"array", []any{1, 2}, TypeArray},
		{"map", map[string]any{"a": 1}, TypeMap},
		{"unknown struct", struct{}{}, TypeAny},
		{"typed int slice", []int{1}, TypeAny},
		{"int-keyed map", map[int]any{1: 2}, TypeAny},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, InferType(tt.in))
		})
	}
}

func TestDataType_String(t *testing.T) {
	tests := []struct {
		t    DataType
		want string
	}{
		{TypeInt, "int"},
		{TypeInt64, "int64"},
		{TypeFloat, "float"},
		{TypeBool, "bool"},
		{TypeString, "string"},
		{TypeTime, "time"},
		{TypeArray, "array"},
		{TypeMap, "map"},
		{TypeAny, "any"},
		{DataType(999), "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.t.String())
		})
	}
}

func TestRegistry_Register(t *testing.T) {
	r := NewRegistry()

	require.NoError(t, r.Register(Schema{Name: "user", Fields: []FieldDef{{Name: "id", Type: TypeInt}}}))
	require.Error(t, r.Register(Schema{Name: ""}))     // empty name rejected
	require.Error(t, r.Register(Schema{Name: "user"})) // duplicate rejected

	_, ok := r.Get("user")
	require.True(t, ok, "first registration must persist")
}

func TestRegistry_Get(t *testing.T) {
	r := NewRegistry()
	require.NoError(t, r.Register(Schema{Name: "user", Fields: []FieldDef{{Name: "id"}}}))

	got, ok := r.Get("user")
	require.True(t, ok)
	assert.Equal(t, "user", got.Name)
	assert.Len(t, got.Fields, 1)

	_, ok = r.Get("missing")
	assert.False(t, ok)
}

func TestRegistry_MustGet(t *testing.T) {
	r := NewRegistry()
	require.NoError(t, r.Register(Schema{Name: "user"}))

	assert.NotPanics(t, func() { _ = r.MustGet("user") })
	assert.Panics(t, func() { _ = r.MustGet("missing") })
}

func TestSchema_Validate(t *testing.T) {
	now := time.Now()
	sch := Schema{
		Name: "record",
		Fields: []FieldDef{
			{Name: "id", Type: TypeInt, Required: true},
			{Name: "name", Type: TypeString},
			{Name: "score", Type: TypeFloat, Default: float64(0)},
			{Name: "tags", Type: TypeArray},
			{Name: "meta", Type: TypeMap},
			{Name: "at", Type: TypeTime},
			{Name: "anything", Type: TypeAny},
		},
	}
	strictSch := Schema{
		Name:   "strict",
		Strict: true,
		Fields: []FieldDef{{Name: "id", Type: TypeInt}},
	}
	reqWithDefault := Schema{
		Name: "reqDefault",
		Fields: []FieldDef{
			{Name: "id", Type: TypeInt, Required: true, Default: int(0)},
		},
	}

	tests := []struct {
		name    string
		schema  Schema
		data    map[string]any
		wantErr bool
	}{
		{"all types match", sch, map[string]any{
			"id": int(1), "name": "n", "score": float64(2.5),
			"tags": []any{"a"}, "meta": map[string]any{"k": "v"},
			"at": now, "anything": struct{}{},
		}, false},
		{"optional fields missing", sch, map[string]any{"id": int(1)}, false},
		{"required-with-default missing", reqWithDefault, map[string]any{}, false},
		{"required-with-default present wrong type", reqWithDefault, map[string]any{"id": "bad"}, true},
		{"numeric int into float field", sch, map[string]any{"id": int(1), "score": int(5)}, false},
		{"numeric int64 into float field", sch, map[string]any{"id": int(1), "score": int64(5)}, false},
		{"numeric float into int field", sch, map[string]any{"id": float64(5)}, false},
		{"nested map value", sch, map[string]any{
			"id":   int(1),
			"meta": map[string]any{"nested": map[string]any{"deep": 1}},
		}, false},
		{"nil into TypeAny", sch, map[string]any{"id": int(1), "anything": nil}, false},
		{"strict allows declared only", strictSch, map[string]any{"id": int(1)}, false},

		{"required missing", sch, map[string]any{}, true},
		{"type mismatch string into int", sch, map[string]any{"id": "nope"}, true},
		{"type mismatch nil into string", sch, map[string]any{"id": int(1), "name": nil}, true},
		{"type mismatch array into map", sch, map[string]any{"id": int(1), "meta": []any{1}}, true},
		{"strict rejects unknown", strictSch, map[string]any{"id": int(1), "extra": 1}, true},
		{"lenient ignores unknown", sch, map[string]any{"id": int(1), "who": "knows"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.schema.Validate(tt.data)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSchema_Validate_Aggregates(t *testing.T) {
	sch := Schema{
		Name: "multi",
		Fields: []FieldDef{
			{Name: "a", Type: TypeInt, Required: true},
			{Name: "b", Type: TypeString, Required: true},
		},
		Strict: true,
	}
	// "c" is unknown (strict), "a" present but wrong type, "b" missing required.
	err := sch.Validate(map[string]any{"c": 1, "a": "bad"})
	require.Error(t, err)

	msg := err.Error()
	assert.Contains(t, msg, `unknown field "c"`)
	assert.Contains(t, msg, `field "a" expects int, got string`)
	assert.Contains(t, msg, `required field "b" is missing`)

	var multi *MultiError
	require.ErrorAs(t, err, &multi)
	assert.Len(t, multi.Errors, 3)
}

func TestMultiError_Empty(t *testing.T) {
	var m MultiError
	require.NoError(t, m.Err())
	assert.Empty(t, m.Error())
	m.Append(nil) // nil is ignored, still empty
	require.NoError(t, m.Err())
}

func TestRegistry_Concurrent(t *testing.T) {
	r := NewRegistry()
	const goroutines = 50
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		go func(n int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = r.Register(Schema{Name: fmt.Sprintf("s%d", n)})
			}
		}(i)
	}
	for i := 0; i < goroutines; i++ {
		go func(n int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = r.Get(fmt.Sprintf("s%d", n))
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < goroutines; i++ {
		_, ok := r.Get(fmt.Sprintf("s%d", i))
		assert.True(t, ok, "schema s%d must be registered", i)
	}
}

func TestDefaultRegistry(t *testing.T) {
	// Isolate from other tests by using a unique name.
	const name = "schema_default_test_singleton"
	require.NoError(t, Default.Register(Schema{Name: name}))
	got, ok := Default.Get(name)
	require.True(t, ok)
	assert.Equal(t, name, got.Name)
}
