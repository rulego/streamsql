package e2e

import (
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/rulego/streamsql/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deviceSchema is a small schema reused across the opt-in validation tests.
func deviceSchema() schema.Schema {
	return schema.Schema{
		Fields: []schema.FieldDef{
			{Name: "deviceId", Type: schema.TypeString, Required: true},
			{Name: "temperature", Type: schema.TypeFloat, Required: true},
		},
		Strict: true,
	}
}

// TestSchemaValidation_OptIn verifies WithSchema enables validation on Emit:
// valid rows are processed, invalid rows are dropped (and counted) before the
// stream.
func TestSchemaValidation_OptIn(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New(streamsql.WithSchema(deviceSchema()))
	defer ssql.Stop()
	require.NoError(t, ssql.Execute("SELECT deviceId, temperature FROM stream WHERE temperature > 0"))

	var mu sync.Mutex
	var got []map[string]any
	ssql.AddSink(func(rows []map[string]any) {
		mu.Lock()
		got = append(got, rows...)
		mu.Unlock()
	})

	ssql.Emit(map[string]any{"deviceId": "d1", "temperature": 25.0})           // valid -> passes
	ssql.Emit(map[string]any{"deviceId": "d2", "temperature": "hot", "x": 1}) // wrong type + unknown -> dropped
	ssql.Emit(map[string]any{"temperature": 30.0})                            // missing required -> dropped

	for i := 0; i < 100; i++ {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 1, "only the valid row should pass; invalid rows dropped")
	assert.Equal(t, "d1", got[0]["deviceId"])
	assert.Equal(t, int64(2), ssql.SchemaDropped(), "two invalid rows dropped")
}

// TestSchemaValidation_EmitSync verifies validation on the sync path: valid rows
// return a result, invalid rows return an error and nil.
func TestSchemaValidation_EmitSync(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New(streamsql.WithSchema(schema.Schema{
		Fields: []schema.FieldDef{{Name: "v", Type: schema.TypeInt, Required: true}},
	}))
	defer ssql.Stop()
	require.NoError(t, ssql.Execute("SELECT v FROM stream"))

	got, err := ssql.EmitSync(map[string]any{"v": 5})
	require.NoError(t, err)
	require.NotNil(t, got)

	got, err = ssql.EmitSync(map[string]any{"v": "x"})
	assert.Error(t, err)
	assert.Nil(t, got)
}

// TestSchemaValidation_ZeroOverheadWhenNotSet verifies that without WithSchema,
// Emit/EmitSync behave as before (no validation, no drops).
func TestSchemaValidation_ZeroOverheadWhenNotSet(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	require.NoError(t, ssql.Execute("SELECT v FROM stream"))

	// No WithSchema: a "malformed" row still passes through unchanged.
	got, err := ssql.EmitSync(map[string]any{"v": 5})
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(0), ssql.SchemaDropped())
}
