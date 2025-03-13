package rsql

import (
	"github.com/rulego/streamsql/aggregator"
	"testing"
	"time"

	"github.com/rulego/streamsql/stream"
	"github.com/stretchr/testify/assert"
)

func TestParseSQL(t *testing.T) {
	tests := []struct {
		sql       string
		expected  *stream.Config
		condition string
	}{
		{
			sql: "select deviceId, avg(temperature/10) as aa from Input where deviceId='aa' group by deviceId, TumblingWindow('10s')",
			expected: &stream.Config{
				WindowConfig: stream.WindowConfig{
					Type: "tumbling",
					Params: map[string]interface{}{
						"size": 10 * time.Second,
					},
				},
				GroupFields: []string{"deviceId"},
				SelectFields: map[string]aggregator.AggregateType{
					"aa": "avg",
				},
			},
			condition: "deviceId == 'aa'",
		},
		{
			sql: "select max(score) as max_score, min(age) as min_age from Sensor group by type, SlidingWindow('20s', '5s')",
			expected: &stream.Config{
				WindowConfig: stream.WindowConfig{
					Type: "sliding",
					Params: map[string]interface{}{
						"size":  20 * time.Second,
						"slide": 5 * time.Second,
					},
				},
				GroupFields: []string{"type"},
				SelectFields: map[string]aggregator.AggregateType{
					"max_score": "max",
					"min_age":   "min",
				},
			},
			condition: "",
		},
	}

	for _, tt := range tests {
		parser := NewParser(tt.sql)
		stmt, err := parser.Parse()
		assert.NoError(t, err)

		config, cond, err := stmt.ToStreamConfig()
		assert.NoError(t, err)

		assert.Equal(t, tt.expected.WindowConfig.Type, config.WindowConfig.Type)
		assert.Equal(t, tt.expected.WindowConfig.Params["size"], config.WindowConfig.Params["size"])
		assert.Equal(t, tt.expected.GroupFields, config.GroupFields)
		assert.Equal(t, tt.expected.SelectFields, config.SelectFields)
		assert.Equal(t, tt.condition, cond)
	}
}
func TestWindowParamParsing(t *testing.T) {
	params := []interface{}{"10s", "5s"}
	result, err := parseWindowParams(params)
	assert.NoError(t, err)
	assert.Equal(t, 10*time.Second, result["size"])
	assert.Equal(t, 5*time.Second, result["slide"])
}

func TestConditionParsing(t *testing.T) {
	sql := "select * from metrics where cpu > 80 || (mem < 20 && disk == '/dev/sda')"
	expected := "cpu > 80 || (mem < 20 && disk == '/dev/sda')"

	parser := NewParser(sql)
	stmt, err := parser.Parse()
	assert.NoError(t, err)
	assert.Equal(t, expected, stmt.Condition)
}
