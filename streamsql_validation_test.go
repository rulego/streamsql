package streamsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFunctionValidationIntegration(t *testing.T) {
	tests := []struct {
		name           string
		sql            string
		expectError    bool
		errorContains  string
	}{
		{
			name:        "Valid builtin function in SELECT",
			sql:         "SELECT abs(temperature) FROM stream",
			expectError: false,
		},
		{
			name:          "Unknown function in SELECT",
			sql:           "SELECT unknown_func(temperature) FROM stream",
			expectError:   true,
			errorContains: "unknown_func",
		},
		{
			name:          "Unknown function in WHERE",
			sql:           "SELECT temperature FROM stream WHERE unknown_func(temperature) > 0",
			expectError:   true,
			errorContains: "unknown_func",
		},
		{
			name:          "Unknown function in HAVING",
			sql:           "SELECT temperature FROM stream GROUP BY device HAVING unknown_func(temperature) > 0",
			expectError:   true,
			errorContains: "unknown_func",
		},
		{
			name:        "Valid nested functions",
			sql:         "SELECT sqrt(abs(temperature)) FROM stream",
			expectError: false,
		},
		{
			name:          "Mixed valid and invalid functions",
			sql:           "SELECT abs(temperature), unknown_func(humidity) FROM stream",
			expectError:   true,
			errorContains: "unknown_func",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ssql := New()
			err := ssql.Execute(tt.sql)

			if tt.expectError {
				assert.Error(t, err, "Expected error for SQL: %s", tt.sql)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains, "Error should contain: %s", tt.errorContains)
				}
			} else {
				assert.NoError(t, err, "Expected no error for SQL: %s", tt.sql)
			}
		})
	}
}

func TestFunctionValidationWithCustomFunctions(t *testing.T) {
	// 测试自定义函数注册后的验证
	sql := "SELECT custom_func(temperature) FROM stream"
	
	// 在没有注册自定义函数时应该报错
	ssql := New()
	err := ssql.Execute(sql)
	assert.Error(t, err, "Should error when custom function is not registered")
	assert.Contains(t, err.Error(), "custom_func", "Error should mention the unknown function")
}