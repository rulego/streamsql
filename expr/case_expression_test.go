package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEvaluateCaseExpression 测试CASE表达式求值
func TestEvaluateCaseExpression(t *testing.T) {
	tests := []struct {
		name     string
		node     *ExprNode
		data     map[string]interface{}
		expected float64
		wantErr  bool
	}{
		{
			"简单CASE表达式",
			&ExprNode{
				Type: TypeCase,
				CaseExpr: &CaseExpression{
					WhenClauses: []WhenClause{
						{
							Condition: &ExprNode{
								Type:  TypeOperator,
								Value: "=",
								Left:  &ExprNode{Type: TypeField, Value: "status"},
								Right: &ExprNode{Type: TypeNumber, Value: "1"},
							},
							Result: &ExprNode{Type: TypeNumber, Value: "100"},
						},
					},
				},
			},
			map[string]interface{}{"status": 1},
			100,
			false,
		},
		{
			"带ELSE的CASE表达式",
			&ExprNode{
				Type: TypeCase,
				CaseExpr: &CaseExpression{
					WhenClauses: []WhenClause{
						{
							Condition: &ExprNode{
								Type:  TypeOperator,
								Value: ">",
								Left:  &ExprNode{Type: TypeField, Value: "score"},
								Right: &ExprNode{Type: TypeNumber, Value: "90"},
							},
							Result: &ExprNode{Type: TypeNumber, Value: "1"},
						},
					},
					ElseResult: &ExprNode{Type: TypeNumber, Value: "0"},
				},
			},
			map[string]interface{}{"score": 85},
			0,
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluateCaseExpression(tt.node, tt.data)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				require.NoError(t, err, "求值不应该失败")
				assert.Equal(t, tt.expected, result, "求值结果应该正确")
			}
		})
	}
}

// TestEvaluateCaseExpressionWithNull 测试支持NULL的CASE表达式求值
func TestEvaluateCaseExpressionWithNull(t *testing.T) {
	tests := []struct {
		name         string
		node         *ExprNode
		data         map[string]interface{}
		expected     interface{}
		expectedNull bool
		wantErr      bool
	}{
		{
			"条件为NULL时返回NULL",
			&ExprNode{
				Type: TypeCase,
				CaseExpr: &CaseExpression{
					WhenClauses: []WhenClause{
						{
							Condition: &ExprNode{Type: TypeField, Value: "missing_field"},
							Result:    &ExprNode{Type: TypeNumber, Value: "1"},
						},
					},
				},
			},
			map[string]interface{}{},
			nil,
			true,
			false,
		},
		{
			"简单CASE表达式匹配",
			&ExprNode{
				Type: TypeCase,
				CaseExpr: &CaseExpression{
					Value: &ExprNode{Type: TypeField, Value: "status"},
					WhenClauses: []WhenClause{
						{
							Condition: &ExprNode{Type: TypeString, Value: "'active'"},
							Result:    &ExprNode{Type: TypeNumber, Value: "1"},
						},
						{
							Condition: &ExprNode{Type: TypeString, Value: "'inactive'"},
							Result:    &ExprNode{Type: TypeNumber, Value: "0"},
						},
					},
					ElseResult: &ExprNode{Type: TypeNumber, Value: "-1"},
				},
			},
			map[string]interface{}{"status": "active"},
			1.0,
			false,
			false,
		},
		{
			"简单CASE表达式不匹配使用ELSE",
			&ExprNode{
				Type: TypeCase,
				CaseExpr: &CaseExpression{
					Value: &ExprNode{Type: TypeField, Value: "status"},
					WhenClauses: []WhenClause{
						{
							Condition: &ExprNode{Type: TypeString, Value: "'active'"},
							Result:    &ExprNode{Type: TypeNumber, Value: "1"},
						},
					},
					ElseResult: &ExprNode{Type: TypeNumber, Value: "0"},
				},
			},
			map[string]interface{}{"status": "unknown"},
			0.0,
			false,
			false,
		},
		{
			"简单CASE表达式Value为NULL",
			&ExprNode{
				Type: TypeCase,
				CaseExpr: &CaseExpression{
					Value: &ExprNode{Type: TypeField, Value: "missing_field"},
					WhenClauses: []WhenClause{
						{
							Condition: &ExprNode{Type: TypeString, Value: "'test'"},
							Result:    &ExprNode{Type: TypeNumber, Value: "1"},
						},
					},
					ElseResult: &ExprNode{Type: TypeNumber, Value: "0"},
				},
			},
			map[string]interface{}{},
			0.0,
			false,
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, isNull, err := evaluateCaseExpressionWithNull(tt.node, tt.data)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				require.NoError(t, err, "求值不应该失败")
				assert.Equal(t, tt.expectedNull, isNull, "NULL状态应该正确")
				if !isNull {
					assert.Equal(t, tt.expected, result, "求值结果应该正确")
				}
			}
		})
	}
}

func TestParseCaseExpression(t *testing.T) {
	tests := []struct {
		name        string
		tokens      []string
		expectError bool
		description string
	}{
		{
			name:        "empty tokens",
			tokens:      []string{},
			expectError: true,
			description: "should return error for empty tokens",
		},
		{
			name:        "not case keyword",
			tokens:      []string{"SELECT", "field"},
			expectError: true,
			description: "should return error when first token is not CASE",
		},
		{
			name:        "missing when after case",
			tokens:      []string{"CASE", "field"},
			expectError: true,
			description: "should return error when missing WHEN after CASE",
		},
		{
			name:        "missing then after when",
			tokens:      []string{"CASE", "WHEN", "field1", ">", "0"},
			expectError: true,
			description: "should return error when missing THEN after WHEN",
		},
		{
			name:        "missing end",
			tokens:      []string{"CASE", "WHEN", "field1", ">", "0", "THEN", "1"},
			expectError: true,
			description: "should return error when missing END",
		},
		{
			name:        "invalid when condition - missing operand",
			tokens:      []string{"CASE", "WHEN", ">", "0", "THEN", "1", "END"},
			expectError: true,
			description: "should return error for invalid WHEN condition",
		},
		{
			name:        "invalid then result - missing operand",
			tokens:      []string{"CASE", "WHEN", "field1", ">", "0", "THEN", "+", "END"},
			expectError: true,
			description: "should return error for invalid THEN result",
		},
		{
			name:        "invalid else expression - missing operand",
			tokens:      []string{"CASE", "WHEN", "field1", ">", "0", "THEN", "1", "ELSE", "+", "END"},
			expectError: true,
			description: "should return error for invalid ELSE expression",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := parseCaseExpression(tt.tokens)
			if tt.expectError && err == nil {
				t.Errorf("expected error but got none: %s", tt.description)
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestEvaluateSimpleCaseExpression(t *testing.T) {
	// Create a simple CASE expression for testing
	caseExpr := &CaseExpression{
		Value: &ExprNode{
			Type:  TypeField,
			Value: "status",
		},
		WhenClauses: []WhenClause{
			{
				Condition: &ExprNode{
					Type:  TypeString,
					Value: "'active'",
				},
				Result: &ExprNode{
					Type:  TypeNumber,
					Value: "1",
				},
			},
			{
				Condition: &ExprNode{
					Type:  TypeString,
					Value: "'inactive'",
				},
				Result: &ExprNode{
					Type:  TypeNumber,
					Value: "0",
				},
			},
		},
		ElseResult: &ExprNode{
			Type:  TypeNumber,
			Value: "-1",
		},
	}

	node := &ExprNode{
		Type:     TypeCase,
		CaseExpr: caseExpr,
	}

	tests := []struct {
		name     string
		data     map[string]interface{}
		expected float64
	}{
		{
			name:     "match first when",
			data:     map[string]interface{}{"status": "active"},
			expected: 1.0,
		},
		{
			name:     "match second when",
			data:     map[string]interface{}{"status": "inactive"},
			expected: 0.0,
		},
		{
			name:     "no match use else",
			data:     map[string]interface{}{"status": "unknown"},
			expected: -1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluateSimpleCaseExpression(node, tt.data)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("expected %f, got %f", tt.expected, result)
			}
		})
	}

	// Test error cases
	t.Run("nil case expression", func(t *testing.T) {
		_, err := evaluateSimpleCaseExpression(&ExprNode{Type: TypeCase}, map[string]interface{}{})
		if err == nil {
			t.Error("expected error for nil case expression")
		}
	})

	t.Run("invalid case value", func(t *testing.T) {
		caseExprWithError := &CaseExpression{
			Value: &ExprNode{
				Type:  TypeField,
				Value: "nonexistent",
			},
			WhenClauses: []WhenClause{
				{
					Condition: &ExprNode{
						Type:  TypeString,
						Value: "'active'",
					},
					Result: &ExprNode{
						Type:  TypeNumber,
						Value: "1",
					},
				},
			},
		}

		nodeWithError := &ExprNode{
			Type:     TypeCase,
			CaseExpr: caseExprWithError,
		}

		_, err := evaluateSimpleCaseExpression(nodeWithError, map[string]interface{}{})
		if err == nil {
			t.Error("expected error for invalid case value")
		}
	})
}
