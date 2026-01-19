package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestTokenizeWithArrayAccess 测试包含数组访问的分词
func TestTokenizeWithArrayAccess(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		expected []string
		wantErr  bool
	}{
		{"数组访问", "sensors[0].temperature", []string{"sensors[0].temperature"}, false},
		{"字符串键访问", "config['key']", []string{"config['key']"}, false},
		{"混合访问", "data[0].items['key'].value", []string{"data[0].items['key'].value"}, false},
		{"表达式中的数组访问", "a[0] + b[1]", []string{"a[0]", "+", "b[1]"}, false},
		{"函数中的数组访问", "AVG(sensors[0].temperature)", []string{"AVG", "(", "sensors[0].temperature", ")"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tokenize(tt.expr)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}
