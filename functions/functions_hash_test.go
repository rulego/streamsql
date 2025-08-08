package functions

import (
	"testing"
)

// TestHashFunctions 测试哈希函数的基本功能
func TestHashFunctions(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		expected interface{}
	}{
		{
			name:     "md5 basic",
			funcName: "md5",
			args:     []interface{}{"hello"},
			expected: "5d41402abc4b2a76b9719d911017c592",
		},
		{
			name:     "sha1 basic",
			funcName: "sha1",
			args:     []interface{}{"hello"},
			expected: "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
		},
		{
			name:     "sha256 basic",
			funcName: "sha256",
			args:     []interface{}{"hello"},
			expected: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
		},
		{
			name:     "sha512 basic",
			funcName: "sha512",
			args:     []interface{}{"hello"},
			expected: "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043",
		},
		{
			name:     "md5 empty string",
			funcName: "md5",
			args:     []interface{}{""},
			expected: "d41d8cd98f00b204e9800998ecf8427e",
		},
		{
			name:     "sha1 empty string",
			funcName: "sha1",
			args:     []interface{}{""},
			expected: "da39a3ee5e6b4b0d3255bfef95601890afd80709",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, exists := Get(tt.funcName)
			if !exists {
				t.Fatalf("Function %s not found", tt.funcName)
			}

			result, err := fn.Execute(&FunctionContext{}, tt.args)
			if err != nil {
				t.Errorf("Execute() error = %v", err)
				return
			}

			if result != tt.expected {
				t.Errorf("Execute() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestHashFunctionValidation 测试哈希函数的参数验证
func TestHashFunctionValidation(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []interface{}
		wantErr  bool
	}{
		{
			name:     "md5 no args",
			function: NewMd5Function(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "md5 too many args",
			function: NewMd5Function(),
			args:     []interface{}{"hello", "world"},
			wantErr:  true,
		},
		{
			name:     "md5 valid args",
			function: NewMd5Function(),
			args:     []interface{}{"hello"},
			wantErr:  false,
		},
		{
			name:     "sha1 no args",
			function: NewSha1Function(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "sha256 valid args",
			function: NewSha256Function(),
			args:     []interface{}{"test"},
			wantErr:  false,
		},
		{
			name:     "sha512 valid args",
			function: NewSha512Function(),
			args:     []interface{}{"test"},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.function.Validate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestHashFunctionErrors 测试哈希函数的错误处理
func TestHashFunctionErrors(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []interface{}
		wantErr  bool
	}{
		{
			name:     "md5 non-string input",
			function: NewMd5Function(),
			args:     []interface{}{123},
			wantErr:  true,
		},
		{
			name:     "sha1 non-string input",
			function: NewSha1Function(),
			args:     []interface{}{123},
			wantErr:  true,
		},
		{
			name:     "sha256 non-string input",
			function: NewSha256Function(),
			args:     []interface{}{123},
			wantErr:  true,
		},
		{
			name:     "sha512 non-string input",
			function: NewSha512Function(),
			args:     []interface{}{123},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.function.Execute(&FunctionContext{}, tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
