package functions

import (
	"testing"
)

func TestCompressionFunctions(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		wantErr  bool
	}{
		// Compress function tests
		{
			name:     "compress_gzip_valid",
			funcName: "compress",
			args:     []interface{}{"hello world", "gzip"},
			wantErr:  false,
		},
		{
			name:     "compress_zlib_valid",
			funcName: "compress",
			args:     []interface{}{"hello world", "zlib"},
			wantErr:  false,
		},
		{
			name:     "compress_invalid_algorithm",
			funcName: "compress",
			args:     []interface{}{"hello world", "invalid"},
			wantErr:  true,
		},
		{
			name:     "compress_empty_string",
			funcName: "compress",
			args:     []interface{}{"", "gzip"},
			wantErr:  false,
		},
		{
			name:     "compress_wrong_arg_count",
			funcName: "compress",
			args:     []interface{}{"hello"},
			wantErr:  true,
		},
		// Decompress function tests
		{
			name:     "decompress_invalid_base64",
			funcName: "decompress",
			args:     []interface{}{"invalid_base64", "gzip"},
			wantErr:  true,
		},
		{
			name:     "decompress_invalid_algorithm",
			funcName: "decompress",
			args:     []interface{}{"SGVsbG8gV29ybGQ=", "invalid"},
			wantErr:  true,
		},
		{
			name:     "decompress_wrong_arg_count",
			funcName: "decompress",
			args:     []interface{}{"SGVsbG8gV29ybGQ="},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, exists := Get(tt.funcName)
			if !exists {
				t.Fatalf("%s function not found", tt.funcName)
			}
			
			// 执行函数
			_, err := fn.Execute(nil, tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestCompressionDecompressionRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		algorithm string
		input     string
	}{
		{
			name:      "gzip_round_trip",
			algorithm: "gzip",
			input:     "Hello, World! This is a test string for compression.",
		},
		{
			name:      "zlib_round_trip",
			algorithm: "zlib",
			input:     "Hello, World! This is a test string for compression.",
		},
		{
			name:      "gzip_empty_string",
			algorithm: "gzip",
			input:     "",
		},
		{
			name:      "zlib_unicode",
			algorithm: "zlib",
			input:     "你好世界！这是一个测试字符串。",
		},
	}

	compressFn, exists := Get("compress")
	if !exists {
		t.Fatal("compress function not found")
	}
	
	decompressFn, exists := Get("decompress")
	if !exists {
		t.Fatal("decompress function not found")
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 压缩
			compressed, err := compressFn.Execute(nil, []interface{}{tt.input, tt.algorithm})
			if err != nil {
				t.Fatalf("Compress failed: %v", err)
			}
			
			// 解压缩
			decompressed, err := decompressFn.Execute(nil, []interface{}{compressed, tt.algorithm})
			if err != nil {
				t.Fatalf("Decompress failed: %v", err)
			}
			
			// 验证结果
			if decompressed != tt.input {
				t.Errorf("Round trip failed: expected %q, got %q", tt.input, decompressed)
			}
		})
	}
}