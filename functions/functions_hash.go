package functions

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
)

// Md5Function 计算MD5哈希值
type Md5Function struct {
	*BaseFunction
}

func NewMd5Function() *Md5Function {
	return &Md5Function{
		BaseFunction: NewBaseFunction("md5", TypeString, "哈希函数", "计算MD5哈希值", 1, 1),
	}
}

func (f *Md5Function) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *Md5Function) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("md5 requires string input")
	}
	
	hash := md5.Sum([]byte(str))
	return fmt.Sprintf("%x", hash), nil
}

// Sha1Function 计算SHA1哈希值
type Sha1Function struct {
	*BaseFunction
}

func NewSha1Function() *Sha1Function {
	return &Sha1Function{
		BaseFunction: NewBaseFunction("sha1", TypeString, "哈希函数", "计算SHA1哈希值", 1, 1),
	}
}

func (f *Sha1Function) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *Sha1Function) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("sha1 requires string input")
	}
	
	hash := sha1.Sum([]byte(str))
	return fmt.Sprintf("%x", hash), nil
}

// Sha256Function 计算SHA256哈希值
type Sha256Function struct {
	*BaseFunction
}

func NewSha256Function() *Sha256Function {
	return &Sha256Function{
		BaseFunction: NewBaseFunction("sha256", TypeString, "哈希函数", "计算SHA256哈希值", 1, 1),
	}
}

func (f *Sha256Function) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *Sha256Function) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("sha256 requires string input")
	}
	
	hash := sha256.Sum256([]byte(str))
	return fmt.Sprintf("%x", hash), nil
}

// Sha512Function 计算SHA512哈希值
type Sha512Function struct {
	*BaseFunction
}

func NewSha512Function() *Sha512Function {
	return &Sha512Function{
		BaseFunction: NewBaseFunction("sha512", TypeString, "哈希函数", "计算SHA512哈希值", 1, 1),
	}
}

func (f *Sha512Function) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *Sha512Function) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("sha512 requires string input")
	}
	
	hash := sha512.Sum512([]byte(str))
	return fmt.Sprintf("%x", hash), nil
}