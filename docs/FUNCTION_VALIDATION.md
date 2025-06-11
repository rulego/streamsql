# 函数验证功能

## 概述

StreamSQL 现在支持在解析阶段对函数进行验证，能够检测并报告未知函数的使用，提供更好的错误处理和用户体验。

## 功能特性

### 1. 函数存在性检查

在 SQL 解析过程中，系统会自动验证以下位置的函数调用：
- SELECT 子句中的函数
- WHERE 子句中的函数
- HAVING 子句中的函数

### 2. 支持的函数类型

验证器能够识别以下类型的函数：
- **内置数学函数**: `abs`, `sqrt`, `sin`, `cos`, `tan`, `floor`, `ceil`, `round`, `log`, `log10`, `exp`, `pow`, `mod`
- **注册的自定义函数**: 通过 `functions.Register()` 注册的函数
- **expr-lang 函数**: 通过 expr-lang 桥接的函数

### 3. 错误类型

新增了专门的错误类型 `ErrorTypeUnknownFunction` 来标识未知函数错误。

### 4. 智能建议

当检测到未知函数时，系统会提供有用的建议：
- 常见拼写错误的纠正建议
- 函数注册和使用的通用指导

## 使用示例

### 正确的函数使用

```go
// 内置函数
ssql := streamsql.New()
err := ssql.Execute("SELECT abs(temperature) FROM stream")
// err == nil

// 嵌套函数
err = ssql.Execute("SELECT sqrt(abs(temperature)) FROM stream")
// err == nil
```

### 未知函数错误

```go
ssql := streamsql.New()
err := ssql.Execute("SELECT unknown_func(temperature) FROM stream")
// err != nil
// err.Error() 包含 "Unknown function 'unknown_func'"
```

### 自定义函数注册

```go
// 注册自定义函数
functions.Register("custom_func", func(args ...interface{}) (interface{}, error) {
    // 函数实现
    return args[0], nil
})

// 现在可以使用自定义函数
ssql := streamsql.New()
err := ssql.Execute("SELECT custom_func(temperature) FROM stream")
// err == nil
```

## 错误处理

### 错误信息格式

未知函数错误包含以下信息：
- 错误类型: `ErrorTypeUnknownFunction`
- 错误消息: 包含具体的未知函数名
- 位置信息: 函数在 SQL 中的位置
- 建议: 可能的解决方案

### 错误恢复

函数验证错误是可恢复的，解析器会继续处理其他部分的 SQL，收集所有可能的错误。

## 实现细节

### 核心组件

1. **FunctionValidator**: 主要的函数验证器
   - `ValidateExpression()`: 验证表达式中的函数
   - `extractFunctionCalls()`: 提取函数调用
   - `isBuiltinFunction()`: 检查内置函数
   - `isKeyword()`: 过滤 SQL 关键字

2. **错误类型扩展**:
   - `ErrorTypeUnknownFunction`: 新的错误类型
   - `CreateUnknownFunctionError()`: 创建未知函数错误
   - `generateFunctionSuggestions()`: 生成建议

3. **解析器集成**:
   - 在 `parseSelect()` 中验证 SELECT 字段
   - 在 `parseWhere()` 中验证 WHERE 条件
   - 在 `parseHaving()` 中验证 HAVING 条件

### 正则表达式模式

函数调用检测使用正则表达式 `([a-zA-Z_][a-zA-Z0-9_]*)\s*\(` 来匹配：
- 以字母或下划线开头的标识符
- 后跟可选的空白字符
- 然后是左括号

### 关键字过滤

验证器会过滤掉 SQL 关键字，避免将 `CASE(...)` 或 `WHEN(...)` 误识别为函数调用。

## 配置选项

目前函数验证是默认启用的，无需额外配置。未来可能会添加以下配置选项：
- 禁用函数验证
- 自定义验证规则
- 扩展内置函数列表

## 性能考虑

- 函数验证在解析阶段进行，不影响运行时性能
- 正则表达式匹配针对表达式长度进行了优化
- 错误收集使用高效的数据结构

## 测试覆盖

功能包含完整的测试覆盖：
- 单元测试: `function_validator_test.go`
- 集成测试: `streamsql_validation_test.go`
- 错误处理测试: `error_test.go` 中的相关用例
