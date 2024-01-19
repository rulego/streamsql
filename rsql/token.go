/*
 * Copyright 2024 The RuleGo Authors.
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

// token.go 定义了一些常量，表示不同的 Token 类型
package rsql

import "strings"

// Token 是一个结构体，用于表示一个词法单元，包括类型和字面值
type Token struct {
	// Type 表示 Token 的类型，如 SELECT, CREATE, STRING, IDENT 等
	Type TokenType
	// Literal 表示 Token 的字面值，如 "SELECT", "users", "'hello'" 等
	Literal string
}

// TokenType 是一个字符串，用于表示 Token 的类型
type TokenType string

// 定义了一些常用的 Token 类型
const (
	// 特殊类型
	ILLEGAL = "ILLEGAL" // 非法字符
	EOF     = "EOF"     // 文件结束

	// 标识符和字面量
	IDENT  = "IDENT"  // 标识符，如 user, name 等
	STRING = "STRING" // 字符串字面量，如 'hello' 等

	// 操作符和分隔符
	ASSIGN    = "="  // 赋值符号
	PLUS      = "+"  // 加号
	MINUS     = "-"  // 减号
	ASTERISK  = "*"  // 星号，表示乘法或通配符
	SLASH     = "/"  // 斜杠，表示除法或注释
	COMMA     = ","  // 逗号，分隔符
	SEMICOLON = ";"  // 分号，结束符
	LPAREN    = "("  // 左括号
	RPAREN    = ")"  // 右括号
	LBRACE    = "{"  // 左大括号
	RBRACE    = "}"  // 右大括号
	LT        = "<"  // 小于号
	LE        = "<=" // 小于等于号
	GT        = ">"  // 大于号
	GE        = ">=" // 大于等于号
	EQ        = "==" // 等于号
	NOT_EQ    = "!=" // 不等于号
	NOT       = "NOT"
	AND       = "AND"
	OR        = "OR"

	// 关键字
	SELECT  = "SELECT" // 查询语句
	CREATE  = "CREATE" // 创建语句
	TABLE   = "TABLE"  // 表
	FROM    = "FROM"   // 来源
	WHERE   = "WHERE"  // 条件
	GROUP   = "GROUP"
	BY      = "BY"
	ORDER   = "ORDER"
	LIMIT   = "LIMIT"
	HAVING  = "HAVING"
	ASC     = "ASC"
	DESC    = "DESC"
	OFFSET  = "OFFSET"
	AS      = "AS"
	COMMENT = "COMMENT" // 注释
	NUMBER  = "NUMBER"  // 数字
)

// keywords 是一个 map，用于存储所有的关键字及其对应的 Token 类型
var keywords = map[string]TokenType{
	"SELECT": SELECT,
	"CREATE": CREATE,
	"TABLE":  TABLE,
	"FROM":   FROM,
	"GROUP":  GROUP,
	"BY":     BY,
	"ORDER":  ORDER,
	"LIMIT":  LIMIT,
	"HAVING": HAVING,
	"ASC":    ASC,
	"DESC":   DESC,
	"OFFSET": OFFSET,
	"WHERE":  WHERE,
	"AS":     AS,
	"NOT":    NOT,
	"AND":    AND,
	"OR":     OR,
}

// LookupIdent 用于根据给定的标识符，返回其对应的 Token 类型
// 如果是一个关键字，就返回关键字的类型，否则就返回 IDENT 类型
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return IDENT
}

// 聚合函数
var aggregateFunctions = map[string]string{
	"COUNT":  "COUNT",
	"SUM":    "SUM",
	"MAX":    "MAX",
	"MIN":    "MIN",
	"AVG":    "AVG",
	"STD":    "STD",
	"VAR":    "VAR",
	"FIRST":  "FIRST",
	"LAST":   "LAST",
	"TOP":    "TOP",
	"BOTTOM": "BOTTOM",
}

// LookupIsAggregateFunc 用于判断是否是聚合函数
func LookupIsAggregateFunc(ident string) bool {
	if _, ok := aggregateFunctions[strings.ToUpper(ident)]; ok {
		return true
	}
	return false
}

type WindowType string

const (
	NOT_WINDOW      WindowType = "NotWindow"
	TUMBLING_WINDOW            = "TumblingWindow"
	SLIDING_WINDOW             = "SlidingWindow"
	SESSION_WINDOW             = "SessionWindow"
	COUNT_WINDOW               = "CountWindow"
)

// 窗口函数
var windowFunctions = map[string]WindowType{
	"TUMBLINGWINDOW": TUMBLING_WINDOW,
	"SLIDINGWINDOW":  SLIDING_WINDOW,
	"SESSIONWINDOW":  SESSION_WINDOW,
	"COUNTWINDOW":    COUNT_WINDOW,
}

// LookupIsWindowFunc 用于判断是否是窗口函数
func LookupIsWindowFunc(ident string) WindowType {
	if windowType, ok := windowFunctions[strings.ToUpper(ident)]; ok {
		return windowType
	}
	return NOT_WINDOW
}
