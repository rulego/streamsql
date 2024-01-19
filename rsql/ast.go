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

// ast.go 文件定义了抽象语法树（AST）的结构和方法

package rsql

import "bytes"

// Node 是 AST 的基础接口，所有的 AST 节点都实现了这个接口
type Node interface {
	// Format 用于将节点格式化为字符串
	Format(buf *bytes.Buffer)
}

// Statement 是表示 SQL 语句的接口，它继承了 Node 接口
type Statement interface {
	Node
	// Statement 接口没有额外的方法，只是为了区分不同类型的节点
}

// SelectStatement 是表示 SELECT 语句的接口，它继承了 Statement 接口
type SelectStatement interface {
	Statement
	// SelectStatement 接口没有额外的方法，只是为了区分不同类型的语句
}

// DDLStatement 是表示数据定义语言（DDL）语句的接口，它继承了 Statement 接口
type DDLStatement interface {
	Statement
	// DDLStatement 接口没有额外的方法，只是为了区分不同类型的语句
}

// Expression 是表示 SQL 表达式的接口，它继承了 Node 接口
type Expression interface {
	Node
	// Expression 接口没有额外的方法，只是为了区分不同类型的节点
}

// Literal 是表示 SQL 字面量的接口，它继承了 Expression 接口
type Literal interface {
	Expression
	// Literal 接口没有额外的方法，只是为了区分不同类型的表达式
}

//// Identifier 是表示 SQL 标识符的接口，它继承了 Expression 接口
//type Identifier interface {
//	Expression
//	// Identifier 接口没有额外的方法，只是为了区分不同类型的表达式
//}

// Select 是表示 SELECT 语句的结构体，它实现了 SelectStatement 接口
type Select struct {
	// Distinct 表示是否有 DISTINCT 关键字
	Distinct bool
	// SelectExprs 表示选择的列或表达式，它是一个 Expression 的切片
	SelectExprs []Field
	// From 表示 FROM 子句，它是一个 Expression 的切片
	From []Expression
	// Where 表示 WHERE 子句，它是一个 Expression 的切片
	Where Expression
	// GroupBy 表示 GROUP BY 子句，它是一个 Expression 的切片
	GroupBy []Expression
	// Having 表示 HAVING 子句，它是一个 Expression
	Having Expression
	// OrderBy 表示 ORDER BY 子句，它是一个 OrderByItem 的切片
	OrderBy []OrderByItem
	// Limit 表示 LIMIT 子句，它是一个 Limit 结构体
	Limit *Limit
	// Offset 表示 OFFSET 子句，它是一个 Expression
	Offset Expression
}

// Format 实现了 Node 接口的 Format 方法，用于将 Select 结构体格式化为字符串
func (s *Select) Format(buf *bytes.Buffer) {
	// 先输出 SELECT 关键字
	buf.WriteString("SELECT ")
	// 如果有 DISTINCT 关键字，就输出 DISTINCT
	if s.Distinct {
		buf.WriteString("DISTINCT ")
	}
	// 遍历选择的列或表达式，用逗号分隔，调用各自的 Format 方法
	for i, expr := range s.SelectExprs {
		if i > 0 {
			buf.WriteString(", ")
		}
		expr.Format(buf)
	}
	// 如果有 FROM 子句，就输出 FROM 关键字
	if len(s.From) > 0 {
		buf.WriteString(" FROM ")
		// 遍历来源的表或子查询，用逗号分隔，调用各自的 Format 方法
		for i, expr := range s.From {
			if i > 0 {
				buf.WriteString(", ")
			}
			expr.Format(buf)
		}
	}
	// 如果有 WHERE 子句，就输出 WHERE 关键字，调用条件表达式的 Format 方法
	if len(s.From) > 0 {
		buf.WriteString(" WHERE ")
		s.Where.Format(buf)
	}
	// 如果有 GROUP BY 子句，就输出 GROUP BY 关键字
	if len(s.GroupBy) > 0 {
		buf.WriteString(" GROUP BY ")
		// 遍历分组的列或表达式，用逗号分隔，调用各自的 Format 方法
		for i, expr := range s.GroupBy {
			if i > 0 {
				buf.WriteString(", ")
			}
			expr.Format(buf)
		}
	}
	// 如果有 HAVING 子句，就输出 HAVING 关键字，调用条件表达式的 Format 方法
	if s.Having != nil {
		buf.WriteString(" HAVING ")
		s.Having.Format(buf)
	}
	// 如果有 ORDER BY 子句，就输出 ORDER BY 关键字
	if len(s.OrderBy) > 0 {
		buf.WriteString(" ORDER BY ")
		// 遍历排序的项，用逗号分隔，调用各自的 Format 方法
		for i, item := range s.OrderBy {
			if i > 0 {
				buf.WriteString(", ")
			}
			item.Format(buf)
		}
	}
	// 如果有 LIMIT 子句，就输出 LIMIT 关键字，调用 Limit 结构体的 Format 方法
	if s.Limit != nil {
		buf.WriteString(" LIMIT ")
		s.Limit.Format(buf)
	}
	// 如果有 OFFSET 子句，就输出 OFFSET 关键字，调用偏移量表达式的 Format 方法
	if s.Offset != nil {
		buf.WriteString(" OFFSET ")
		s.Offset.Format(buf)
	}
}

type Field struct {
	Expr  Expression
	Alias string
}

func (f *Field) Format(buf *bytes.Buffer) {
	f.Expr.Format(buf)
	if f.Alias != "" {
		buf.WriteString(" AS " + f.Alias)
	}
}

type ExpressionLang struct {
	Val  string
	Type string
}

func (f *ExpressionLang) Format(buf *bytes.Buffer) {
	if f.Type == "" {
		buf.WriteString("expr")
	} else {
		buf.WriteString(f.Type)
	}
	buf.WriteString(" ")
	buf.WriteString(f.Val)
}

// OrderByItem 是表示 ORDER BY 子句中的一个排序项的结构体
type OrderByItem struct {
	// Expr 表示排序的列或表达式，它是一个 Expression
	Expr Expression
	// Desc 表示是否降序排序
	Desc bool
}

// Format 实现了 Node 接口的 Format 方法，用于将 OrderByItem 结构体格式化为字符串
func (o *OrderByItem) Format(buf *bytes.Buffer) {
	// 先输出排序的列或表达式，调用其 Format 方法
	o.Expr.Format(buf)
	// 如果是降序排序，就输出 DESC 关键字
	if o.Desc {
		buf.WriteString(" DESC")
	}
}

// Limit 是表示 LIMIT 子句的结构体
type Limit struct {
	// RowCount 表示限制的行数，它是一个 Expression
	RowCount Expression
}

// Format 实现了 Node 接口的 Format 方法，用于将 Limit 结构体格式化为字符串
func (l *Limit) Format(buf *bytes.Buffer) {
	// 直接输出限制的行数，调用其 Format 方法
	l.RowCount.Format(buf)
}

// Create 是表示 CREATE 语句的结构体，它实现了 DDLStatement 接口
type Create struct {
	// Table 表示要创建的表的名称，它是一个 Identifier
	Table Identifier
	// Columns 表示要创建的表的列的定义，它是一个 ColumnDefinition 的切片
	Columns []ColumnDefinition
}

// Format 实现了 Node 接口的 Format 方法，用于将 Create 结构体格式化为字符串
func (c *Create) Format(buf *bytes.Buffer) {
	// 先输出 CREATE TABLE 关键字
	buf.WriteString("CREATE TABLE ")
	// 然后输出要创建的表的名称，调用其 Format 方法
	c.Table.Format(buf)
	// 然后输出左大括号
	buf.WriteString(" {")
	// 遍历要创建的表的列的定义，用逗号分隔，调用各自的 Format 方法
	for i, col := range c.Columns {
		if i > 0 {
			buf.WriteString(", ")
		}
		col.Format(buf)
	}
	// 最后输出右大括号
	buf.WriteString("}")
}

// ColumnDefinition 是表示表的列的定义的结构体
type ColumnDefinition struct {
	// Name 表示列的名称，它是一个 Identifier
	Name Identifier
	// Type 表示列的数据类型，它是一个 DataType
	Type DataType
	// Constraints 表示列的约束条件，它是一个 Constraint 的切片
	Constraints []Constraint
}

// Format 实现了 Node 接口的 Format 方法，用于将 ColumnDefinition 结构体格式化为字符串
func (c *ColumnDefinition) Format(buf *bytes.Buffer) {
	// 先输出列的名称，调用其 Format 方法
	c.Name.Format(buf)
	// 然后输出空格
	buf.WriteString(" ")
	// 然后输出列的数据类型，调用其 Format 方法
	c.Type.Format(buf)
	// 如果有约束条件，就输出空格
	if len(c.Constraints) > 0 {
		buf.WriteString(" ")
	}
	// 遍历列的约束条件，用空格分隔，调用各自的 Format 方法
	for i, cons := range c.Constraints {
		if i > 0 {
			buf.WriteString(" ")
		}
		cons.Format(buf)
	}
}

// DataType 是表示数据类型的接口，它继承了 Node 接口
type DataType interface {
	Node
	// DataType 接口没有额外的方法，只是为了区分不同类型的节点
}

// Integer 是表示整数类型的结构体，它实现了 DataType 接口
type Integer struct {
	val int64
	// Unsigned 表示是否是无符号整数
	Unsigned bool
}

// Format 实现了 Node 接口的 Format 方法，用于将 Integer 结构体格式化为字符串
func (i *Integer) Format(buf *bytes.Buffer) {
	// 先输出 INT 关键字
	buf.WriteString("INT")
	// 如果是无符号整数，就输出 UNSIGNED 关键字
	if i.Unsigned {
		buf.WriteString(" UNSIGNED")
	}
}

// Constraint 是表示约束条件的接口，它继承了 Node 接口
type Constraint interface {
	Node
	// Constraint 接口没有额外的方法，只是为了区分不同类型的节点
}

// NotNull 是表示非空约束的结构体，它实现了 Constraint 接口
type NotNull struct {
	// NotNull 结构体没有额外的字段，只是为了区分不同类型的约束
}

// Format 实现了 Node 接口的 Format 方法，用于将 NotNull 结构体格式化为字符串
func (n *NotNull) Format(buf *bytes.Buffer) {
	// 直接输出 NOT NULL 关键字
	buf.WriteString("NOT NULL")
}

// StringLiteral 是表示字符串字面量的结构体，它实现了 Literal 接口
type StringLiteral struct {
	// Val 表示字符串的值，它是一个 string
	Val string
}

// Format 实现了 Node 接口的 Format 方法，用于将 StringLiteral 结构体格式化为字符串
func (s *StringLiteral) Format(buf *bytes.Buffer) {
	// 先输出一个单引号
	buf.WriteString("'")
	// 然后输出字符串的值，对于特殊字符，需要进行转义
	for _, r := range s.Val {
		switch r {
		case '\n':
			buf.WriteString("\\n")
		case '\r':
			buf.WriteString("\\r")
		case '\t':
			buf.WriteString("\\t")
		case '\'':
			buf.WriteString("\\'")
		case '\\':
			buf.WriteString("\\\\")
		default:
			buf.WriteRune(r)
		}
	}
	// 最后输出一个单引号
	buf.WriteString("'")
}

// Identifier 是表示字符串字面量的结构体，它实现了 Literal 接口
type Identifier struct {
	// Val 表示字符串的值，它是一个 string
	Val string
}

// Format 实现了 Node 接口的 Format 方法，用于将 Identifier 结构体格式化为字符串
func (s *Identifier) Format(buf *bytes.Buffer) {

}

// ComparisonExpr 是表示比较表达式的结构体，它实现了 Expression 接口
type ComparisonExpr struct {
	// Left 是比较表达式的左操作数，它是一个 Expression 类型
	Left Expression
	// Op 是比较表达式的运算符，它是一个 Token 类型
	Op Token
	// Right 是比较表达式的右操作数，它也是一个 Expression 类型
	Right Expression
}

// Format 实现了 Node 接口的 Format 方法，用于将 ComparisonExpr 结构体格式化为字符串
func (c *ComparisonExpr) Format(buf *bytes.Buffer) {
	// 先输出左操作数
	c.Left.Format(buf)
	// 再输出一个空格
	buf.WriteString(" ")
	// 再输出运算符
	buf.WriteString(c.Op.Literal)
	// 再输出一个空格
	buf.WriteString(" ")
	// 最后输出右操作数
	c.Right.Format(buf)
}

// ParenExpr 是一个表示括号表达式的结构体
type ParenExpr struct {
	// Lparen 表示左括号，它是一个 Token 类型
	Lparen Token
	// Rparen 表示右括号，它是一个 Token 类型
	Rparen Token
	// Expr 表示括号内的表达式，它是一个 Expression 类型
	Expr Expression
}

// Format 实现了接口中的 Format 方法，用于将 ParenExpr 结构体格式化为字符串
func (p *ParenExpr) Format(buf *bytes.Buffer) {
	// 使用 buf.WriteString 方法向缓冲区写入左括号的字面值
	buf.WriteString(p.Lparen.Literal)
	// 调用括号内的表达式的 Format 方法，将其内容写入缓冲区
	p.Expr.Format(buf)
	// 使用 buf.WriteString 方法向缓冲区写入右括号的字面值
	buf.WriteString(p.Rparen.Literal)
}

// FunctionCall 函数
type FunctionCall struct {
	//是否是聚合函数
	IsAggregate bool
	//是否是窗口函数
	IsWindow bool
	Name     string
	Args     []Expression
}

// Format 实现了接口中的 Format 方法，用于将 ParenExpr 结构体格式化为字符串
func (p *FunctionCall) Format(buf *bytes.Buffer) {
	buf.WriteString(p.Name)
	buf.WriteString("(")
}
