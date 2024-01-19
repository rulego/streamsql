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

// parser.go 文件定义了语法分析的接口和方法
package rsql

import (
	"strconv"
	"strings"
)

// Parser 是语法分析器的接口，它定义了从 Lexer 中获取 Token，并构建 AST 的方法
type Parser interface {
	// ParseStatement 用于解析一个 SQL 语句，返回一个 Statement 的实例
	ParseStatement() Statement
	// ParseExpression 用于解析一个 SQL 表达式，返回一个 Expression 的实例
	ParseExpression() Expression
}

// NewParser 是一个工厂函数，用于根据一个 Lexer 的实例创建一个 Parser 的实例
func NewParser(l Lexer) Parser {
	// 创建一个 parser 的实例
	p := &parser{l: l}
	// 读取两个 Token，分别存入 curToken 和 peekToken
	p.nextToken()
	p.nextToken()
	// 返回这个实例
	return p
}

// parser 是 Parser 接口的一个实现，它用一个 Lexer 的实例作为输入
type parser struct {
	// l 表示输入的 Lexer 的实例
	l Lexer
	// curToken 表示当前读取的 Token
	curToken Token
	// peekToken 表示下一个要读取的 Token
	peekToken Token
}

// ParseStatement 实现了 Parser 接口的 ParseStatement 方法，用于解析一个 SQL 语句，返回一个 Statement 的实例
func (p *parser) ParseStatement() Statement {
	// 省略具体的实现细节，只是简单地根据当前的 Token 类型调用相应的方法
	switch p.curToken.Type {
	case SELECT:
		return p.parseSelectStatement()
	case CREATE:
		return p.parseCreateStatement()
	default:
		return nil
	}
}

// ParseExpression 实现了 Parser 接口的 ParseExpression 方法，用于解析一个 SQL 表达式，返回一个 Expression 的实例
func (p *parser) ParseExpression() Expression {
	// 省略具体的实现细节，只是简单地根据当前的 Token 类型调用相应的方法
	switch p.curToken.Type {
	case STRING:
		return p.parseStringLiteral()
	case IDENT:
		return p.parseIdentifier()
	default:
		return nil
	}
}

// parseCreateStatement 用于解析一个 CREATE 语句，返回一个 Create 的实例
func (p *parser) parseCreateStatement() *Create {
	//todo 暂不支持
	return &Create{}
}

// parseSelectStatement 用于解析一个 SELECT 语句，返回一个 Select 的实例
func (p *parser) parseSelectStatement() *Select {
	// 创建一个 Select 的实例
	stmt := &Select{}
	// 如果当前的 Token 不是 SELECT，就返回 nil
	if p.curToken.Type != SELECT {
		return nil
	}
	// 读取下一个 Token
	p.nextToken()
	// 解析选择的列或表达式，调用 parseSelectExprs 方法，赋值给 stmt.SelectExprs
	stmt.SelectExprs = p.parseSelectExprs()
	// 读取下一个 Token
	p.nextToken()
	// 如果当前的 Token 是 FROM，就读取下一个 Token
	if p.curToken.Type == FROM {
		p.nextToken()
		// 解析来源的表或子查询，调用 parseFrom 方法，赋值给 stmt.From
		stmt.From = p.parseFrom()
		// 读取下一个 Token
		p.nextToken()
	}
	// 如果当前的 Token 是 WHERE，就读取下一个 Token
	if p.curToken.Type == WHERE {
		p.nextToken()
		// 解析条件表达式，调用 parseExpression 方法，赋值给 stmt.Where
		stmt.Where = p.parseWhere()
		// 读取下一个 Token
		p.nextToken()
	}
	// 如果当前的 Token 是 GROUP BY，就读取下一个 Token
	if p.curToken.Type == GROUP {
		p.nextToken()
		// 如果下一个 Token 不是 BY，就返回 nil
		if p.curToken.Type != BY {
			return nil
		}
		// 读取下一个 Token
		p.nextToken()
		// 解析分组的列或表达式，调用 parseGroupBy 方法，赋值给 stmt.GroupBy
		stmt.GroupBy = p.parseGroupBy()
		// 读取下一个 Token
		p.nextToken()
	}
	// 如果当前的 Token 是 HAVING，就读取下一个 Token
	if p.curToken.Type == HAVING {
		p.nextToken()
		// 解析条件表达式，调用 parseExpression 方法，赋值给 stmt.Having
		stmt.Having = p.parseExpression()
		// 读取下一个 Token
		p.nextToken()
	}
	// 如果当前的 Token 是 ORDER BY，就读取下一个 Token
	if p.curToken.Type == ORDER {
		p.nextToken()
		// 如果下一个 Token 不是 BY，就返回 nil
		if p.peekToken.Type != BY {
			return nil
		}
		// 读取下一个 Token
		p.nextToken()
		// 解析排序的项，调用 parseOrderBy 方法，赋值给 stmt.OrderBy
		stmt.OrderBy = p.parseOrderBy()
		// 读取下一个 Token
		p.nextToken()
	}
	// 如果当前的 Token 是 LIMIT，就读取下一个 Token
	if p.curToken.Type == LIMIT {
		p.nextToken()
		// 解析限制的行数，调用 parseLimit 方法，赋值给 stmt.Limit
		stmt.Limit = p.parseLimit()
		// 读取下一个 Token
		p.nextToken()
	}
	// 如果当前的 Token 是 OFFSET，就读取下一个 Token
	if p.curToken.Type == OFFSET {
		p.nextToken()
		// 解析偏移量，调用 parseExpression 方法，赋值给 stmt.Offset
		stmt.Offset = p.parseExpression()
		// 读取下一个 Token
		p.nextToken()
	}
	// 返回 stmt
	return stmt
}

// parseSelectExprs 用于解析选择的列或表达式，返回一个 Expression 的切片
func (p *parser) parseSelectExprs() []Field {
	// 创建一个空的 Expression 切片
	var fields []Field
	// for 循环，直到遇到非列或表达式的 Token
	for {
		// 解析一个列或表达式，调用 parseExpression 方法，追加到 exprs 中
		expr := p.parseExpression()
		field := Field{
			Expr: expr,
		}
		if p.peekToken.Type == AS {
			p.nextToken()
			p.nextToken()
			field.Alias = p.curToken.Literal
		}
		fields = append(fields, field)
		// 如果下一个 Token 不是逗号，就跳出循环
		if p.peekToken.Type != COMMA {
			break
		}
		// 读取下一个 Token
		p.nextToken()
		// 读取下一个 Token
		p.nextToken()
	}
	// 返回 exprs
	return fields
}

// parseFrom 用于解析来源的表或子查询，返回一个 Expression 的切片
func (p *parser) parseFrom() []Expression {
	// 创建一个空的 Expression 切片
	exprs := []Expression{}
	// for 循环，直到遇到非表或子查询的 Token
	for {
		// 解析一个表或子查询，调用 parseExpression 方法，追加到 exprs 中
		expr := p.parseExpression()
		exprs = append(exprs, expr)
		// 如果下一个 Token 不是逗号，就跳出循环
		if p.peekToken.Type != COMMA {
			break
		}
		// 读取下一个 Token
		p.nextToken()
	}
	// 返回 exprs
	return exprs
}

func (p *parser) parseWhere() *ExpressionLang {
	var builder strings.Builder
	// for 循环，直到遇到非条件表达式的 Token
	for {
		switch p.curToken.Type {
		case ASSIGN:
			builder.WriteString("==")
		case OR, AND, NOT:
			builder.WriteString(" ")
			builder.WriteString(p.curToken.Literal)
			builder.WriteString(" ")
		default:
			builder.WriteString(p.curToken.Literal)
			//builder.WriteString(" ")
		}

		if p.peekToken.Type == GROUP || p.peekToken.Type == LIMIT || p.peekToken.Type == HAVING {
			break
		}
		// 读取下一个 Token
		p.nextToken()
	}
	return &ExpressionLang{
		Val: builder.String(),
	}
}

// parseComparisonExpr 用于解析一个比较表达式，返回一个 ComparisonExpr 的实例
func (p *parser) parseComparisonExpr() Expression {
	// 获取左操作数
	left := p.parseExpression()
	// 获取比较运算符
	op := p.curToken
	//// 检查是否是合法的比较运算符
	//if !op.isComparisonOperator() {
	//	p.errorf("invalid comparison operator: %s", op.Literal)
	//}
	// 移动到下一个 Token
	p.nextToken()
	// 获取右操作数
	right := p.parseExpression()
	// 返回一个 ComparisonExpr
	return &ComparisonExpr{
		Left:  left,
		Op:    op,
		Right: right,
	}
}

// parseOrderBy 用于解析排序的项，返回一个 OrderByItem 的切片
func (p *parser) parseOrderBy() []OrderByItem {
	// 创建一个空的 OrderByItem 切片
	var items []OrderByItem
	// for 循环，直到遇到非排序项的 Token
	for {
		// 创建一个 OrderByItem 的实例
		item := OrderByItem{}
		// 解析排序的列或表达式，调用 parseExpression 方法，赋值给 item.Expr
		item.Expr = p.parseExpression()
		// 如果下一个 Token 是 ASC 或 DESC，就读取下一个 Token
		if p.peekToken.Type == ASC || p.peekToken.Type == DESC {
			p.nextToken()
			// 如果是 DESC，就设置 item.Desc 为 true
			if p.curToken.Type == DESC {
				item.Desc = true
			}
		}
		// 追加 item 到 items 中
		items = append(items, item)
		// 如果下一个 Token 不是逗号，就跳出循环
		if p.peekToken.Type != COMMA {
			break
		}
		// 读取下一个 Token
		p.nextToken()
	}
	// 返回 items
	return items
}

// parseLimit 用于解析限制的行数，返回一个 Limit 的实例
func (p *parser) parseLimit() *Limit {
	// 创建一个 Limit 的实例
	limit := &Limit{}
	// 解析限制的行数，调用 parseExpression 方法，赋值给 limit.RowCount
	limit.RowCount = p.parseExpression()
	// 返回 limit
	return limit
}

// parseStringLiteral 用于解析一个字符串字面量，返回一个 StringLiteral 的实例
func (p *parser) parseStringLiteral() *StringLiteral {
	// 创建一个 StringLiteral 的实例
	lit := &StringLiteral{}
	// 如果当前的 Token 不是 STRING，就返回 nil
	if p.curToken.Type != STRING {
		return nil
	}
	// 将当前的 Token 的字面值赋给 lit.Val
	lit.Val = p.curToken.Literal
	// 返回 lit
	return lit
}

// parseIdentifier 用于解析一个标识符，返回一个 Identifier 的实例
func (p *parser) parseIdentifier() *Identifier {
	// 创建一个 Identifier 的实例
	ident := &Identifier{}
	// 如果当前的 Token 不是 IDENT，就返回 nil
	if p.curToken.Type != IDENT {
		return nil
	}
	// 将当前的 Token 的字面值赋给 ident.Val
	ident.Val = p.curToken.Literal
	// 返回 ident
	return ident
}

func (p *parser) parseNumber() *Integer {
	// 创建一个 Identifier 的实例
	ident := &Integer{}
	// 如果当前的 Token 不是 IDENT，就返回 nil
	if p.curToken.Type != NUMBER {
		return nil
	}
	i, _ := strconv.ParseInt(p.curToken.Literal, 10, 64)
	ident.val = i
	// 返回 ident
	return ident
}

// nextToken 用于从 Lexer 中读取下一个 Token，并更新 curToken 和 peekToken
func (p *parser) nextToken() {
	// 将 peekToken 赋值给 curToken
	p.curToken = p.peekToken
	// 从 Lexer 中读取下一个 Token，赋值给 peekToken
	p.peekToken = p.l.NextToken()
}

// parseExpression 用于解析一个 SQL 表达式，返回一个 Expression 的实例
func (p *parser) parseExpression() Expression {
	// 省略具体的实现细节，只是简单地根据当前的 Token 类型调用相应的方法
	switch p.curToken.Type {
	case STRING:
		return p.parseStringLiteral()
	case IDENT:
		if p.peekToken.Type == LPAREN {
			return p.parseFunctionCall()
		} else {
			return p.parseIdentifier()
		}
	case LPAREN:
		return p.parseExpressionLang()
	case NUMBER:
		return p.parseNumber()
	default:
		switch p.peekToken.Type {
		case ASSIGN, PLUS, MINUS, ASTERISK, SLASH:
			return p.parseExpressionLang()
		}
		// 如果遇到无法解析的 Token 类型，就返回 nil
		return nil
	}
}

// parseFunctionCall 用于解析一个函数调用，返回一个 FunctionCall 的实例
func (p *parser) parseFunctionCall() Expression {
	// 获取函数名，它是前一个 Token 的字面值
	name := p.curToken.Literal
	// 创建一个空的 Expression 切片，用于存放参数
	var args []Expression
	// 移动到下一个 Token
	p.nextToken()
	// for 循环，直到遇到右括号或结束符
	for p.curToken.Type != RPAREN && p.curToken.Type != EOF {
		arg := p.parseExpressionLang()
		//// 解析一个参数，调用 parseExpression 方法，追加到 args 中
		//arg := p.parseExpression()
		args = append(args, arg)
		// 如果下一个 Token 是逗号，就跳过它
		if p.peekToken.Type == COMMA {
			p.nextToken()
		}
		if p.curToken.Type == RPAREN {
			break
		}
		// 移动到下一个 Token
		p.nextToken()
	}
	// 返回一个 FunctionCall
	return &FunctionCall{
		IsAggregate: LookupIsAggregateFunc(name),
		IsWindow:    LookupIsWindowFunc(name) != NOT_WINDOW,
		Name:        name,
		Args:        args,
	}
}

func (p *parser) parseExpressionLang() *ExpressionLang {
	var parenNum int
	var builder strings.Builder
	for p.curToken.Type != EOF {
		//if index > 0 {
		//	builder.WriteString(" ")
		//}
		switch p.curToken.Type {
		case LPAREN:
			parenNum += 1
		case RPAREN:
			parenNum -= 1
		case ASSIGN:
			builder.WriteString("==")
		default:
			builder.WriteString(p.curToken.Literal)
		}
		if parenNum <= 0 || p.peekToken.Type == COMMA || p.peekToken.Type == FROM || p.peekToken.Type == GROUP || p.peekToken.Type == LIMIT || p.peekToken.Type == HAVING {
			break
		}
		// 读取下一个 Token
		p.nextToken()
	}
	return &ExpressionLang{
		Val: builder.String(),
	}
}

// parseParenExpr 用于解析一个括号表达式，返回一个 ParenExpr 的实例
func (p *parser) parseParenExpr() Expression {
	// 创建一个 ast.ParenExpr 的实例
	expr := &ParenExpr{}
	// 保存当前的 Token 作为左括号
	expr.Lparen = p.curToken
	// 读取下一个 Token
	p.nextToken()
	// 解析括号内的表达式，调用 parseExpression 方法，赋值给 expr.X
	expr.Expr = p.parseExpression()
	// 读取下一个 Token
	p.nextToken()
	// 如果当前的 Token 不是右括号，就返回 nil
	if p.curToken.Type != RPAREN {
		return nil
	}
	// 保存当前的 Token 作为右括号
	expr.Rparen = p.curToken
	// 返回 expr
	return expr
}

// parseGroupBy 用于解析分组的列或表达式，返回一个 Expression 的切片
func (p *parser) parseGroupBy() []Expression {
	// 创建一个空的 Expression 切片
	exprs := []Expression{}
	// for 循环，直到遇到非列或表达式的 Token
	for {
		// 解析一个列或表达式，调用 parseExpression 方法，追加到 exprs 中
		expr := p.parseExpression()
		exprs = append(exprs, expr)
		// 如果下一个 Token 不是逗号，就跳出循环
		if p.peekToken.Type != COMMA {
			break
		}
		// 读取下一个 Token
		p.nextToken()
		p.nextToken()
	}
	// 返回 exprs
	return exprs
}
