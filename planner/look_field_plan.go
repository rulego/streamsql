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

package planner

import (
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/rulego/streamsql/operator"
	"github.com/rulego/streamsql/rsql"
	"github.com/rulego/streamsql/types"
)

type LookFieldPlan struct {
	*BaseLogicalPlan
}

func (p *LookFieldPlan) New() types.LogicalPlan {
	return &LookFieldPlan{BaseLogicalPlan: &BaseLogicalPlan{}}
}

func (p *LookFieldPlan) Plan(statement rsql.Statement) error {
	if selectStatement, ok := statement.(*rsql.Select); ok {
		for _, field := range selectStatement.SelectExprs {
			if functionCallField, ok := field.Expr.(*rsql.FunctionCall); ok && functionCallField.IsAggregate {
				if len(functionCallField.Args) > 0 {
					code, ok := functionCallField.Args[0].(*rsql.ExpressionLang)
					if !ok {
						return nil
					}
					if program, err := expr.Compile(code.Val); err != nil {
						return err
					} else {
						//p.AddOperators(&operator.LookupFieldOp{
						//	Field: operator.EvalField{
						//		EvalProgram: program,
						//		FieldId:     code.Val,
						//	},
						//})
						p.AddOperators(&operator.AggregateOp{
							AggregateFunc: functionCallField,
							ArgsProgram:   []*vm.Program{program},
						})
					}

				} else {
					p.AddOperators(&operator.AggregateOp{
						AggregateFunc: functionCallField,
					})
				}

			} else {
				code, ok := field.Expr.(*rsql.Identifier)
				if !ok {
					return nil
				}
				if program, err := expr.Compile(code.Val); err != nil {
					return err
				} else {
					p.AddOperators(&operator.LookupFieldOp{
						Field: operator.EvalField{
							EvalProgram: program,
							FieldId:     code.Val,
							Alias:       field.Alias,
						},
					})
				}
			}
		}
	}
	return nil
}

func (p *LookFieldPlan) Explain() string {
	return ""
}

func (p *LookFieldPlan) Type() string {
	return "LookFieldPlan"
}
