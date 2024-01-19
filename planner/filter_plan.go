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
	"github.com/rulego/streamsql/operator"
	"github.com/rulego/streamsql/rsql"
	"github.com/rulego/streamsql/types"
)

type FilterPlan struct {
	*BaseLogicalPlan
}

func (p *FilterPlan) New() types.LogicalPlan {
	return &FilterPlan{BaseLogicalPlan: &BaseLogicalPlan{}}
}

func (p *FilterPlan) Plan(statement rsql.Statement) error {
	if selectStatement, ok := statement.(*rsql.Select); ok {
		if expressionLang, ok := selectStatement.Where.(*rsql.ExpressionLang); ok {
			code := expressionLang.Val
			if program, err := expr.Compile(code); err != nil {
				return err
			} else {
				p.AddOperators(&operator.FilterOp{Program: program})
			}
		}
	}
	return nil
}

func (p *FilterPlan) Explain() string {
	return ""
}

func (p *FilterPlan) Type() string {
	return "FilterPlan"
}
