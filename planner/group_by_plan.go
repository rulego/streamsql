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
	"github.com/rulego/streamsql/operator"
	"github.com/rulego/streamsql/rsql"
	"github.com/rulego/streamsql/types"
)

type GroupByPlan struct {
	*BaseLogicalPlan
}

func (p *GroupByPlan) New() types.LogicalPlan {
	return &GroupByPlan{BaseLogicalPlan: &BaseLogicalPlan{}}
}

func (p *GroupByPlan) Plan(statement rsql.Statement) error {
	if selectStatement, ok := statement.(*rsql.Select); ok {
		for _, item := range selectStatement.GroupBy {
			if functionCallField, ok := item.(*rsql.FunctionCall); ok && functionCallField.IsWindow {
				p.AddOperators(&operator.WindowOp{
					WindowType: rsql.LookupIsWindowFunc(functionCallField.Name),
				})
			} else {
				p.AddOperators(&operator.GroupByOp{
					GroupByKey: types.GroupFields(item.(*rsql.Identifier).Val),
				})
			}
		}
	}
	return nil
}

func (p *GroupByPlan) Explain() string {
	return ""
}

func (p *GroupByPlan) Type() string {
	return "GroupByPlan"
}
