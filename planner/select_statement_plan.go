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
	"github.com/rulego/streamsql/rsql"
	"github.com/rulego/streamsql/types"
)

// selectPlans 注册select statement planner组件
// 注册顺序不能乱，注册顺序会影响执行结果
var selectPlans = []types.LogicalPlan{
	&TablePlan{BaseLogicalPlan: &BaseLogicalPlan{}},
	&FilterPlan{BaseLogicalPlan: &BaseLogicalPlan{}},
	&GroupByPlan{BaseLogicalPlan: &BaseLogicalPlan{}},
	&LookFieldPlan{BaseLogicalPlan: &BaseLogicalPlan{}},
	&LimitPlan{BaseLogicalPlan: &BaseLogicalPlan{}},
	&OrderByPlan{BaseLogicalPlan: &BaseLogicalPlan{}},
}

type SelectStatementPlan struct {
	*BaseLogicalPlan
}

func NewSelectStatementPlan() *SelectStatementPlan {
	selectStatementPlan := &SelectStatementPlan{BaseLogicalPlan: &BaseLogicalPlan{}}
	selectStatementPlan.AddChildren(selectPlans...)
	return selectStatementPlan
}

func (p *SelectStatementPlan) New() types.LogicalPlan {
	return &SelectStatementPlan{BaseLogicalPlan: &BaseLogicalPlan{}}
}

func (p *SelectStatementPlan) Plan(statement rsql.Statement) error {
	for _, plan := range p.children {
		if err := plan.Plan(statement); err != nil {
			return err
		}
	}
	return nil
}

func (p *SelectStatementPlan) Explain() string {
	return ""
}

func (p *SelectStatementPlan) Type() string {
	return "SelectStatementPlan"
}
