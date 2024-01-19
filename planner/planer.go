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
	"github.com/rulego/streamsql/collector"
	"github.com/rulego/streamsql/rsql"
	"github.com/rulego/streamsql/types"
)

type BaseLogicalPlan struct {
	// operators 计划的实际处理逻辑
	operators []types.Operator
	// children 子计划
	children []types.LogicalPlan
}

// AddOperators 添加操作
func (p *BaseLogicalPlan) AddOperators(operators ...types.Operator) {
	p.operators = append(p.operators, operators...)
}

// AddChildren 添加子计划
func (p *BaseLogicalPlan) AddChildren(plans ...types.LogicalPlan) {
	p.children = append(p.children, plans...)
}

func (p *BaseLogicalPlan) Init(context types.StreamSqlContext) error {
	for _, op := range p.operators {
		if err := op.Init(context); err != nil {
			return err
		}
	}
	for _, plan := range p.children {
		if err := plan.Init(context); err != nil {
			return err
		}
	}
	return nil
}

// Apply 执行
func (p *BaseLogicalPlan) Apply(context types.StreamSqlContext) error {
	for _, op := range p.operators {
		if err := op.Apply(context); err != nil {
			return err
		}
	}
	for _, plan := range p.children {
		if err := plan.Apply(context); err != nil {
			return err
		}
	}
	return nil
}

func (p *BaseLogicalPlan) AllOperators() []types.Operator {
	var operators []types.Operator
	operators = append(operators, p.operators...)
	for _, plan := range p.children {
		operators = append(operators, plan.AllOperators()...)
	}
	return operators
}

func (p *BaseLogicalPlan) AggregateOperators() []types.AggregateOperator {
	var aggregateOperators []types.AggregateOperator
	var operators = p.AllOperators()
	for _, op := range operators {
		if aggregateOp, ok := op.(types.AggregateOperator); ok {
			aggregateOperators = append(aggregateOperators, aggregateOp)
		}
	}
	return aggregateOperators
}

func CreateSelectPlanner(statement *rsql.Select) (types.Collector, error) {
	var err error
	selectStatementPlan := NewSelectStatementPlan()
	err = selectStatementPlan.Plan(statement)
	// 优化
	plan, err := Optimize(selectStatementPlan)

	c := collector.NewStreamCollector(plan)
	// 初始化
	err = c.Init()
	return c, err
}
