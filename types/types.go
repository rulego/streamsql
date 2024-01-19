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

package types

import (
	"context"
	"github.com/rulego/streamsql/dataset"
	"github.com/rulego/streamsql/rsql"
	"time"
)

type Window interface {
	// FieldName 获取聚合运算字段名称
	FieldName() string
	// LastData 获取最后一条数据
	LastData() (float64, bool)
	// StartTime 获取窗口开始时间
	StartTime() time.Time
	// EndTime 获取窗口结束时间
	EndTime() time.Time
	// Add 添加数据
	Add(data float64)
	//Archive 保存数据
	Archive()
	Stop()
	////Release 释放空间
	//Release()
}

//// StreamWindow 流式计算window,不保存原始数据
//type StreamWindow interface {
//	Window
//}

// StreamSqlContext 上下文
type StreamSqlContext interface {
	context.Context
}

type StreamSqlOperatorContext interface {
	StreamSqlContext
	AddWindow(groupValues GroupValues, window Window)
	CreteWindowObserver() WindowObserver
	//GetWindow 获取窗口实例
	GetWindow(groupValues GroupValues) Window
	IsInitWindow(groupValues GroupValues) bool
	AddFieldAggregateValue(groupValues GroupValues, fieldId string, value float64)
	GetFieldAggregateValue(groupValues GroupValues, fieldId string) []float64
	SetGroupByKey(groupByKey GroupFields)
	GetRow(groupValues GroupValues) (*dataset.Row, bool)
	AddColumn(groupValues GroupValues, kv dataset.KeyValue)
	GetColumn(groupValues GroupValues, key dataset.Key) (dataset.KeyValue, bool)
}

type SelectStreamSqlContext interface {
	StreamSqlOperatorContext
	InputAsMap() map[string]interface{}
	RawInput() Msg
	SetCurrentGroupValues(groupValues GroupValues)
	GetCurrentGroupValues() GroupValues
}

// WindowObserver 窗口事件观察者
type WindowObserver struct {
	// AddHandler 窗口添加数据事件
	AddHandler func(context StreamSqlOperatorContext, data float64)
	////FullHandler 空间满事件
	//FullHandler func(context StreamSqlContext, dataList []float64)
	//ArchiveHandler 清除原始数据，观察者需要保存中间过程
	ArchiveHandler func(context StreamSqlOperatorContext, dataList []float64)
	//StartHandler 窗口开始事件
	StartHandler func(context StreamSqlOperatorContext)
	//EndHandler 窗口结束事件
	EndHandler func(context StreamSqlOperatorContext, dataList []float64)
}

// Operator 操作器接口
type Operator interface {
	Init(context StreamSqlContext) error
	// Apply 执行
	Apply(context StreamSqlContext) error
}

type AggregateOperator interface {
	Operator
	AddHandler(context StreamSqlOperatorContext, data float64)
	ArchiveHandler(context StreamSqlOperatorContext, dataList []float64)
	StartHandler(context StreamSqlOperatorContext)
	EndHandler(context StreamSqlOperatorContext, dataList []float64)
}

// LogicalPlan 逻辑计划接口
type LogicalPlan interface {
	New() LogicalPlan
	Plan(statement rsql.Statement) error
	Init(context StreamSqlContext) error
	Apply(context StreamSqlContext) error
	Explain() string
	Type() string
	// AllOperators 获取所有操作器
	AllOperators() []Operator
	// AggregateOperators 获取所有聚合操作器
	AggregateOperators() []AggregateOperator
}

type Collector interface {
	Collect(ctx context.Context, msg Msg) error
}
