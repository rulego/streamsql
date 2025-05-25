/*
 * Copyright 2025 The RuleGo Authors.
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

// Package types 定义了StreamSQL系统中使用的核心数据类型和配置结构。
// 包含投影配置、窗口配置、字段表达式等SQL处理过程中的关键类型定义。
package types

import (
	"time"

	"github.com/rulego/streamsql/aggregator"
)

// ProjectionSourceType defines the source of a projected field's value.
type ProjectionSourceType int

const (
	SourceGroupKey ProjectionSourceType = iota
	SourceAggregateResult
	SourceWindowProperty // For window_start, window_end
)

// Projection indicates one item in the SELECT list.
type Projection struct {
	OutputName string               // Final name in the result map (e.g., alias or derived name)
	SourceType ProjectionSourceType // What kind of value is this?
	// InputName refers to:
	// - the name of the group key field, if SourceType is SourceGroupKey.
	// - the key for the aggregate in the SelectFields map (usually the field being aggregated, e.g., "temperature" for AVG(temperature)), if SourceType is SourceAggregateResult.
	// - the property name ("window_start", "window_end"), if SourceType is SourceWindowProperty.
	InputName string
}

// FieldExpression 存储字段表达式信息
type FieldExpression struct {
	// Field 原始字段名
	Field string
	// Expression 完整表达式
	Expression string
	// Fields 表达式中引用的所有字段
	Fields []string
}

type Config struct {
	WindowConfig WindowConfig
	GroupFields  []string
	// SelectFields: key is the field to aggregate (e.g., "temperature", or "window_start"), value is AggregateType
	SelectFields map[string]aggregator.AggregateType
	// FieldAlias: key is the original field name used in an aggregation (e.g. "temperature" for sum(temperature)), value is the desired output alias (e.g. "sum_temp")
	// For window_start(), key could be "window_start", value could be "start_time".
	FieldAlias map[string]string
	Distinct   bool
	// Projections: Defines the fields to be included in the final output and their sources.
	Projections []Projection
	// Limit: 限制结果集的大小，0表示不限制
	Limit int
	// 是否需要窗口处理
	NeedWindow bool
	// 非聚合查询的字段列表
	SimpleFields []string
	// Having: HAVING子句，用于过滤聚合结果
	Having string
	// FieldExpressions: 字段表达式映射，key是字段名，value是表达式信息
	FieldExpressions map[string]FieldExpression
}

type WindowConfig struct {
	Type       string
	Params     map[string]interface{}
	TsProp     string
	TimeUnit   time.Duration
	GroupByKey string // 会话窗口分组键
}
