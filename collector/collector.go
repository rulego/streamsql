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

package collector

import (
	"context"
	"errors"
	"github.com/rulego/streamsql/dataset"
	"github.com/rulego/streamsql/types"
	"sync"
)

// DefaultSelectStreamSqlContext sql执行生命周期上下文
type DefaultSelectStreamSqlContext struct {
	context.Context
	Collector *StreamCollector
	// 当前传入已经转成map的流数据
	msgAsMap    map[string]interface{}
	msg         types.Msg
	locker      sync.RWMutex
	groupValues types.GroupValues
}

func NewDefaultSelectStreamSqlContext(ctx context.Context, collector *StreamCollector, msg types.Msg) *DefaultSelectStreamSqlContext {
	return &DefaultSelectStreamSqlContext{
		Context:   ctx,
		Collector: collector,
		msg:       msg,
	}
}
func (c *DefaultSelectStreamSqlContext) Decode() error {
	if c.msg.Data == nil {
		return errors.New("data can not nil")
	}
	//已经解码
	if c.msgAsMap != nil {
		return nil
	}
	if mapData, err := c.msg.Data.DecodeAsMap(); err != nil {
		return err
	} else {
		c.msgAsMap = mapData
	}
	return nil
}

func (c *DefaultSelectStreamSqlContext) InputAsMap() map[string]interface{} {
	c.locker.RLock()
	defer c.locker.RUnlock()
	return c.msgAsMap
}

func (c *DefaultSelectStreamSqlContext) RawInput() types.Msg {
	return c.msg
}

func (c *DefaultSelectStreamSqlContext) SetGroupByKey(groupByKey types.GroupFields) {
	c.Collector.SetGroupByKey(groupByKey)
}
func (c *DefaultSelectStreamSqlContext) GetGroupByKey() types.GroupFields {
	return c.Collector.GetGroupByKey()
}
func (c *DefaultSelectStreamSqlContext) IsInitWindow(groupValues types.GroupValues) bool {
	return c.Collector.IsInitWindow(groupValues)
}

func (c *DefaultSelectStreamSqlContext) AddWindow(groupByKey types.GroupValues, window types.Window) {
	c.Collector.AddWindow(groupByKey, window)
}

func (c *DefaultSelectStreamSqlContext) CreteWindowObserver() types.WindowObserver {
	return c.Collector.CreteWindowObserver()
}

func (c *DefaultSelectStreamSqlContext) GetWindow(groupByKey types.GroupValues) types.Window {
	return c.Collector.GetWindow(groupByKey)
}

func (c *DefaultSelectStreamSqlContext) GetRow(groupValues types.GroupValues) (*dataset.Row, bool) {
	return c.Collector.GetRow(groupValues)
}

func (c *DefaultSelectStreamSqlContext) AddColumn(groupValues types.GroupValues, kv dataset.KeyValue) {
	c.Collector.AddColumn(groupValues, kv)
}

func (c *DefaultSelectStreamSqlContext) GetColumn(groupValues types.GroupValues, key dataset.Key) (dataset.KeyValue, bool) {
	return c.Collector.GetColumn(groupValues, key)
}

//func (c *DefaultSelectStreamSqlContext) AddField(fieldId string, value any) {
//	c.locker.Lock()
//	defer c.locker.Unlock()
//	c.msgAsMap[fieldId] = value
//
//}
//
//func (c *DefaultSelectStreamSqlContext) GetField(fieldId string) (any, bool) {
//	c.locker.RLock()
//	defer c.locker.RUnlock()
//	v, ok := c.msgAsMap[fieldId]
//	return v, ok
//}

func (c *DefaultSelectStreamSqlContext) SetCurrentGroupValues(groupValues types.GroupValues) {
	c.groupValues = groupValues
}

func (c *DefaultSelectStreamSqlContext) GetCurrentGroupValues() types.GroupValues {
	return c.groupValues
}

func (c *DefaultSelectStreamSqlContext) AddFieldAggregateValue(groupValues types.GroupValues, fieldId string, value float64) {
	c.Collector.AddFieldAggregateValue(groupValues, fieldId, value)
}

func (c *DefaultSelectStreamSqlContext) GetFieldAggregateValue(groupValues types.GroupValues, fieldId string) []float64 {
	return c.Collector.GetFieldAggregateValue(groupValues, fieldId)
}

type aggregateFieldValue struct {
	GroupFields  types.GroupFields
	FieldWindows map[string]types.Window
}

func (afv *aggregateFieldValue) AddFieldValue(fieldId string, data float64) {
	if w, ok := afv.FieldWindows[fieldId]; !ok {
		//afv.FieldWindows[fieldId] = newQueue
	} else {
		w.Add(data)
	}
}

func (afv *aggregateFieldValue) GetFieldValues(fieldId string) []float64 {
	return nil
}

// StreamCollector 收集器
type StreamCollector struct {
	context.Context
	keyedWindow          map[types.GroupValues]types.Window
	planner              types.LogicalPlan
	aggregateOperators   []types.AggregateOperator
	groupByKey           types.GroupFields
	aggregateFieldValues map[types.GroupValues]*aggregateFieldValue
	rows                 *dataset.Rows
	sync.Mutex
}

func (c *StreamCollector) SetGroupByKey(groupByKey types.GroupFields) {
	c.groupByKey = groupByKey
}
func (c *StreamCollector) GetGroupByKey() types.GroupFields {
	return c.groupByKey
}

func (c *StreamCollector) IsInitWindow(groupValues types.GroupValues) bool {
	_, ok := c.keyedWindow[groupValues]
	return ok
}

func (c *StreamCollector) AddWindow(groupByKey types.GroupValues, window types.Window) {
	c.keyedWindow[groupByKey] = window
}

func (c *StreamCollector) GetWindow(groupByKey types.GroupValues) types.Window {
	return c.keyedWindow[groupByKey]
}

func (c *StreamCollector) CreteWindowObserver() types.WindowObserver {
	return types.WindowObserver{
		AddHandler: func(context types.StreamSqlOperatorContext, data float64) {
			for _, op := range c.aggregateOperators {
				op.AddHandler(context, data)
			}
		},
		ArchiveHandler: func(context types.StreamSqlOperatorContext, dataList []float64) {
			for _, op := range c.aggregateOperators {
				op.ArchiveHandler(context, dataList)
			}
		},
		StartHandler: func(context types.StreamSqlOperatorContext) {
		},
		EndHandler: func(context types.StreamSqlOperatorContext, dataList []float64) {
			for _, op := range c.aggregateOperators {
				op.EndHandler(context, dataList)
			}

		},
	}
}
func (c *StreamCollector) AddFieldAggregateValue(groupValues types.GroupValues, fieldId string, data float64) {
	c.Lock()
	defer c.Unlock()
	if w, ok := c.keyedWindow[groupValues]; ok {
		w.Add(data)
	}
}

func (c *StreamCollector) GetFieldAggregateValue(groupValues types.GroupValues, fieldId string) []float64 {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.keyedWindow[groupValues]; ok {
		return nil
	}
	return nil
}

func (c *StreamCollector) GetRow(groupValues types.GroupValues) (*dataset.Row, bool) {
	c.Lock()
	defer c.Unlock()
	return c.rows.GetRow(groupValues)
}

func (c *StreamCollector) AddColumn(groupValues types.GroupValues, kv dataset.KeyValue) {
	c.Lock()
	defer c.Unlock()
	c.rows.AddColumn(groupValues, kv)
}

func (c *StreamCollector) GetColumn(groupValues types.GroupValues, key dataset.Key) (dataset.KeyValue, bool) {
	c.Lock()
	defer c.Unlock()
	return c.rows.GetColumn(groupValues, key)
}

func NewStreamCollector(planner types.LogicalPlan) *StreamCollector {
	c := &StreamCollector{
		planner:              planner,
		aggregateOperators:   planner.AggregateOperators(),
		keyedWindow:          make(map[types.GroupValues]types.Window),
		aggregateFieldValues: make(map[types.GroupValues]*aggregateFieldValue),
		rows:                 dataset.NewRows(),
	}
	return c
}

// Init 初始化
func (c *StreamCollector) Init() error {
	return c.planner.Init(c)
}

func (c *StreamCollector) Collect(ctx context.Context, msg types.Msg) error {
	selectStreamSqlContext := NewDefaultSelectStreamSqlContext(ctx, c, msg)
	if err := selectStreamSqlContext.Decode(); err != nil {
		return err
	}
	return c.planner.Apply(selectStreamSqlContext)
}
