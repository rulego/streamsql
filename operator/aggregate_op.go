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

package operator

import (
	"fmt"
	"github.com/expr-lang/expr/vm"
	"github.com/rulego/streamsql/builtin"
	"github.com/rulego/streamsql/dataset"
	"github.com/rulego/streamsql/rsql"
	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/utils/cast"
)

type AggregateOp struct {
	BaseOp
	//WindowType    rsql.WindowType
	AggregateFunc *rsql.FunctionCall
	//dataList      *queue.Queue
	//groupDataList map[types.GroupFields]*queue.Queue
	ArgsProgram []*vm.Program
}

func (o *AggregateOp) Init(context types.StreamSqlContext) error {
	//o.groupDataList = make(map[types.GroupFields]*queue.Queue)

	//o.dataList = queue.NewCircleQueue(10000)
	return nil
}

func (o *AggregateOp) Apply(context types.StreamSqlContext) error {
	if ctx, ok := context.(types.SelectStreamSqlContext); ok {
		for index, arg := range o.AggregateFunc.Args {
			fieldId := arg.(*rsql.ExpressionLang).Val
			if v, ok := ctx.GetColumn(ctx.GetCurrentGroupValues(), dataset.Key(fieldId)); ok {
				ctx.AddFieldAggregateValue(ctx.GetCurrentGroupValues(), fieldId, cast.ToFloat(v))
			} else {
				if result, err := o.eval(o.ArgsProgram[index], ctx.InputAsMap()); err != nil {
					return err
				} else {
					ctx.AddColumn(ctx.GetCurrentGroupValues(), dataset.Float64(fieldId, cast.ToFloat(result)))
					ctx.AddFieldAggregateValue(ctx.GetCurrentGroupValues(), fieldId, cast.ToFloat(result))
				}
			}
		}
	}
	return nil
}

// AddHandler 窗口添加数据事件
func (o *AggregateOp) AddHandler(context types.StreamSqlOperatorContext, data float64) {
	fmt.Println(data)
}

// ArchiveHandler 清除原始数据，观察者需要保存中间过程
func (o *AggregateOp) ArchiveHandler(context types.StreamSqlOperatorContext, dataList []float64) {

}

// StartHandler 窗口开始事件
func (o *AggregateOp) StartHandler(context types.StreamSqlOperatorContext) {

}

// EndHandler 窗口结束事件
func (o *AggregateOp) EndHandler(context types.StreamSqlOperatorContext, dataList []float64) {
	if f, ok := builtin.AggregateBuiltins[o.AggregateFunc.Name]; ok {

		fieldId := o.AggregateFunc.Args[0].(*rsql.ExpressionLang).Val
		values := context.GetFieldAggregateValue(fieldId)
		result, err := f.Func(values)
		fmt.Println(result, err)
		//o.dataList.Reset()
	}
}
