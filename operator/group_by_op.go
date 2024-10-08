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
	"github.com/rulego/streamsql/dataset"
	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/utils/cast"
)

//// CreateGroupByKey 创建GroupByKey
//func CreateGroupByKey(fields ...string) GroupFields {
//	return fields
//}

type GroupByOp struct {
	*BaseOp
	GroupByKey dataset.GroupFields
	keys       []string
}

func (o *GroupByOp) Init(context types.StreamSqlContext) error {
	o.keys = o.GroupByKey.ToList()
	//if selectStreamSqlContext, ok := context.(types.StreamSqlOperatorContext); ok {
	//	selectStreamSqlContext.SetGroupByKey(o.GroupByKey)
	//}
	return nil
}

func (o *GroupByOp) Apply(context types.StreamSqlContext) error {
	if selectStreamSqlContext, ok := context.(types.SelectStreamSqlContext); ok {
		if groupValues, ok := o.getGroupValues(selectStreamSqlContext.InputAsMap()); ok {
			selectStreamSqlContext.SetCurrentGroupValues(groupValues)
		} else {
			selectStreamSqlContext.SetCurrentGroupValues(dataset.EmptyGroupValues)
		}
	}
	return nil
}

func (o *GroupByOp) getGroupValues(data map[string]interface{}) (dataset.GroupValues, bool) {
	var list []string
	for _, key := range o.keys {
		if v, ok := data[key]; ok {
			list = append(list, cast.ToString(v))
		} else {
			return "", false
		}
	}
	return dataset.NewGroupValues(list...), true
}

//// 检查是否是当前分组数据
//func (o *GroupByOp) checkIsGroup(data map[string]interface{}) bool {
//	if data == nil {
//		return false
//	}
//	for _, key := range o.keys {
//		if _, ok := data[key]; ok {
//			return true
//		}
//	}
//	return false
//}
