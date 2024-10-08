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
	"github.com/expr-lang/expr/vm"
	"github.com/rulego/streamsql/dataset"
	"github.com/rulego/streamsql/types"
)

type LookupFieldOp struct {
	*BaseOp
	Field EvalField
}

func (o *LookupFieldOp) Init(context types.StreamSqlContext) error {
	return nil
}

func (o *LookupFieldOp) Apply(context types.StreamSqlContext) error {
	if ctx, ok := context.(types.SelectStreamSqlContext); ok {
		if o.Field.EvalProgram != nil {
			if result, err := o.eval(o.Field.EvalProgram, ctx.InputAsMap()); err == nil {
				ctx.AddColumn(ctx.GetCurrentGroupValues(), dataset.Interface(o.Field.FieldId, result))
			} else {
				return err
			}
		}
	}
	return nil
}

type EvalField struct {
	EvalProgram *vm.Program
	FieldId     string
	Alias       string
}
