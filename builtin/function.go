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

package builtin

import "github.com/rulego/streamsql/types"

//type Function struct {
//	AuthorName         string
//	Func         func(args ...any) (any, error)
//	Fast         func(arg any) any
//	ValidateArgs func(args ...any) (any, error)
//	Types        []reflect.Type
//	Validate     func(args []reflect.Type) (reflect.Type, error)
//	Predicate    bool
//}

type AggregateFunction struct {
	//函数名
	Name string
	//别名
	Alias []string
	//函数
	Func func(input []float64, args ...any) (any, error)
	//验证参数
	ValidateArgs func(input []float64, args ...any) (any, error)
	//流计算处理
	StreamHandler types.StreamFunc
}
