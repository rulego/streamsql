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

import (
	"fmt"
	"github.com/montanaflynn/stats"
)

var AggregateBuiltins = map[string]*AggregateFunction{}

func init() {
	for _, item := range aggregateBuiltins {
		AggregateBuiltins[item.Name] = item
		for _, name := range item.Alias {
			AggregateBuiltins[name] = item
		}
	}
}

var aggregateBuiltins = []*AggregateFunction{
	{
		Name: "avg",
		Func: func(input []float64, args ...any) (any, error) {
			return stats.Mean(input)
		},
	},
	{
		Name: "count",
		Func: func(input []float64, args ...any) (any, error) {
			return len(input), nil
		},
	},
	{
		Name: "sum",
		Func: func(input []float64, args ...any) (any, error) {
			return stats.Sum(input)
		},
	},
	{
		Name: "min",
		Func: func(input []float64, args ...any) (any, error) {
			return stats.Min(input)
		},
	},
	{
		Name: "max",
		Func: func(input []float64, args ...any) (any, error) {
			return stats.Max(input)
		},
	},
	//返回组中所有值的标准差。空值不参与计算。
	{
		Name: "stddev",
		Func: func(input []float64, args ...any) (any, error) {
			return stats.StandardDeviation(input)
		},
	},
	//返回组中所有值的样本标准差。空值不参与计算。
	{
		Name: "stddevs",
		Func: func(input []float64, args ...any) (any, error) {
			return stats.StandardDeviationSample(input)
		},
	},
	{
		Name: "var",
		Func: func(input []float64, args ...any) (any, error) {
			return stats.Variance(input)
		},
	},
	{
		Name: "vars",
		Func: func(input []float64, args ...any) (any, error) {
			return stats.VarS(input)
		},
	},
	{
		Name: "median",
		Func: func(input []float64, args ...any) (any, error) {
			return stats.Median(input)
		},
	},
	{
		Name: "percentile",
		Func: func(input []float64, args ...any) (any, error) {
			if len(args) < 1 {
				return 0, fmt.Errorf("invalid number of arguments for percentile (expected 1, got %d)", len(args))
			}
			if percent, ok := args[0].(float64); !ok {
				return 0, fmt.Errorf("percent need float64 type (got %d)", len(args))
			} else {
				return stats.Percentile(input, percent)
			}
		},
	},
}
