/*
 * Copyright 2023 The RuleGo Authors.
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

package rsql

import (
	"fmt"
	"github.com/expr-lang/expr"
	"github.com/montanaflynn/stats"
	"testing"
	"time"
)

//type Env struct {
//	Posts []Post `expr:"posts"`
//}
//
//func (Env) Format(t time.Time) string {
//	return t.Format(time.RFC822)
//}

type Post struct {
	Name string
	Body string
	Date time.Time
}

func TestExpr(t *testing.T) {

	env := map[string]interface{}{
		"greet":   "Hello, %v!",
		"names":   []string{"world", "you"},
		"sprintf": fmt.Sprintf,
		"name":    "WORLd",
		"name2":   "我们",
		"name3":   "5oiR5Lus",
		"foo":     100.2,
		"bar":     2004,
		"post":    Post{Name: "lala", Body: "aa"},
	}
	//code := `(foo + bar)/2`
	//code := `duration("1m")`
	code := `fromBase64(name3)`
	program, err := expr.Compile(code, expr.Env(env), expr.AllowUndefinedVariables())
	if err != nil {
		panic(err)
	}

	output, err := expr.Run(program, env)
	if err != nil {
		panic(err)
	}
	fmt.Println(output)
}

func TestStat(t *testing.T) {
	// start with some source data to use
	data := []float64{1.0, 2.1, 3.2, 4.823, 4.1, 5.8}
	//随机生成100W数据
	for i := 0; i < 10000000; i++ {
		data = append(data, float64(i))
	}
	// you could also use different types like this
	// data := stats.LoadRawData([]int{1, 2, 3, 4, 5})
	// data := stats.LoadRawData([]interface{}{1.1, "2", 3})
	// etc...
	//计算耗时
	start := time.Now()
	median, _ := stats.StandardDeviation(data)
	fmt.Printf("%f", median)
	fmt.Println("\n", time.Since(start))
}
