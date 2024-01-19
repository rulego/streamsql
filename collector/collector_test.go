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

package collector

import (
	"bytes"
	"fmt"
	"github.com/rulego/streamsql/rsql"
	"os"
	"testing"
)

func TestCollector(t *testing.T) {
	sql := "select deviceId,avg(temperature+20/20,aa)/2 as aa from Input where deviceId='aa' OR deviceId='bb' group by deviceId,TumblingWindow(10m) ;"
	newParser := rsql.NewParser(rsql.NewLexer(sql))
	statement := newParser.ParseStatement()
	buf := new(bytes.Buffer)
	statement.Format(buf)
	buf.WriteTo(os.Stdout)
	fmt.Println(statement)
	//collector, err := planner.CreateSelectPlanner(statement.(*rsql.Select))
	//fmt.Println(err)
	//msg := types.NewJsonMsg(0, types.NewMetadata(), `{"temperature":50}`)
	//err = collector.Collect(context.TODO(), msg)
	//fmt.Println(err)
	//time.Sleep(time.Second * 10)
}
