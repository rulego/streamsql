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

package streamsql

import (
	"github.com/rulego/streamsql/rsql"
	"github.com/rulego/streamsql/stream"
)

// Streamsql 流式SQL，用于对流式数据进行SQL查询和计算
type Streamsql struct {
	stream *stream.Stream
}

// New returns a new Streamsql job runner, modified by the given options.
func New(opts ...Option) *Streamsql {
	return &Streamsql{}
}

// Execute 执行SQ
// 如果执行成功，则返回nil，否则返回错误信息
func (s *Streamsql) Execute(sql string) error {
	var err error
	//根据sql初始stream，并启动stream
	stmt, err := rsql.NewParser(sql).Parse()
	if err != nil {
		return err
	}
	config, condition, err := stmt.ToStreamConfig()
	if err != nil {
		return err
	}
	s.stream, err = stream.NewStream(*config)
	if err != nil {
		return err
	}
	err = s.stream.RegisterFilter(condition)
	if err != nil {
		return err
	}
	//开始接收和处理数据
	s.stream.Start()
	return nil

}

// Stop 停止接收和处理数据
func (s *Streamsql) Stop() {
}

// GetResult 获取结果
func (s *Streamsql) GetResult() <-chan interface{} {
	return s.stream.GetResultsChan()
}

// AddData 添加流数据
func (s *Streamsql) AddData(data interface{}) {
	s.stream.AddData(data)
}

func (s *Streamsql) Stream() *stream.Stream {
	return s.stream
}
