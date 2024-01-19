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

package types

import (
	"encoding/json"
	"time"
)

// DataType 消息数据类型
type DataType string

const (
	JSON      = DataType("JSON")
	TEXT      = DataType("TEXT")
	BINARY    = DataType("BINARY")
	INTERFACE = DataType("interface")
)

// Metadata 消息元数据
type Metadata map[string]string

// NewMetadata 创建一个新的消息元数据实例
func NewMetadata() Metadata {
	return make(Metadata)
}

// BuildMetadata 通过map，创建一个新的消息元数据实例
func BuildMetadata(data Metadata) Metadata {
	metadata := make(Metadata)
	for k, v := range data {
		metadata[k] = v
	}
	return metadata
}

// Copy 复制
func (md Metadata) Copy() Metadata {
	return BuildMetadata(md)
}

// Has 是否存在某个key
func (md Metadata) Has(key string) bool {
	_, ok := md[key]
	return ok
}

// GetValue 通过key获取值
func (md Metadata) GetValue(key string) string {
	v, _ := md[key]
	return v
}

// PutValue 设置值
func (md Metadata) PutValue(key, value string) {
	if key != "" {
		md[key] = value
	}
}

// Values 获取所有值
func (md Metadata) Values() map[string]string {
	return md
}

// Msg 消息
type Msg struct {
	// 消息时间戳
	Ts int64 `json:"ts"`
	//数据类型
	DataType DataType `json:"dataType"`
	//消息内容
	Data IData `json:"data"`
	//消息元数据
	Metadata Metadata `json:"metadata"`
}

func NewJsonMsg(ts int64, metaData Metadata, data string) Msg {
	return newMsg(ts, JSON, metaData, JsonData(data))
}
func NewTextMsg(ts int64, metaData Metadata, data string) Msg {
	return newMsg(ts, TEXT, metaData, TextData(data))
}
func NewBinaryMsg(ts int64, metaData Metadata, data []byte) Msg {
	return newMsg(ts, BINARY, metaData, BinaryData(data))
}
func NewInterfaceMsg(ts int64, metaData Metadata, data map[string]any) Msg {
	return newMsg(ts, INTERFACE, metaData, InterfaceData(data))
}

func newMsg(ts int64, dataType DataType, metaData Metadata, data IData) Msg {
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	}
	return Msg{
		Ts:       ts,
		DataType: dataType,
		Data:     data,
		Metadata: metaData,
	}
}

// Copy 复制
func (m *Msg) Copy() Msg {
	return newMsg(m.Ts, m.DataType, m.Metadata.Copy(), m.Data)
}

type IData interface {
	Decode() (any, error)
	DecodeAsMap() (map[string]any, error)
}

type JsonData string

func (d JsonData) Decode() (any, error) {
	return d.DecodeAsMap()
}

func (d JsonData) DecodeAsMap() (map[string]any, error) {
	var result map[string]any
	err := json.Unmarshal([]byte(d), &result)
	return result, err
}

type InterfaceData map[string]any

func (d InterfaceData) Decode() (any, error) {
	return d, nil
}

func (d InterfaceData) DecodeAsMap() (map[string]any, error) {
	return d, nil
}

type TextData string

func (d TextData) Decode() (any, error) {
	return d, nil
}

func (d TextData) DecodeAsMap() (map[string]any, error) {
	return nil, ErrUnsupported
}

type BinaryData []byte

func (d BinaryData) Decode() (any, error) {
	return string(d), ErrUnsupported
}

func (d BinaryData) DecodeAsMap() (map[string]any, error) {
	return nil, ErrUnsupported
}
