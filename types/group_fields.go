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

import "strings"

// EmptyGroupFields 空的GroupFields
var EmptyGroupFields = NewGroupFields("")

type GroupFields string

func NewGroupFields(keys ...string) GroupFields {
	return GroupFields(strings.Join(keys, ","))
}

func (g GroupFields) ToList() []string {
	return strings.Split(string(g), ",")
}

func (g GroupFields) ToMap() map[string]struct{} {
	keys := g.ToList()
	keysMap := make(map[string]struct{})
	for _, key := range keys {
		keysMap[key] = struct{}{}
	}
	return keysMap
}

// EmptyGroupValues 空的GroupValues
var EmptyGroupValues = NewGroupValues("")

type GroupValues GroupFields

func NewGroupValues(keys ...string) GroupValues {
	return GroupValues(strings.Join(keys, ","))
}
