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

package dataset

type Row struct {
	GroupValues GroupValues
	Columns     map[Key]KeyValue
}

func NewRow(groupValues GroupValues) *Row {
	r := &Row{
		GroupValues: groupValues,
		Columns:     make(map[Key]KeyValue),
	}
	return r
}

func (r *Row) AddColumn(kv KeyValue) {
	if r.Columns == nil {
		r.Columns = make(map[Key]KeyValue)
	}
	r.Columns[kv.Key] = kv
}

func (r *Row) GetColumn(key Key) (KeyValue, bool) {
	if r.Columns == nil {
		return KeyValue{}, false
	}
	v, ok := r.Columns[key]
	return v, ok
}

type Rows struct {
	Rows map[GroupValues]*Row
}

func NewRows() *Rows {
	r := &Rows{
		Rows: make(map[GroupValues]*Row),
	}
	return r
}

func (r *Rows) AddColumn(groupValues GroupValues, kv KeyValue) {
	if r.Rows == nil {
		r.Rows = make(map[GroupValues]*Row)
	}
	row, ok := r.Rows[groupValues]
	if !ok {
		row = NewRow(groupValues)
		r.Rows[groupValues] = row
	}
	row.AddColumn(kv)
}

func (r *Rows) GetColumn(groupValues GroupValues, key Key) (KeyValue, bool) {
	if r.Rows == nil {
		return KeyValue{}, false
	}
	row, ok := r.Rows[groupValues]
	if !ok {
		return KeyValue{}, false
	}
	return row.GetColumn(key)
}

func (r *Rows) GetRow(groupValues GroupValues) (*Row, bool) {
	if r.Rows == nil {
		return nil, false
	}
	v, ok := r.Rows[groupValues]
	return v, ok
}

func (r *Rows) AddRow(row *Row) {
	if row == nil {
		return
	}
	if r.Rows == nil {
		r.Rows = make(map[GroupValues]*Row)
	}
	r.Rows[row.GroupValues] = row
}
