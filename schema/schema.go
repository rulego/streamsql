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

// Package schema provides a runtime registry for typed record schemas and
// validates incoming maps against the declared field definitions.
package schema

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// DataType enumerates the field types understood by the schema validator.
type DataType int

// Field type identifiers used by Schema definitions.
const (
	TypeInt DataType = iota
	TypeInt64
	TypeFloat
	TypeBool
	TypeString
	TypeTime
	TypeArray
	TypeMap
	TypeAny
)

// String returns the textual name of the DataType.
func (t DataType) String() string {
	switch t {
	case TypeInt:
		return "int"
	case TypeInt64:
		return "int64"
	case TypeFloat:
		return "float"
	case TypeBool:
		return "bool"
	case TypeString:
		return "string"
	case TypeTime:
		return "time"
	case TypeArray:
		return "array"
	case TypeMap:
		return "map"
	case TypeAny:
		return "any"
	default:
		return "unknown"
	}
}

// InferType maps a runtime value to its DataType.
//
// nil and any unrecognised value return TypeAny. float32 and float64 both map
// to TypeFloat. Only []any and map[string]any are recognised
// as containers; other slice or map types fall back to TypeAny.
func InferType(v any) DataType {
	if v == nil {
		return TypeAny
	}
	switch v.(type) {
	case int:
		return TypeInt
	case int64:
		return TypeInt64
	case float32, float64:
		return TypeFloat
	case bool:
		return TypeBool
	case string:
		return TypeString
	case time.Time:
		return TypeTime
	case []any:
		return TypeArray
	case map[string]any:
		return TypeMap
	default:
		return TypeAny
	}
}

// FieldDef describes a single field of a Schema.
type FieldDef struct {
	// Name is the field key matched against the input map.
	Name string
	// Type is the expected DataType of the field value.
	Type DataType
	// Required reports whether an absent key is an error.
	Required bool
	// Default, when non-nil, fills an absent key with this value (and so also
	// suppresses the required-missing error). Callers must use a typed value
	// (for example float64(0)) so that the interface is non-nil even when the
	// default is a zero value.
	Default any
}

// Schema is a named, ordered set of field definitions.
type Schema struct {
	// Name uniquely identifies the schema within a Registry.
	Name string
	// Fields is the ordered list of field definitions.
	Fields []FieldDef
	// Strict, when true, makes unknown keys in the input map an error.
	Strict bool
}

// MultiError aggregates one or more validation errors into a single value.
// It is returned by Validate when more than one problem is found.
type MultiError struct {
	// Errors holds the individual validation errors in the order they were found.
	Errors []error
}

// Append records err when it is non-nil.
func (m *MultiError) Append(err error) {
	if err != nil {
		m.Errors = append(m.Errors, err)
	}
}

// Err returns nil when no errors were collected, or the receiver itself.
func (m *MultiError) Err() error {
	if len(m.Errors) == 0 {
		return nil
	}
	return m
}

// Error joins the collected error messages with a semicolon separator.
func (m *MultiError) Error() string {
	msgs := make([]string, len(m.Errors))
	for i, e := range m.Errors {
		msgs[i] = e.Error()
	}
	return strings.Join(msgs, "; ")
}

// Registry stores named Schemas and is safe for concurrent use.
type Registry struct {
	mu      sync.RWMutex
	schemas map[string]Schema
}

// NewRegistry returns an empty Registry ready to accept registrations.
func NewRegistry() *Registry {
	return &Registry{schemas: make(map[string]Schema)}
}

// Register stores s under s.Name.
// It returns an error when the name is empty or already registered.
func (r *Registry) Register(s Schema) error {
	if s.Name == "" {
		return errors.New("schema: empty name")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.schemas[s.Name]; exists {
		return fmt.Errorf("schema: %q already registered", s.Name)
	}
	r.schemas[s.Name] = s
	return nil
}

// Get returns the Schema registered under name and a found flag.
func (r *Registry) Get(name string) (Schema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.schemas[name]
	return s, ok
}

// MustGet returns the Schema registered under name and panics when missing.
func (r *Registry) MustGet(name string) Schema {
	s, ok := r.Get(name)
	if !ok {
		panic(fmt.Sprintf("schema: %q not found", name))
	}
	return s
}

// Default is the package-level Registry shared by callers that do not need a private one.
var Default = NewRegistry()

// Validate reports every problem found in data with respect to s.
//
// An absent key errors only when the field is Required and has no Default; a
// field with a non-nil Default is filled with that value in data. Present
// values are checked with InferType; numeric fields (int, int64, float) accept
// any numeric value interchangeably, and TypeAny accepts every value including
// nil. When Strict is true, keys in data that are not declared as fields are
// reported as errors. All problems are aggregated and returned together, with
// a nil result when data is clean.
func (s *Schema) Validate(data map[string]any) error {
	var errs MultiError

	defined := make(map[string]bool, len(s.Fields))
	for _, f := range s.Fields {
		defined[f.Name] = true
	}

	if s.Strict {
		for k := range data {
			if !defined[k] {
				errs.Append(fmt.Errorf("schema %q: unknown field %q", s.Name, k))
			}
		}
	}

	for _, f := range s.Fields {
		v, present := data[f.Name]
		if !present {
			if f.Default != nil {
				// Fill the declared default so downstream sees the value, not nil.
				data[f.Name] = f.Default
				continue
			}
			if f.Required {
				errs.Append(fmt.Errorf("schema %q: required field %q is missing", s.Name, f.Name))
			}
			continue
		}
		if !typeMatches(f.Type, v) {
			errs.Append(fmt.Errorf("schema %q: field %q expects %s, got %s", s.Name, f.Name, f.Type, InferType(v)))
		}
	}

	return errs.Err()
}

// typeMatches reports whether v is acceptable for a field declared as expected.
func typeMatches(expected DataType, v any) bool {
	if expected == TypeAny {
		return true
	}
	actual := InferType(v)
	if expected == actual {
		return true
	}
	return isNumeric(expected) && isNumeric(actual)
}

// isNumeric reports whether t is one of the interchangeable numeric field types.
func isNumeric(t DataType) bool {
	return t == TypeInt || t == TypeInt64 || t == TypeFloat
}
