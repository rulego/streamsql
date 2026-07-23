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

package reflectutil

import (
	"reflect"
	"testing"
)

// TestStruct is used for testing structures
type TestStruct struct {
	Name    string
	Age     int
	Email   string
	Active  bool
	Balance float64
}

// TestSafeFieldByName Tests the basic functionality of the SafeFieldByName function
func TestSafeFieldByName(t *testing.T) {
	testObj := TestStruct{
		Name:    "John Doe",
		Age:     30,
		Email:   "john@example.com",
		Active:  true,
		Balance: 1000.50,
	}

	v := reflect.ValueOf(testObj)

	// Test to retrieve the fields that exist
	nameField, err := SafeFieldByName(v, "Name")
	if err != nil {
		t.Errorf("Expected no error for existing field 'Name', got: %v", err)
	}

	if !nameField.IsValid() {
		t.Error("Expected valid field for 'Name'")
	}

	if nameField.String() != "John Doe" {
		t.Errorf("Expected field value 'John Doe', got: %v", nameField.String())
	}

	// Test to get the Age field
	ageField, err := SafeFieldByName(v, "Age")
	if err != nil {
		t.Errorf("Expected no error for existing field 'Age', got: %v", err)
	}

	if ageField.Int() != 30 {
		t.Errorf("Expected field value 30, got: %v", ageField.Int())
	}

	// Test to get the Active field
	activeField, err := SafeFieldByName(v, "Active")
	if err != nil {
		t.Errorf("Expected no error for existing field 'Active', got: %v", err)
	}

	if !activeField.Bool() {
		t.Errorf("Expected field value true, got: %v", activeField.Bool())
	}

	// Test to get the Balance field
	balanceField, err := SafeFieldByName(v, "Balance")
	if err != nil {
		t.Errorf("Expected no error for existing field 'Balance', got: %v", err)
	}

	if balanceField.Float() != 1000.50 {
		t.Errorf("Expected field value 1000.50, got: %v", balanceField.Float())
	}
}

// TestSafeFieldByNameNonExistentField tests to retrieve fields that do not exist
func TestSafeFieldByNameNonExistentField(t *testing.T) {
	testObj := TestStruct{Name: "John Doe"}
	v := reflect.ValueOf(testObj)

	// Test to retrieve fields that don't exist
	_, err := SafeFieldByName(v, "NonExistentField")
	if err == nil {
		t.Error("Expected error for non-existent field, got nil")
	}

	expectedError := "field NonExistentField not found"
	if err.Error() != expectedError {
		t.Errorf("Expected error message '%s', got: %v", expectedError, err.Error())
	}
}

// TestSafeFieldByNameInvalidValue Invalid test reflect.Value
func TestSafeFieldByNameInvalidValue(t *testing.T) {
	// Create an invalid reflect.Value
	var invalidValue reflect.Value

	_, err := SafeFieldByName(invalidValue, "Name")
	if err == nil {
		t.Error("Expected error for invalid value, got nil")
	}

	expectedError := "invalid value"
	if err.Error() != expectedError {
		t.Errorf("Expected error message '%s', got: %v", expectedError, err.Error())
	}
}

// TestSafeFieldByNameNonStructValue tests the value of the non-structure type
func TestSafeFieldByNameNonStructValue(t *testing.T) {
	// Test string types
	stringValue := reflect.ValueOf("test string")
	_, err := SafeFieldByName(stringValue, "Name")
	if err == nil {
		t.Error("Expected error for non-struct value, got nil")
	}

	expectedError := "value is not a struct, got string"
	if err.Error() != expectedError {
		t.Errorf("Expected error message '%s', got: %v", expectedError, err.Error())
	}

	// Test integer types
	intValue := reflect.ValueOf(42)
	_, err = SafeFieldByName(intValue, "Name")
	if err == nil {
		t.Error("Expected error for non-struct value, got nil")
	}

	expectedError = "value is not a struct, got int"
	if err.Error() != expectedError {
		t.Errorf("Expected error message '%s', got: %v", expectedError, err.Error())
	}

	// Test the type of slice
	sliceValue := reflect.ValueOf([]string{"a", "b", "c"})
	_, err = SafeFieldByName(sliceValue, "Name")
	if err == nil {
		t.Error("Expected error for non-struct value, got nil")
	}

	expectedError = "value is not a struct, got slice"
	if err.Error() != expectedError {
		t.Errorf("Expected error message '%s', got: %v", expectedError, err.Error())
	}
}

// TestSafeFieldByNameWithPointer to test the structure of pointer type
func TestSafeFieldByNameWithPointer(t *testing.T) {
	testObj := &TestStruct{
		Name:   "Jane Doe",
		Age:    25,
		Active: false,
	}

	// Get the value pointed to by the pointer
	v := reflect.ValueOf(testObj).Elem()

	// Test to get fields
	nameField, err := SafeFieldByName(v, "Name")
	if err != nil {
		t.Errorf("Expected no error for existing field 'Name', got: %v", err)
	}

	if nameField.String() != "Jane Doe" {
		t.Errorf("Expected field value 'Jane Doe', got: %v", nameField.String())
	}

	ageField, err := SafeFieldByName(v, "Age")
	if err != nil {
		t.Errorf("Expected no error for existing field 'Age', got: %v", err)
	}

	if ageField.Int() != 25 {
		t.Errorf("Expected field value 25, got: %v", ageField.Int())
	}
}

// TestSafeFieldByNameWithInterface Test interface type
func TestSafeFieldByNameWithInterface(t *testing.T) {
	var testInterface any = TestStruct{
		Name:  "Interface Test",
		Age:   35,
		Email: "interface@test.com",
	}

	v := reflect.ValueOf(testInterface)

	nameField, err := SafeFieldByName(v, "Name")
	if err != nil {
		t.Errorf("Expected no error for existing field 'Name', got: %v", err)
	}

	if nameField.String() != "Interface Test" {
		t.Errorf("Expected field value 'Interface Test', got: %v", nameField.String())
	}
}

// TestSafeFieldByNameEmptyStruct Tests the empty structure
func TestSafeFieldByNameEmptyStruct(t *testing.T) {
	type EmptyStruct struct{}

	emptyObj := EmptyStruct{}
	v := reflect.ValueOf(emptyObj)

	// Try to get fields that don't exist
	_, err := SafeFieldByName(v, "NonExistentField")
	if err == nil {
		t.Error("Expected error for non-existent field in empty struct, got nil")
	}

	expectedError := "field NonExistentField not found"
	if err.Error() != expectedError {
		t.Errorf("Expected error message '%s', got: %v", expectedError, err.Error())
	}
}

// TestSafeFieldByNameCaseSensitive tests field name case sensitivity
func TestSafeFieldByNameCaseSensitive(t *testing.T) {
	testObj := TestStruct{Name: "Case Test"}
	v := reflect.ValueOf(testObj)

	// Test the correct case case
	nameField, err := SafeFieldByName(v, "Name")
	if err != nil {
		t.Errorf("Expected no error for correct case 'Name', got: %v", err)
	}

	if nameField.String() != "Case Test" {
		t.Errorf("Expected field value 'Case Test', got: %v", nameField.String())
	}

	// Test for case errors
	_, err = SafeFieldByName(v, "name") // Lowercase
	if err == nil {
		t.Error("Expected error for incorrect case 'name', got nil")
	}

	_, err = SafeFieldByName(v, "NAME") // Uppercase
	if err == nil {
		t.Error("Expected error for incorrect case 'NAME', got nil")
	}
}

// TestSafeFieldByNameConcurrentAccess tests for concurrent access
func TestSafeFieldByNameConcurrentAccess(t *testing.T) {
	testObj := TestStruct{
		Name:    "Concurrent Test",
		Age:     40,
		Email:   "concurrent@test.com",
		Active:  true,
		Balance: 2000.75,
	}

	v := reflect.ValueOf(testObj)

	// Initiate multiple GoRoutine concurrent accesses
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				// Test to obtain different fields
				nameField, err := SafeFieldByName(v, "Name")
				if err != nil {
					t.Errorf("Concurrent access error for Name: %v", err)
					return
				}
				if nameField.String() != "Concurrent Test" {
					t.Errorf("Concurrent access value error for Name: expected 'Concurrent Test', got %v", nameField.String())
					return
				}

				ageField, err := SafeFieldByName(v, "Age")
				if err != nil {
					t.Errorf("Concurrent access error for Age: %v", err)
					return
				}
				if ageField.Int() != 40 {
					t.Errorf("Concurrent access value error for Age: expected 40, got %v", ageField.Int())
					return
				}
			}
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}
