package functions

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAggregatorAdapterBasic Tests the basic functions of the aggregator adapter
func TestAggregatorAdapterBasic(t *testing.T) {
	// Test the creation of aggregator adapters
	adapter, err := NewAggregatorAdapter("sum")
	require.NoError(t, err)
	require.NotNil(t, adapter)

	// Test GetFunctionName
	funcName := adapter.GetFunctionName()
	assert.Equal(t, "sum", funcName)

	// Test the new method
	newAdapter := adapter.New()
	assert.NotNil(t, newAdapter)

	// Test the Add and Result methods
	adapter.Add(10)
	adapter.Add(20)
	adapter.Add(30)
	result := adapter.Result()
	assert.Equal(t, 60.0, result)

	// Test to create aggregators that don't exist
	_, err = NewAggregatorAdapter("nonexistent")
	assert.Error(t, err)
}

// TestAggregatorAdapterWithNilFunction tests the nil function of the aggregator adapter
func TestAggregatorAdapterWithNilFunction(t *testing.T) {
	adapter := &AggregatorAdapter{aggFunc: nil}
	funcName := adapter.GetFunctionName()
	assert.Equal(t, "", funcName)
}

// TestRegisterAggregatorAdapter Tests the registration aggregator adapter
func TestRegisterAggregatorAdapter(t *testing.T) {
	// Register the aggregator adapter
	err := RegisterAggregatorAdapter("sum")
	assert.NoError(t, err)

	// Get the aggregator adapter
	constructor, exists := GetAggregatorAdapter("sum")
	assert.True(t, exists)
	assert.NotNil(t, constructor)

	// Create instances using constructors
	instance := constructor()
	assert.NotNil(t, instance)

	// Testing adapters that don't exist
	_, exists = GetAggregatorAdapter("nonexistent")
	assert.False(t, exists)
}

// TestCreateBuiltinAggregatorFromFunctions tests the built-in aggregator created from function modules
func TestCreateBuiltinAggregatorFromFunctions(t *testing.T) {
	// First, register an adapter
	err := RegisterAggregatorAdapter("sum")
	assert.NoError(t, err)

	// Created from the registered adapter
	aggregator := CreateBuiltinAggregatorFromFunctions("sum")
	assert.NotNil(t, aggregator)

	// Directly created from unregistered functions
	aggregator2 := CreateBuiltinAggregatorFromFunctions("avg")
	assert.NotNil(t, aggregator2)

	// Testing aggregators that don't exist
	aggregator3 := CreateBuiltinAggregatorFromFunctions("nonexistent")
	assert.Nil(t, aggregator3)
}

// TestAggregatorAdapterErrorHandling Tests aggregator adapter error handling
func TestAggregatorAdapterErrorHandling(t *testing.T) {
	// Register an adapter that will fail (using a non-existent function name)
	err := RegisterAggregatorAdapter("invalid_function")
	assert.NoError(t, err)

	// Obtain and attempt to create instances
	constructor, exists := GetAggregatorAdapter("invalid_function")
	assert.True(t, exists)

	// Creating an instance should return nil (because the function does not exist)
	instance := constructor()
	assert.Nil(t, instance)
}
