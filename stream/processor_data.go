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

package stream

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/condition"
	"github.com/rulego/streamsql/expr"
	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/types"
)

// DataProcessor data processor responsible for processing data streams
type DataProcessor struct {
	stream *Stream
}

// NewDataProcessor creates a data processor
func NewDataProcessor(stream *Stream) *DataProcessor {
	return &DataProcessor{stream: stream}
}

// Process main processing loop
func (dp *DataProcessor) Process() {
	// Initialize aggregator for window mode
	if dp.stream.config.NeedWindow {
		dp.initializeAggregator()
		dp.startWindowProcessing()
	}

	// Create a timer to avoid creating multiple temporary timers causing resource leaks
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop() // Ensure timer is stopped when function exits

	// Main processing loop
	for {
		// Safely access dataChan using read lock
		dp.stream.dataChanMux.RLock()
		currentDataChan := dp.stream.dataChan
		dp.stream.dataChanMux.RUnlock()

		select {
		case data, ok := <-currentDataChan:
			if !ok {
				// Channel is closed
				return
			}
			// Apply filter conditions
			if dp.stream.filter == nil || dp.stream.filter.Evaluate(data) {
				if dp.stream.config.NeedWindow {
					// Window mode, add data to window
					dp.stream.Window.Add(data)
				} else {
					// Non-window mode, process data directly and output
					dp.processDirectData(data)
				}
			}
		case <-dp.stream.done:
			// Received close signal
			return
		case <-ticker.C:
			// Timer triggered, do nothing, just prevent CPU spinning
		}
	}
}

// initializeAggregator initializes the aggregator
func (dp *DataProcessor) initializeAggregator() {
	// Convert to new AggregationField format
	aggregationFields := convertToAggregationFields(dp.stream.config.SelectFields, dp.stream.config.FieldAlias)

	// Check if we have post-aggregation expressions
	if len(dp.stream.config.PostAggExpressions) > 0 {
		// Use enhanced aggregator for post-aggregation support
		enhancedAgg := aggregator.NewEnhancedGroupAggregator(dp.stream.config.GroupFields, aggregationFields)

		// Add post-aggregation expressions
		for _, postExpr := range dp.stream.config.PostAggExpressions {
			err := enhancedAgg.AddPostAggregationExpression(
				postExpr.OutputField,
				postExpr.OriginalExpr,
				convertToAggregationFieldInfos(postExpr.RequiredFields),
			)
			if err != nil {
				// Log error but continue
				fmt.Printf("Error adding post-aggregation expression %s: %v\n", postExpr.OriginalExpr, err)
			}
		}

		dp.stream.aggregator = enhancedAgg
	} else {
		// Use regular aggregator
		dp.stream.aggregator = aggregator.NewGroupAggregator(dp.stream.config.GroupFields, aggregationFields)
	}

	// Register expression calculators
	for field, fieldExpr := range dp.stream.config.FieldExpressions {
		dp.registerExpressionCalculator(field, fieldExpr)
	}
}

// convertToAggregationFieldInfos converts types.AggregationFieldInfo to aggregator.AggregationFieldInfo
func convertToAggregationFieldInfos(fields []types.AggregationFieldInfo) []aggregator.AggregationFieldInfo {
	result := make([]aggregator.AggregationFieldInfo, len(fields))
	for i, field := range fields {
		result[i] = aggregator.AggregationFieldInfo{
			FuncName:    field.FuncName,
			InputField:  field.InputField,
			Placeholder: field.Placeholder,
			AggType:     field.AggType,
			FullCall:    field.FullCall, // 保持FullCall字段
		}
	}
	return result
}

// registerExpressionCalculator registers expression calculator
func (dp *DataProcessor) registerExpressionCalculator(field string, fieldExpr types.FieldExpression) {
	// Create local variables to avoid closure issues
	currentField := field
	currentFieldExpr := fieldExpr

	// Register expression calculator
	dp.stream.aggregator.RegisterExpression(
		currentField,
		currentFieldExpr.Expression,
		currentFieldExpr.Fields,
		func(data interface{}) (interface{}, error) {
			// Ensure data is map[string]interface{} type
			if dataMap, ok := data.(map[string]interface{}); ok {
				return dp.evaluateExpressionForAggregation(currentFieldExpr, dataMap)
			}
			return nil, fmt.Errorf("unsupported data type: %T, expected map[string]interface{}", data)
		},
	)
}

// evaluateExpressionForAggregation evaluates expression for aggregation
// Parameters:
//   - fieldExpr: field expression
//   - data: data to process, must be map[string]interface{} type
func (dp *DataProcessor) evaluateExpressionForAggregation(fieldExpr types.FieldExpression, data map[string]interface{}) (interface{}, error) {
	// Directly use the passed map data
	dataMap := data

	// Check if expression contains nested fields, if so use custom expression engine directly
	hasNestedFields := strings.Contains(fieldExpr.Expression, ".")

	if hasNestedFields {
		return dp.evaluateNestedFieldExpression(fieldExpr.Expression, dataMap)
	}

	// Check if it's a CASE expression
	trimmedExpr := strings.TrimSpace(fieldExpr.Expression)
	upperExpr := strings.ToUpper(trimmedExpr)
	if strings.HasPrefix(upperExpr, SQLKeywordCase) {
		return dp.evaluateCaseExpression(fieldExpr.Expression, dataMap)
	}

	// Use bridge to evaluate expression, supporting string concatenation and IS NULL syntax
	bridge := functions.GetExprBridge()

	// Preprocess IS NULL and LIKE syntax in expression
	processedExpr := fieldExpr.Expression
	if bridge.ContainsIsNullOperator(processedExpr) {
		if processed, err := bridge.PreprocessIsNullExpression(processedExpr); err == nil {
			processedExpr = processed
		}
	}
	if bridge.ContainsLikeOperator(processedExpr) {
		if processed, err := bridge.PreprocessLikeExpression(processedExpr); err == nil {
			processedExpr = processed
		}
	}

	result, err := bridge.EvaluateExpression(processedExpr, dataMap)
	if err != nil {
		// If bridge fails, fallback to original expression engine
		return dp.fallbackExpressionEvaluation(fieldExpr.Expression, dataMap)
	}

	return result, nil
}

// convertToDataMap converts data to map format
// convertToDataMap method has been removed, please use github.com/rulego/streamsql/utils/converter.ToDataMap function instead

// evaluateNestedFieldExpression evaluates nested field expression
func (dp *DataProcessor) evaluateNestedFieldExpression(expression string, dataMap map[string]interface{}) (interface{}, error) {
	// Directly use custom expression engine to handle nested fields, supporting NULL values
	// Preprocess backtick identifiers
	exprToUse := expression
	bridge := functions.GetExprBridge()
	if bridge.ContainsBacktickIdentifiers(exprToUse) {
		if processed, err := bridge.PreprocessBacktickIdentifiers(exprToUse); err == nil {
			exprToUse = processed
		}
	}
	expr, parseErr := expr.NewExpression(exprToUse)
	if parseErr != nil {
		return nil, fmt.Errorf("expression parse failed: %w", parseErr)
	}

	// Use EvaluateValueWithNull to get actual values (including strings)
	result, isNull, err := expr.EvaluateValueWithNull(dataMap)
	if err != nil {
		return nil, fmt.Errorf("expression evaluation failed: %w", err)
	}
	if isNull {
		return nil, nil // Return nil to represent NULL value
	}
	return result, nil
}

// evaluateCaseExpression evaluates CASE expression
func (dp *DataProcessor) evaluateCaseExpression(expression string, dataMap map[string]interface{}) (interface{}, error) {
	// CASE expression uses NULL-supporting evaluation method
	// Preprocess backtick identifiers
	exprToUse := expression
	bridge := functions.GetExprBridge()
	if bridge.ContainsBacktickIdentifiers(exprToUse) {
		if processed, err := bridge.PreprocessBacktickIdentifiers(exprToUse); err == nil {
			exprToUse = processed
		}
	}
	expr, parseErr := expr.NewExpression(exprToUse)
	if parseErr != nil {
		return nil, fmt.Errorf("CASE expression parse failed: %w", parseErr)
	}

	// Use EvaluateValueWithNull to get actual value (including strings)
	result, isNull, err := expr.EvaluateValueWithNull(dataMap)
	if err != nil {
		return nil, fmt.Errorf("CASE expression evaluation failed: %w", err)
	}
	if isNull {
		return nil, nil // Return nil to indicate NULL value
	}
	return result, nil
}

// fallbackExpressionEvaluation fallback expression evaluation
func (dp *DataProcessor) fallbackExpressionEvaluation(expression string, dataMap map[string]interface{}) (interface{}, error) {
	// Preprocess backtick identifiers
	exprToUse := expression
	bridge := functions.GetExprBridge()
	if bridge.ContainsBacktickIdentifiers(exprToUse) {
		if processed, err := bridge.PreprocessBacktickIdentifiers(exprToUse); err == nil {
			exprToUse = processed
		}
	}

	// First try using bridge processor (supports string concatenation etc.)
	if result, err := bridge.EvaluateExpression(exprToUse, dataMap); err == nil {
		return result, nil
	}

	// If bridge fails, fallback to custom expression engine
	expr, parseErr := expr.NewExpression(exprToUse)
	if parseErr != nil {
		return nil, fmt.Errorf("expression parse failed: %w", parseErr)
	}

	// Use EvaluateValueWithNull to get actual value (including strings)
	result, isNull, err := expr.EvaluateValueWithNull(dataMap)
	if err != nil {
		return nil, fmt.Errorf("expression evaluation failed: %w", err)
	}
	if isNull {
		return nil, nil // Return nil to indicate NULL value
	}
	return result, nil
}

// startWindowProcessing starts window processing
func (dp *DataProcessor) startWindowProcessing() {
	// Start window processing goroutine
	dp.stream.Window.Start()

	// Process window mode
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("Window processing goroutine panic recovered: %v", r)
			}
		}()

		for batch := range dp.stream.Window.OutputChan() {
			dp.processWindowBatch(batch)
		}
	}()
}

// processWindowBatch processes window batch data
func (dp *DataProcessor) processWindowBatch(batch []types.Row) {
	// Process window batch data
	for _, item := range batch {
		if err := dp.stream.aggregator.Put(WindowStartField, item.Slot.WindowStart()); err != nil {
			logger.Error("failed to put window start: %v", err)
		}
		if err := dp.stream.aggregator.Put(WindowEndField, item.Slot.WindowEnd()); err != nil {
			logger.Error("failed to put window end: %v", err)
		}
		if err := dp.stream.aggregator.Add(item.Data); err != nil {
			logger.Error("aggregate error: %v", err)
		}
	}

	// Get and send aggregation results
	if results, err := dp.stream.aggregator.GetResults(); err == nil {
		dp.processAggregationResults(results)
		dp.stream.aggregator.Reset()
	}
}

// processAggregationResults processes aggregation results
func (dp *DataProcessor) processAggregationResults(results []map[string]interface{}) {
	var finalResults []map[string]interface{}

	// Process DISTINCT
	if dp.stream.config.Distinct {
		finalResults = dp.applyDistinct(results)
	} else {
		finalResults = results
	}

	// Apply HAVING filter condition
	if dp.stream.config.Having != "" {
		finalResults = dp.applyHavingFilter(finalResults)
	}

	// Apply LIMIT restriction
	if dp.stream.config.Limit > 0 && len(finalResults) > dp.stream.config.Limit {
		finalResults = finalResults[:dp.stream.config.Limit]
	}

	// Send results to result channel and Sink functions
	if len(finalResults) > 0 {
		// Non-blocking send to result channel
		dp.stream.sendResultNonBlocking(finalResults)

		// Asynchronously call all sinks
		dp.stream.callSinksAsync(finalResults)
	}
}

// applyDistinct applies DISTINCT deduplication
func (dp *DataProcessor) applyDistinct(results []map[string]interface{}) []map[string]interface{} {
	seenResults := make(map[string]bool)
	var finalResults []map[string]interface{}

	for _, result := range results {
		serializedResult, jsonErr := json.Marshal(result)
		if jsonErr != nil {
			logger.Error("Error serializing result for distinct check: %v", jsonErr)
			finalResults = append(finalResults, result)
			continue
		}
		if !seenResults[string(serializedResult)] {
			finalResults = append(finalResults, result)
			seenResults[string(serializedResult)] = true
		}
	}

	return finalResults
}

// applyHavingFilter applies HAVING filter
func (dp *DataProcessor) applyHavingFilter(results []map[string]interface{}) []map[string]interface{} {
	// Check if HAVING condition contains CASE expression
	hasCaseExpression := strings.Contains(strings.ToUpper(dp.stream.config.Having), SQLKeywordCase)

	var filteredResults []map[string]interface{}

	if hasCaseExpression {
		filteredResults = dp.applyHavingWithCaseExpression(results)
	} else {
		filteredResults = dp.applyHavingWithCondition(results)
	}

	return filteredResults
}

// applyHavingWithCaseExpression applies HAVING filter using CASE expression
func (dp *DataProcessor) applyHavingWithCaseExpression(results []map[string]interface{}) []map[string]interface{} {
	// HAVING condition contains CASE expression, use our expression parser
	// Preprocess backtick identifiers
	exprToUse := dp.stream.config.Having
	bridge := functions.GetExprBridge()
	if bridge.ContainsBacktickIdentifiers(exprToUse) {
		if processed, err := bridge.PreprocessBacktickIdentifiers(exprToUse); err == nil {
			exprToUse = processed
		}
	}
	expression, err := expr.NewExpression(exprToUse)
	if err != nil {
		logger.Error("having filter error (CASE expression): %v", err)
		return results
	}

	var filteredResults []map[string]interface{}
	// Apply HAVING filter using CASE expression calculator
	for _, result := range results {
		// Use EvaluateValueWithNull method to support NULL value processing
		havingResult, isNull, err := expression.EvaluateValueWithNull(result)
		if err != nil {
			logger.Error("having filter evaluation error: %v", err)
			continue
		}

		// If result is NULL, condition is not satisfied (SQL standard behavior)
		if isNull {
			continue
		}

		// For numeric results, greater than 0 is considered true (satisfies HAVING condition)
		// For string results, non-empty is considered true
		if havingResult != nil {
			if numResult, ok := havingResult.(float64); ok {
				if numResult > 0 {
					filteredResults = append(filteredResults, result)
				}
			} else if strResult, ok := havingResult.(string); ok {
				if strResult != "" {
					filteredResults = append(filteredResults, result)
				}
			} else {
				// Other types, non-nil is considered true
				filteredResults = append(filteredResults, result)
			}
		}
	}

	return filteredResults
}

// applyHavingWithCondition applies HAVING filter using condition expression
func (dp *DataProcessor) applyHavingWithCondition(results []map[string]interface{}) []map[string]interface{} {
	// HAVING condition doesn't contain CASE expression, use original expr-lang processing
	// Preprocess LIKE syntax in HAVING condition, convert to expr-lang understandable form
	processedHaving := dp.stream.config.Having
	bridge := functions.GetExprBridge()
	if bridge.ContainsLikeOperator(dp.stream.config.Having) {
		if processed, err := bridge.PreprocessLikeExpression(dp.stream.config.Having); err == nil {
			processedHaving = processed
		}
	}

	// Preprocess IS NULL syntax in HAVING condition
	if bridge.ContainsIsNullOperator(processedHaving) {
		if processed, err := bridge.PreprocessIsNullExpression(processedHaving); err == nil {
			processedHaving = processed
		}
	}

	// Create HAVING condition
	havingFilter, err := condition.NewExprCondition(processedHaving)
	if err != nil {
		logger.Error("having filter error: %v", err)
		return results
	}

	var filteredResults []map[string]interface{}
	// Apply HAVING filter
	for _, result := range results {
		if havingFilter.Evaluate(result) {
			filteredResults = append(filteredResults, result)
		}
	}

	return filteredResults
}

// processDirectData directly processes non-window data
// Parameters:
//   - data: data to be processed, must be map[string]interface{} type
func (dp *DataProcessor) processDirectData(data map[string]interface{}) {
	// Directly use the passed map data
	dataMap := data

	// Create result map, pre-allocate appropriate capacity
	estimatedSize := len(dp.stream.config.FieldExpressions) + len(dp.stream.config.SimpleFields)
	if estimatedSize < 8 {
		estimatedSize = 8 // Minimum capacity
	}
	result := make(map[string]interface{}, estimatedSize)

	// Process expression fields (using pre-compiled information)
	for fieldName := range dp.stream.config.FieldExpressions {
		dp.stream.processExpressionField(fieldName, dataMap, result)
	}

	// Use pre-compiled field information to process SimpleFields
	if len(dp.stream.config.SimpleFields) > 0 {
		for _, fieldSpec := range dp.stream.config.SimpleFields {
			dp.stream.processSimpleField(fieldSpec, dataMap, dataMap, result)
		}
	} else if len(dp.stream.config.FieldExpressions) == 0 {
		// If no fields specified and no expression fields, keep all fields
		for k, v := range dataMap {
			result[k] = v
		}
	}

	// Wrap result as array
	results := []map[string]interface{}{result}

	// Non-blocking send result to resultChan
	dp.stream.sendResultNonBlocking(results)

	// Asynchronously call all sinks, avoid blocking
	dp.stream.callSinksAsync(results)
}
