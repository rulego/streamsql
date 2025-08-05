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
	dp.stream.aggregator = aggregator.NewGroupAggregator(dp.stream.config.GroupFields, aggregationFields)

	// Register expression calculators
	for field, fieldExpr := range dp.stream.config.FieldExpressions {
		dp.registerExpressionCalculator(field, fieldExpr)
	}
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

	// 使用EvaluateValueWithNull来获取实际值（包括字符串）
	result, isNull, err := expr.EvaluateValueWithNull(dataMap)
	if err != nil {
		return nil, fmt.Errorf("CASE expression evaluation failed: %w", err)
	}
	if isNull {
		return nil, nil // 返回nil表示NULL值
	}
	return result, nil
}

// fallbackExpressionEvaluation 回退表达式计算
func (dp *DataProcessor) fallbackExpressionEvaluation(expression string, dataMap map[string]interface{}) (interface{}, error) {
	// 预处理反引号标识符
	exprToUse := expression
	bridge := functions.GetExprBridge()
	if bridge.ContainsBacktickIdentifiers(exprToUse) {
		if processed, err := bridge.PreprocessBacktickIdentifiers(exprToUse); err == nil {
			exprToUse = processed
		}
	}

	// 首先尝试使用桥接器处理（支持字符串拼接等）
	if result, err := bridge.EvaluateExpression(exprToUse, dataMap); err == nil {
		return result, nil
	}

	// 如果桥接器失败，回退到自定义表达式引擎
	expr, parseErr := expr.NewExpression(exprToUse)
	if parseErr != nil {
		return nil, fmt.Errorf("expression parse failed: %w", parseErr)
	}

	// 使用EvaluateValueWithNull获取实际值（包括字符串）
	result, isNull, err := expr.EvaluateValueWithNull(dataMap)
	if err != nil {
		return nil, fmt.Errorf("expression evaluation failed: %w", err)
	}
	if isNull {
		return nil, nil // 返回nil表示NULL值
	}
	return result, nil
}

// startWindowProcessing 启动窗口处理
func (dp *DataProcessor) startWindowProcessing() {
	// 启动窗口处理协程
	dp.stream.Window.Start()

	// 处理窗口模式
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

// processWindowBatch 处理窗口批数据
func (dp *DataProcessor) processWindowBatch(batch []types.Row) {
	// 处理窗口批数据
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

	// 获取并发送聚合结果
	if results, err := dp.stream.aggregator.GetResults(); err == nil {
		dp.processAggregationResults(results)
		dp.stream.aggregator.Reset()
	}
}

// processAggregationResults 处理聚合结果
func (dp *DataProcessor) processAggregationResults(results []map[string]interface{}) {
	var finalResults []map[string]interface{}

	// 处理DISTINCT
	if dp.stream.config.Distinct {
		finalResults = dp.applyDistinct(results)
	} else {
		finalResults = results
	}

	// 应用 HAVING 过滤条件
	if dp.stream.config.Having != "" {
		finalResults = dp.applyHavingFilter(finalResults)
	}

	// 应用 LIMIT 限制
	if dp.stream.config.Limit > 0 && len(finalResults) > dp.stream.config.Limit {
		finalResults = finalResults[:dp.stream.config.Limit]
	}

	// 发送结果到结果通道和 Sink 函数
	if len(finalResults) > 0 {
		// 非阻塞发送到结果通道
		dp.stream.sendResultNonBlocking(finalResults)

		// 异步调用所有sinks
		dp.stream.callSinksAsync(finalResults)
	}
}

// applyDistinct 应用DISTINCT去重
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

// applyHavingFilter 应用HAVING过滤
func (dp *DataProcessor) applyHavingFilter(results []map[string]interface{}) []map[string]interface{} {
	// 检查HAVING条件是否包含CASE表达式
	hasCaseExpression := strings.Contains(strings.ToUpper(dp.stream.config.Having), SQLKeywordCase)

	var filteredResults []map[string]interface{}

	if hasCaseExpression {
		filteredResults = dp.applyHavingWithCaseExpression(results)
	} else {
		filteredResults = dp.applyHavingWithCondition(results)
	}

	return filteredResults
}

// applyHavingWithCaseExpression 使用CASE表达式应用HAVING过滤
func (dp *DataProcessor) applyHavingWithCaseExpression(results []map[string]interface{}) []map[string]interface{} {
	// HAVING条件包含CASE表达式，使用我们的表达式解析器
	// 预处理反引号标识符
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
	// 应用 HAVING 过滤，使用CASE表达式计算器
	for _, result := range results {
		// 使用EvaluateValueWithNull方法以支持NULL值处理
		havingResult, isNull, err := expression.EvaluateValueWithNull(result)
		if err != nil {
			logger.Error("having filter evaluation error: %v", err)
			continue
		}

		// 如果结果是NULL，则不满足条件（SQL标准行为）
		if isNull {
			continue
		}

		// 对于数值结果，大于0视为true（满足HAVING条件）
		// 对于字符串结果，非空视为true
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
				// 其他类型，非nil视为true
				filteredResults = append(filteredResults, result)
			}
		}
	}

	return filteredResults
}

// applyHavingWithCondition 使用条件表达式应用HAVING过滤
func (dp *DataProcessor) applyHavingWithCondition(results []map[string]interface{}) []map[string]interface{} {
	// HAVING条件不包含CASE表达式，使用原有的expr-lang处理
	// 预处理HAVING条件中的LIKE语法，转换为expr-lang可理解的形式
	processedHaving := dp.stream.config.Having
	bridge := functions.GetExprBridge()
	if bridge.ContainsLikeOperator(dp.stream.config.Having) {
		if processed, err := bridge.PreprocessLikeExpression(dp.stream.config.Having); err == nil {
			processedHaving = processed
		}
	}

	// 预处理HAVING条件中的IS NULL语法
	if bridge.ContainsIsNullOperator(processedHaving) {
		if processed, err := bridge.PreprocessIsNullExpression(processedHaving); err == nil {
			processedHaving = processed
		}
	}

	// 创建 HAVING 条件
	havingFilter, err := condition.NewExprCondition(processedHaving)
	if err != nil {
		logger.Error("having filter error: %v", err)
		return results
	}

	var filteredResults []map[string]interface{}
	// 应用 HAVING 过滤
	for _, result := range results {
		if havingFilter.Evaluate(result) {
			filteredResults = append(filteredResults, result)
		}
	}

	return filteredResults
}

// processDirectData 直接处理非窗口数据
// 参数:
//   - data: 要处理的数据，必须是map[string]interface{}类型
func (dp *DataProcessor) processDirectData(data map[string]interface{}) {
	// 直接使用传入的map数据
	dataMap := data

	// 创建结果map，预分配合适容量
	estimatedSize := len(dp.stream.config.FieldExpressions) + len(dp.stream.config.SimpleFields)
	if estimatedSize < 8 {
		estimatedSize = 8 // 最小容量
	}
	result := make(map[string]interface{}, estimatedSize)

	// 处理表达式字段（使用预编译信息）
	for fieldName := range dp.stream.config.FieldExpressions {
		dp.stream.processExpressionField(fieldName, dataMap, result)
	}

	// 使用预编译的字段信息处理SimpleFields
	if len(dp.stream.config.SimpleFields) > 0 {
		for _, fieldSpec := range dp.stream.config.SimpleFields {
			dp.stream.processSimpleField(fieldSpec, dataMap, dataMap, result)
		}
	} else if len(dp.stream.config.FieldExpressions) == 0 {
		// 如果没有指定字段且没有表达式字段，保留所有字段
		for k, v := range dataMap {
			result[k] = v
		}
	}

	// 将结果包装为数组
	results := []map[string]interface{}{result}

	// 非阻塞发送结果到resultChan
	dp.stream.sendResultNonBlocking(results)

	// 异步调用所有sinks，避免阻塞
	dp.stream.callSinksAsync(results)
}
