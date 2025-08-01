package stream

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/condition"
	"github.com/rulego/streamsql/expr"
	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/types"
)

// DataProcessor 数据处理器，负责处理数据流
type DataProcessor struct {
	stream *Stream
}

// NewDataProcessor 创建数据处理器
func NewDataProcessor(stream *Stream) *DataProcessor {
	return &DataProcessor{stream: stream}
}

// Process 主处理循环
func (dp *DataProcessor) Process() {
	// 初始化聚合器，用于窗口模式
	if dp.stream.config.NeedWindow {
		dp.initializeAggregator()
		dp.startWindowProcessing()
	}

	// 创建一个定时器，避免创建多个临时定时器导致资源泄漏
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop() // 确保在函数退出时停止定时器

	// 主处理循环
	for {
		// 使用读锁安全访问dataChan
		dp.stream.dataChanMux.RLock()
		currentDataChan := dp.stream.dataChan
		dp.stream.dataChanMux.RUnlock()

		select {
		case data, ok := <-currentDataChan:
			if !ok {
				// 通道已关闭
				return
			}
			// 应用过滤条件
			if dp.stream.filter == nil || dp.stream.filter.Evaluate(data) {
				if dp.stream.config.NeedWindow {
					// 窗口模式，添加数据到窗口
					dp.stream.Window.Add(data)
				} else {
					// 非窗口模式，直接处理数据并输出
					dp.processDirectData(data)
				}
			}
		case <-dp.stream.done:
			// 收到关闭信号
			return
		case <-ticker.C:
			// 定时器触发，什么都不做，只是防止 CPU 空转
		}
	}
}

// initializeAggregator 初始化聚合器
func (dp *DataProcessor) initializeAggregator() {
	// 转换为新的AggregationField格式
	aggregationFields := convertToAggregationFields(dp.stream.config.SelectFields, dp.stream.config.FieldAlias)
	dp.stream.aggregator = aggregator.NewGroupAggregator(dp.stream.config.GroupFields, aggregationFields)

	// 注册表达式计算器
	for field, fieldExpr := range dp.stream.config.FieldExpressions {
		dp.registerExpressionCalculator(field, fieldExpr)
	}
}

// registerExpressionCalculator 注册表达式计算器
func (dp *DataProcessor) registerExpressionCalculator(field string, fieldExpr types.FieldExpression) {
	// 创建局部变量避免闭包问题
	currentField := field
	currentFieldExpr := fieldExpr

	// 注册表达式计算器
	dp.stream.aggregator.RegisterExpression(
		currentField,
		currentFieldExpr.Expression,
		currentFieldExpr.Fields,
		func(data interface{}) (interface{}, error) {
			return dp.evaluateExpressionForAggregation(currentFieldExpr, data)
		},
	)
}

// evaluateExpressionForAggregation 为聚合计算表达式
func (dp *DataProcessor) evaluateExpressionForAggregation(fieldExpr types.FieldExpression, data interface{}) (interface{}, error) {
	// 将数据转换为 map[string]interface{} 以便计算
	dataMap, err := dp.convertToDataMap(data)
	if err != nil {
		return nil, err
	}

	// 检查表达式是否包含嵌套字段，如果有则直接使用自定义表达式引擎
	hasNestedFields := strings.Contains(fieldExpr.Expression, ".")

	if hasNestedFields {
		return dp.evaluateNestedFieldExpression(fieldExpr.Expression, dataMap)
	}

	// 检查是否为CASE表达式
	trimmedExpr := strings.TrimSpace(fieldExpr.Expression)
	upperExpr := strings.ToUpper(trimmedExpr)
	if strings.HasPrefix(upperExpr, SQLKeywordCase) {
		return dp.evaluateCaseExpression(fieldExpr.Expression, dataMap)
	}

	// 使用桥接器计算表达式，支持字符串拼接和IS NULL等语法
	bridge := functions.GetExprBridge()

	// 预处理表达式中的IS NULL和LIKE语法
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
		// 如果桥接器失败，回退到原来的表达式引擎
		return dp.fallbackExpressionEvaluation(fieldExpr.Expression, dataMap)
	}

	return result, nil
}

// convertToDataMap 将数据转换为map格式
func (dp *DataProcessor) convertToDataMap(data interface{}) (map[string]interface{}, error) {
	switch d := data.(type) {
	case map[string]interface{}:
		return d, nil
	default:
		// 如果不是 map，尝试转换
		v := reflect.ValueOf(data)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		if v.Kind() == reflect.Struct {
			// 将结构体转换为 map
			dataMap := make(map[string]interface{})
			t := v.Type()
			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i)
				dataMap[field.Name] = v.Field(i).Interface()
			}
			return dataMap, nil
		} else {
			return nil, fmt.Errorf("unsupported data type for expression: %T", data)
		}
	}
}

// evaluateNestedFieldExpression 计算嵌套字段表达式
func (dp *DataProcessor) evaluateNestedFieldExpression(expression string, dataMap map[string]interface{}) (interface{}, error) {
	// 直接使用自定义表达式引擎处理嵌套字段，支持NULL值
	// 预处理反引号标识符
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

	// 使用支持NULL的计算方法
	numResult, isNull, err := expr.EvaluateWithNull(dataMap)
	if err != nil {
		return nil, fmt.Errorf("expression evaluation failed: %w", err)
	}
	if isNull {
		return nil, nil // 返回nil表示NULL值
	}
	return numResult, nil
}

// evaluateCaseExpression 计算CASE表达式
func (dp *DataProcessor) evaluateCaseExpression(expression string, dataMap map[string]interface{}) (interface{}, error) {
	// CASE表达式使用支持NULL的计算方法
	// 预处理反引号标识符
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

	numResult, isNull, err := expr.EvaluateWithNull(dataMap)
	if err != nil {
		return nil, fmt.Errorf("CASE expression evaluation failed: %w", err)
	}
	if isNull {
		return nil, nil // 返回nil表示NULL值
	}
	return numResult, nil
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
	expr, parseErr := expr.NewExpression(exprToUse)
	if parseErr != nil {
		return nil, fmt.Errorf("expression parse failed: %w", parseErr)
	}

	// 计算表达式，支持NULL值
	numResult, isNull, err := expr.EvaluateWithNull(dataMap)
	if err != nil {
		return nil, fmt.Errorf("expression evaluation failed: %w", err)
	}
	if isNull {
		return nil, nil // 返回nil表示NULL值
	}
	return numResult, nil
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
		// 使用EvaluateWithNull方法以支持NULL值处理
		havingResult, isNull, err := expression.EvaluateWithNull(result)
		if err != nil {
			logger.Error("having filter evaluation error: %v", err)
			continue
		}

		// 如果结果是NULL，则不满足条件（SQL标准行为）
		if isNull {
			continue
		}

		// 对于数值结果，大于0视为true（满足HAVING条件）
		if havingResult > 0 {
			filteredResults = append(filteredResults, result)
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
func (dp *DataProcessor) processDirectData(data interface{}) {
	// 直接将数据作为map处理
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		logger.Error("Unsupported data type: %T", data)
		atomic.AddInt64(&dp.stream.droppedCount, 1)
		return
	}

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
			dp.stream.processSimpleField(fieldSpec, dataMap, data, result)
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
