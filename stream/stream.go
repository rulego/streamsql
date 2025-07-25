package stream

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rulego/streamsql/condition"
	"github.com/rulego/streamsql/utils/fieldpath"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/expr"
	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/window"
)

// fieldProcessInfo 字段处理信息，用于缓存预编译的字段处理逻辑
type fieldProcessInfo struct {
	fieldName      string // 原始字段名
	outputName     string // 输出字段名
	isFunctionCall bool   // 是否为函数调用
	hasNestedField bool   // 是否包含嵌套字段
	isSelectAll    bool   // 是否为SELECT *
}

type Stream struct {
	dataChan       chan interface{}
	filter         condition.Condition
	Window         window.Window
	aggregator     aggregator.Aggregator
	config         types.Config
	sinks          []func(interface{})
	resultChan     chan interface{} // 结果通道
	seenResults    *sync.Map
	done           chan struct{} // 用于关闭处理协程
	sinkWorkerPool chan func()   // Sink工作池，避免阻塞

	// 新增：线程安全控制
	dataChanMux      sync.RWMutex // 保护dataChan访问的读写锁
	sinksMux         sync.RWMutex // 保护sinks访问的读写锁
	expansionMux     sync.Mutex   // 防止并发扩容的互斥锁
	retryMux         sync.Mutex   // 控制持久化重试的互斥锁
	expanding        int32        // 扩容状态标记，使用原子操作
	activeRetries    int32        // 活跃重试计数，使用原子操作
	maxRetryRoutines int32        // 最大重试协程数限制

	// 性能监控指标
	inputCount   int64 // 输入数据计数
	outputCount  int64 // 输出结果计数
	droppedCount int64 // 丢弃数据计数

	// 数据丢失策略配置
	allowDataDrop      bool                // 是否允许数据丢失
	blockingTimeout    time.Duration       // 阻塞超时时间
	overflowStrategy   string              // 溢出策略: "drop", "block", "expand", "persist"
	persistenceManager *PersistenceManager // 持久化管理器

	// 性能优化：预编译的AddData函数指针，避免每次switch判断
	addDataFunc func(interface{}) // 根据策略预设的函数指针

	// 性能优化：预编译字段处理信息，避免重复解析
	compiledFieldInfo map[string]*fieldProcessInfo // 字段处理信息缓存

}

// NewStream 使用统一配置创建Stream
func NewStream(config types.Config) (*Stream, error) {
	// 如果没有指定性能配置，使用默认配置
	if (config.PerformanceConfig == types.PerformanceConfig{}) {
		config.PerformanceConfig = types.DefaultPerformanceConfig()
	}

	return newStreamWithUnifiedConfig(config)
}

// NewStreamWithHighPerformance 创建高性能Stream
func NewStreamWithHighPerformance(config types.Config) (*Stream, error) {
	config.PerformanceConfig = types.HighPerformanceConfig()
	return newStreamWithUnifiedConfig(config)
}

// NewStreamWithLowLatency 创建低延迟Stream
func NewStreamWithLowLatency(config types.Config) (*Stream, error) {
	config.PerformanceConfig = types.LowLatencyConfig()
	return newStreamWithUnifiedConfig(config)
}

// NewStreamWithZeroDataLoss 创建零数据丢失Stream
func NewStreamWithZeroDataLoss(config types.Config) (*Stream, error) {
	config.PerformanceConfig = types.ZeroDataLossConfig()
	return newStreamWithUnifiedConfig(config)
}

// NewStreamWithCustomPerformance 创建自定义性能配置的Stream
func NewStreamWithCustomPerformance(config types.Config, perfConfig types.PerformanceConfig) (*Stream, error) {
	config.PerformanceConfig = perfConfig
	return newStreamWithUnifiedConfig(config)
}

// newStreamWithUnifiedConfig 使用统一配置创建Stream的内部实现
func newStreamWithUnifiedConfig(config types.Config) (*Stream, error) {
	var win window.Window
	var err error

	// 只有在需要窗口时才创建窗口
	if config.NeedWindow {
		// 将统一的性能配置传递给窗口
		windowConfig := config.WindowConfig
		if windowConfig.Params == nil {
			windowConfig.Params = make(map[string]interface{})
		}
		// 传递完整的性能配置给窗口
		windowConfig.Params["performanceConfig"] = config.PerformanceConfig

		win, err = window.CreateWindow(windowConfig)
		if err != nil {
			return nil, err
		}
	}

	// 使用统一配置创建Stream
	perfConfig := config.PerformanceConfig
	stream := &Stream{
		dataChan:         make(chan interface{}, perfConfig.BufferConfig.DataChannelSize),
		config:           config,
		Window:           win,
		resultChan:       make(chan interface{}, perfConfig.BufferConfig.ResultChannelSize),
		seenResults:      &sync.Map{},
		done:             make(chan struct{}),
		sinkWorkerPool:   make(chan func(), perfConfig.WorkerConfig.SinkPoolSize),
		allowDataDrop:    perfConfig.OverflowConfig.AllowDataLoss,
		blockingTimeout:  perfConfig.OverflowConfig.BlockTimeout,
		overflowStrategy: perfConfig.OverflowConfig.Strategy,
		maxRetryRoutines: int32(perfConfig.WorkerConfig.MaxRetryRoutines),
	}

	// 如果是持久化策略，初始化持久化管理器
	if perfConfig.OverflowConfig.Strategy == "persist" && perfConfig.OverflowConfig.PersistenceConfig != nil {
		persistConfig := perfConfig.OverflowConfig.PersistenceConfig
		stream.persistenceManager = NewPersistenceManagerWithConfig(
			persistConfig.DataDir,
			persistConfig.MaxFileSize,
			persistConfig.FlushInterval,
		)
		if err := stream.persistenceManager.Start(); err != nil {
			return nil, fmt.Errorf("failed to start persistence manager: %w", err)
		}
	}

	// 性能优化：根据溢出策略预设AddData函数指针，避免运行时switch判断
	switch perfConfig.OverflowConfig.Strategy {
	case "block":
		stream.addDataFunc = stream.addDataBlocking
	case "expand":
		stream.addDataFunc = stream.addDataWithExpansion
	case "persist":
		stream.addDataFunc = stream.addDataWithPersistence
	default:
		stream.addDataFunc = stream.addDataWithDrop
	}

	// 性能优化：预编译字段处理信息
	stream.compileFieldProcessInfo()

	// 启动工作协程，使用配置的工作线程数
	go stream.startSinkWorkerPool(perfConfig.WorkerConfig.SinkWorkerCount)
	go stream.startResultConsumer()

	return stream, nil
}

// startSinkWorkerPool 启动Sink工作池，支持配置工作线程数
func (s *Stream) startSinkWorkerPool(workerCount int) {
	// 使用配置的工作线程数
	if workerCount <= 0 {
		workerCount = 8 // 默认值
	}

	for i := 0; i < workerCount; i++ {
		go func(workerID int) {
			for {
				select {
				case task := <-s.sinkWorkerPool:
					// 执行sink任务
					func() {
						defer func() {
							// 增强错误恢复，防止单个worker崩溃
							if r := recover(); r != nil {
								logger.Error("Sink worker %d panic recovered: %v", workerID, r)
							}
						}()
						task()
					}()
				case <-s.done:
					return
				}
			}
		}(i)
	}
}

// startResultConsumer 启动自动结果消费者，防止resultChan阻塞
func (s *Stream) startResultConsumer() {
	for {
		select {
		case <-s.resultChan:
			// 自动消费结果，防止通道阻塞
			// 这是一个保底机制，确保即使没有外部消费者，系统也不会阻塞
		case <-s.done:
			return
		}
	}
}

func (s *Stream) RegisterFilter(conditionStr string) error {
	if strings.TrimSpace(conditionStr) == "" {
		return nil
	}

	// 预处理LIKE语法，转换为expr-lang可理解的形式
	processedCondition := conditionStr
	bridge := functions.GetExprBridge()
	if bridge.ContainsLikeOperator(conditionStr) {
		if processed, err := bridge.PreprocessLikeExpression(conditionStr); err == nil {
			processedCondition = processed
		}
	}

	// 预处理IS NULL和IS NOT NULL语法
	if bridge.ContainsIsNullOperator(processedCondition) {
		if processed, err := bridge.PreprocessIsNullExpression(processedCondition); err == nil {
			processedCondition = processed
		}
	}

	filter, err := condition.NewExprCondition(processedCondition)
	if err != nil {
		return fmt.Errorf("compile filter error: %w", err)
	}
	s.filter = filter
	return nil
}

// convertToAggregationFields 将旧格式的配置转换为新的AggregationField格式
func convertToAggregationFields(selectFields map[string]aggregator.AggregateType, fieldAlias map[string]string) []aggregator.AggregationField {
	var fields []aggregator.AggregationField

	for outputAlias, aggType := range selectFields {
		field := aggregator.AggregationField{
			AggregateType: aggType,
			OutputAlias:   outputAlias,
		}

		// 查找对应的输入字段名
		if inputField, exists := fieldAlias[outputAlias]; exists {
			field.InputField = inputField
		} else {
			// 如果没有别名映射，假设输入字段名等于输出别名
			field.InputField = outputAlias
		}

		fields = append(fields, field)
	}

	return fields
}

// compileFieldProcessInfo 预编译字段处理信息，避免运行时重复解析
func (s *Stream) compileFieldProcessInfo() {
	s.compiledFieldInfo = make(map[string]*fieldProcessInfo)

	// 编译SimpleFields信息
	for _, fieldSpec := range s.config.SimpleFields {
		info := &fieldProcessInfo{}

		if fieldSpec == "*" {
			info.isSelectAll = true
			info.fieldName = "*"
			info.outputName = "*"
		} else {
			// 解析别名
			parts := strings.Split(fieldSpec, ":")
			info.fieldName = parts[0]
			info.outputName = info.fieldName
			if len(parts) > 1 {
				info.outputName = parts[1]
			}

			// 预判断字段特征
			info.isFunctionCall = strings.Contains(info.fieldName, "(") && strings.Contains(info.fieldName, ")")
			info.hasNestedField = !info.isFunctionCall && fieldpath.IsNestedField(info.fieldName)
		}

		s.compiledFieldInfo[fieldSpec] = info
	}
}

func (s *Stream) Start() {
	// 启动处理协程
	go s.process()
}

func (s *Stream) process() {
	// 初始化聚合器，用于窗口模式
	if s.config.NeedWindow {
		// 转换为新的AggregationField格式
		aggregationFields := convertToAggregationFields(s.config.SelectFields, s.config.FieldAlias)
		s.aggregator = aggregator.NewGroupAggregator(s.config.GroupFields, aggregationFields)

		// 为表达式字段创建计算器
		for field, fieldExpr := range s.config.FieldExpressions {
			// 创建局部变量避免闭包问题
			currentField := field
			currentFieldExpr := fieldExpr

			// 注册表达式计算器
			s.aggregator.RegisterExpression(
				currentField,
				currentFieldExpr.Expression,
				currentFieldExpr.Fields,
				func(data interface{}) (interface{}, error) {
					// 将数据转换为 map[string]interface{} 以便计算
					var dataMap map[string]interface{}
					switch d := data.(type) {
					case map[string]interface{}:
						dataMap = d
					default:
						// 如果不是 map，尝试转换
						v := reflect.ValueOf(data)
						if v.Kind() == reflect.Ptr {
							v = v.Elem()
						}

						if v.Kind() == reflect.Struct {
							// 将结构体转换为 map
							dataMap = make(map[string]interface{})
							t := v.Type()
							for i := 0; i < t.NumField(); i++ {
								field := t.Field(i)
								dataMap[field.Name] = v.Field(i).Interface()
							}
						} else {
							return nil, fmt.Errorf("unsupported data type for expression: %T", data)
						}
					}

					// 检查表达式是否包含嵌套字段，如果有则直接使用自定义表达式引擎
					hasNestedFields := strings.Contains(currentFieldExpr.Expression, ".")

					if hasNestedFields {
						// 直接使用自定义表达式引擎处理嵌套字段，支持NULL值
						expression, parseErr := expr.NewExpression(currentFieldExpr.Expression)
						if parseErr != nil {
							return nil, fmt.Errorf("expression parse failed: %w", parseErr)
						}

						// 使用支持NULL的计算方法
						numResult, isNull, err := expression.EvaluateWithNull(dataMap)
						if err != nil {
							return nil, fmt.Errorf("expression evaluation failed: %w", err)
						}
						if isNull {
							return nil, nil // 返回nil表示NULL值
						}
						return numResult, nil
					}

					// 检查是否为CASE表达式
					trimmedExpr := strings.TrimSpace(currentFieldExpr.Expression)
					upperExpr := strings.ToUpper(trimmedExpr)
					if strings.HasPrefix(upperExpr, "CASE") {
						// CASE表达式使用支持NULL的计算方法
						expression, parseErr := expr.NewExpression(currentFieldExpr.Expression)
						if parseErr != nil {
							return nil, fmt.Errorf("CASE expression parse failed: %w", parseErr)
						}

						numResult, isNull, err := expression.EvaluateWithNull(dataMap)
						if err != nil {
							return nil, fmt.Errorf("CASE expression evaluation failed: %w", err)
						}
						if isNull {
							return nil, nil // 返回nil表示NULL值
						}
						return numResult, nil
					}

					// 使用桥接器计算表达式，支持字符串拼接和IS NULL等语法
					bridge := functions.GetExprBridge()

					// 预处理表达式中的IS NULL和LIKE语法
					processedExpr := currentFieldExpr.Expression
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
						// 如果桥接器失败，回退到原来的表达式引擎（使用原始表达式，不是预处理的）
						expression, parseErr := expr.NewExpression(currentFieldExpr.Expression)
						if parseErr != nil {
							return nil, fmt.Errorf("expression parse failed: %w", parseErr)
						}

						// 计算表达式，支持NULL值
						numResult, isNull, evalErr := expression.EvaluateWithNull(dataMap)
						if evalErr != nil {
							return nil, fmt.Errorf("expression evaluation failed: %w", evalErr)
						}
						if isNull {
							return nil, nil // 返回nil表示NULL值
						}
						return numResult, nil
					}

					return result, nil
				},
			)
		}

		// 启动窗口处理协程
		s.Window.Start()

		// 处理窗口模式
		go func() {
			for batch := range s.Window.OutputChan() {
				// 处理窗口批数据
				for _, item := range batch {
					_ = s.aggregator.Put("window_start", item.Slot.WindowStart())
					_ = s.aggregator.Put("window_end", item.Slot.WindowEnd())
					if err := s.aggregator.Add(item.Data); err != nil {
						logger.Error("aggregate error: %v", err)
					}
				}

				// 获取并发送聚合结果
				if results, err := s.aggregator.GetResults(); err == nil {
					var finalResults []map[string]interface{}
					if s.config.Distinct {
						seenResults := make(map[string]bool)
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
					} else {
						finalResults = results
					}

					// 应用 HAVING 过滤条件
					if s.config.Having != "" {
						// 检查HAVING条件是否包含CASE表达式
						hasCaseExpression := strings.Contains(strings.ToUpper(s.config.Having), "CASE")

						var filteredResults []map[string]interface{}

						if hasCaseExpression {
							// HAVING条件包含CASE表达式，使用我们的表达式解析器
							expression, err := expr.NewExpression(s.config.Having)
							if err != nil {
								logger.Error("having filter error (CASE expression): %v", err)
							} else {
								// 应用 HAVING 过滤，使用CASE表达式计算器
								for _, result := range finalResults {
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
							}
						} else {
							// HAVING条件不包含CASE表达式，使用原有的expr-lang处理
							// 预处理HAVING条件中的LIKE语法，转换为expr-lang可理解的形式
							processedHaving := s.config.Having
							bridge := functions.GetExprBridge()
							if bridge.ContainsLikeOperator(s.config.Having) {
								if processed, err := bridge.PreprocessLikeExpression(s.config.Having); err == nil {
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
							} else {
								// 应用 HAVING 过滤
								for _, result := range finalResults {
									if havingFilter.Evaluate(result) {
										filteredResults = append(filteredResults, result)
									}
								}
							}
						}

						finalResults = filteredResults
					}

					// 应用 LIMIT 限制
					if s.config.Limit > 0 && len(finalResults) > s.config.Limit {
						finalResults = finalResults[:s.config.Limit]
					}

					// 优化: 发送结果到结果通道和 Sink 函数
					if len(finalResults) > 0 {
						// 非阻塞发送到结果通道
						s.sendResultNonBlocking(finalResults)

						// 异步调用所有sinks
						s.callSinksAsync(finalResults)
					}
					s.aggregator.Reset()
				}
			}
		}()
	}

	// 创建一个定时器，避免创建多个临时定时器导致资源泄漏
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop() // 确保在函数退出时停止定时器

	// 主处理循环
	for {
		// 使用读锁安全访问dataChan
		s.dataChanMux.RLock()
		currentDataChan := s.dataChan
		s.dataChanMux.RUnlock()

		select {
		case data, ok := <-currentDataChan:
			if !ok {
				// 通道已关闭
				return
			}
			// 应用过滤条件
			if s.filter == nil || s.filter.Evaluate(data) {
				if s.config.NeedWindow {
					// 窗口模式，添加数据到窗口
					s.Window.Add(data)
				} else {
					// 非窗口模式，直接处理数据并输出
					s.processDirectData(data)
				}
			}
		case <-s.done:
			// 收到关闭信号
			return
		case <-ticker.C:
			// 定时器触发，什么都不做，只是防止 CPU 空转
		}
	}
}

// processDirectData 直接处理非窗口数据 (优化版本)
func (s *Stream) processDirectData(data interface{}) {
	// 增加输入计数
	atomic.AddInt64(&s.inputCount, 1)

	// 简化：直接将数据作为map处理
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		logger.Error("Unsupported data type: %T", data)
		atomic.AddInt64(&s.droppedCount, 1)
		return
	}

	// 创建结果map，预分配合适容量
	estimatedSize := len(s.config.FieldExpressions) + len(s.config.SimpleFields)
	if estimatedSize < 8 {
		estimatedSize = 8 // 最小容量
	}
	result := make(map[string]interface{}, estimatedSize)

	// 处理表达式字段
	for fieldName, fieldExpr := range s.config.FieldExpressions {

		// 使用桥接器计算表达式，支持IS NULL等语法
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

		// 检查表达式是否是函数调用（包含括号）
		isFunctionCall := strings.Contains(fieldExpr.Expression, "(") && strings.Contains(fieldExpr.Expression, ")")

		// 检查表达式是否包含嵌套字段（但排除函数调用中的点号）
		hasNestedFields := false
		if !isFunctionCall && strings.Contains(fieldExpr.Expression, ".") {
			hasNestedFields = true
		}

		var evalResult interface{}

		if isFunctionCall {
			// 对于函数调用，优先使用桥接器处理，这样可以保持原始类型
			exprResult, err := bridge.EvaluateExpression(processedExpr, dataMap)
			if err != nil {
				logger.Error("Function call evaluation failed for field %s: %v", fieldName, err)
				result[fieldName] = nil
				continue
			}
			evalResult = exprResult
		} else if hasNestedFields {
			// 检测到嵌套字段（非函数调用），使用自定义表达式引擎
			expression, parseErr := expr.NewExpression(fieldExpr.Expression)
			if parseErr != nil {
				logger.Error("Expression parse failed for field %s: %v", fieldName, parseErr)
				result[fieldName] = nil
				continue
			}

			numResult, err := expression.Evaluate(dataMap)
			if err != nil {
				logger.Error("Expression evaluation failed for field %s: %v", fieldName, err)
				result[fieldName] = nil
				continue
			}
			evalResult = numResult
		} else {
			// 尝试使用桥接器处理其他表达式
			exprResult, err := bridge.EvaluateExpression(processedExpr, dataMap)
			if err != nil {
				// 如果桥接器失败，回退到原来的表达式引擎（使用原始表达式，不是预处理的）
				expression, parseErr := expr.NewExpression(fieldExpr.Expression)
				if parseErr != nil {
					logger.Error("Expression parse failed for field %s: %v", fieldName, parseErr)
					result[fieldName] = nil
					continue
				}

				// 计算表达式
				numResult, evalErr := expression.Evaluate(dataMap)
				if evalErr != nil {
					logger.Error("Expression evaluation failed for field %s: %v", fieldName, evalErr)
					result[fieldName] = nil
					continue
				}
				evalResult = numResult
			} else {
				evalResult = exprResult
			}
		}

		result[fieldName] = evalResult
	}

	// 性能优化：使用预编译的字段信息处理SimpleFields
	if len(s.config.SimpleFields) > 0 {
		for _, fieldSpec := range s.config.SimpleFields {
			info := s.compiledFieldInfo[fieldSpec]
			if info == nil {
				// 如果没有预编译信息，回退到原逻辑（安全性保证）
				s.processSingleFieldFallback(fieldSpec, dataMap, data, result)
				continue
			}

			if info.isSelectAll {
				// SELECT *：批量复制所有字段，跳过表达式字段
				for k, v := range dataMap {
					if _, isExpression := s.config.FieldExpressions[k]; !isExpression {
						result[k] = v
					}
				}
				continue
			}

			// 跳过已经通过表达式字段处理的字段
			if _, isExpression := s.config.FieldExpressions[info.outputName]; isExpression {
				continue
			}

			if info.isFunctionCall {
				// 执行函数调用
				if funcResult, err := s.executeFunction(info.fieldName, dataMap); err == nil {
					result[info.outputName] = funcResult
				} else {
					logger.Error("Function execution error %s: %v", info.fieldName, err)
					result[info.outputName] = nil
				}
			} else {
				// 普通字段处理
				var value interface{}
				var exists bool

				if info.hasNestedField {
					value, exists = fieldpath.GetNestedField(data, info.fieldName)
				} else {
					value, exists = dataMap[info.fieldName]
				}

				if exists {
					result[info.outputName] = value
				} else {
					result[info.outputName] = nil
				}
			}
		}
	} else if len(s.config.FieldExpressions) == 0 {
		// 如果没有指定字段且没有表达式字段，保留所有字段
		for k, v := range dataMap {
			result[k] = v
		}
	}

	// 将结果包装为数组
	results := []map[string]interface{}{result}

	// 优化: 非阻塞发送结果到resultChan
	s.sendResultNonBlocking(results)

	// 优化: 异步调用所有sinks，避免阻塞
	s.callSinksAsync(results)
}

// processSingleFieldFallback 回退处理单个字段（当预编译信息缺失时）
func (s *Stream) processSingleFieldFallback(fieldSpec string, dataMap map[string]interface{}, data interface{}, result map[string]interface{}) {
	// 处理SELECT *的特殊情况
	if fieldSpec == "*" {
		// SELECT *：返回所有字段，但跳过已经通过表达式字段处理的字段
		for k, v := range dataMap {
			// 如果该字段已经通过表达式字段处理，则跳过，保持表达式计算结果
			if _, isExpression := s.config.FieldExpressions[k]; !isExpression {
				result[k] = v
			}
		}
		return
	}

	// 处理别名
	parts := strings.Split(fieldSpec, ":")
	fieldName := parts[0]
	outputName := fieldName
	if len(parts) > 1 {
		outputName = parts[1]
	}

	// 跳过已经通过表达式字段处理的字段
	if _, isExpression := s.config.FieldExpressions[outputName]; isExpression {
		return
	}

	// 检查是否是函数调用
	if strings.Contains(fieldName, "(") && strings.Contains(fieldName, ")") {
		// 执行函数调用
		if funcResult, err := s.executeFunction(fieldName, dataMap); err == nil {
			result[outputName] = funcResult
		} else {
			logger.Error("Function execution error %s: %v", fieldName, err)
			result[outputName] = nil
		}
	} else {
		// 普通字段 - 支持嵌套字段
		var value interface{}
		var exists bool

		if fieldpath.IsNestedField(fieldName) {
			value, exists = fieldpath.GetNestedField(data, fieldName)
		} else {
			value, exists = dataMap[fieldName]
		}

		if exists {
			result[outputName] = value
		} else {
			result[outputName] = nil
		}
	}
}

// sendResultNonBlocking 非阻塞方式发送结果到resultChan (智能背压控制)
func (s *Stream) sendResultNonBlocking(results []map[string]interface{}) {
	select {
	case s.resultChan <- results:
		// 成功发送到结果通道
		atomic.AddInt64(&s.outputCount, 1)
	default:
		// 结果通道已满，使用智能背压控制策略
		chanLen := len(s.resultChan)
		chanCap := cap(s.resultChan)

		// 如果通道使用率超过90%，进入背压模式
		if float64(chanLen)/float64(chanCap) > 0.9 {
			// 尝试清理一些旧数据，为新数据腾出空间
			select {
			case <-s.resultChan:
				// 清理一个旧结果，然后尝试添加新结果
				select {
				case s.resultChan <- results:
					atomic.AddInt64(&s.outputCount, 1)
				default:
					logger.Warn("Result channel is full, dropping result data")
					atomic.AddInt64(&s.droppedCount, 1)
				}
			default:
				logger.Warn("Result channel is full, dropping result data")
				atomic.AddInt64(&s.droppedCount, 1)
			}
		} else {
			logger.Warn("Result channel is full, dropping result data")
			atomic.AddInt64(&s.droppedCount, 1)
		}
	}
}

// callSinksAsync 异步调用所有sink函数
func (s *Stream) callSinksAsync(results []map[string]interface{}) {
	// 使用读锁安全地访问sinks切片
	s.sinksMux.RLock()
	if len(s.sinks) == 0 {
		s.sinksMux.RUnlock()
		return
	}

	// 复制sinks切片以避免在持有锁的情况下执行耗时操作
	sinksCopy := make([]func(interface{}), len(s.sinks))
	copy(sinksCopy, s.sinks)
	s.sinksMux.RUnlock()

	// 为每个sink创建异步任务
	for _, sink := range sinksCopy {
		// 捕获sink变量，避免闭包问题
		currentSink := sink

		// 提交任务到工作池
		task := func() {
			defer func() {
				// 恢复panic，防止单个sink错误影响整个系统
				if r := recover(); r != nil {
					logger.Error("Sink execution exception: %v", r)
				}
			}()
			currentSink(results)
		}

		// 非阻塞提交任务
		select {
		case s.sinkWorkerPool <- task:
			// 成功提交任务
		default:
			// 工作池已满，直接在当前goroutine执行（降级处理）
			go task()
		}
	}
}

// executeFunction 执行函数调用
func (s *Stream) executeFunction(funcExpr string, data map[string]interface{}) (interface{}, error) {
	// 检查是否是自定义函数
	funcName := extractFunctionName(funcExpr)
	if funcName != "" {
		// 直接使用函数系统
		fn, exists := functions.Get(funcName)
		if exists {
			// 解析参数
			args, err := s.parseFunctionArgs(funcExpr, data)
			if err != nil {
				return nil, err
			}

			// 创建函数上下文
			ctx := &functions.FunctionContext{Data: data}

			// 执行函数
			return fn.Execute(ctx, args)
		}
	}

	// 对于复杂的嵌套函数调用，直接使用ExprBridge
	// 这样可以避免Expression.Evaluate的float64类型限制
	bridge := functions.GetExprBridge()
	result, err := bridge.EvaluateExpression(funcExpr, data)
	if err != nil {
		return nil, fmt.Errorf("evaluate function expression failed: %w", err)
	}

	return result, nil
}

// extractFunctionName 从表达式中提取函数名
func extractFunctionName(expr string) string {
	parenIndex := strings.Index(expr, "(")
	if parenIndex == -1 {
		return ""
	}
	funcName := strings.TrimSpace(expr[:parenIndex])
	if strings.ContainsAny(funcName, " +-*/=<>!&|") {
		return ""
	}
	return funcName
}

// parseFunctionArgs 解析函数参数，支持嵌套函数调用
func (s *Stream) parseFunctionArgs(funcExpr string, data map[string]interface{}) ([]interface{}, error) {
	// 提取括号内的参数
	start := strings.Index(funcExpr, "(")
	end := strings.LastIndex(funcExpr, ")")
	if start == -1 || end == -1 || end <= start {
		return nil, fmt.Errorf("invalid function expression: %s", funcExpr)
	}

	argsStr := strings.TrimSpace(funcExpr[start+1 : end])
	if argsStr == "" {
		return []interface{}{}, nil
	}

	// 智能分割参数，处理嵌套函数和引号
	argParts, err := s.smartSplitArgs(argsStr)
	if err != nil {
		return nil, err
	}

	args := make([]interface{}, len(argParts))

	for i, arg := range argParts {
		arg = strings.TrimSpace(arg)

		// 如果参数是字符串常量（用引号包围）
		if strings.HasPrefix(arg, "'") && strings.HasSuffix(arg, "'") {
			args[i] = strings.Trim(arg, "'")
		} else if strings.HasPrefix(arg, "\"") && strings.HasSuffix(arg, "\"") {
			args[i] = strings.Trim(arg, "\"")
		} else if strings.Contains(arg, "(") {
			// 如果参数包含函数调用，递归执行
			result, err := s.executeFunction(arg, data)
			if err != nil {
				return nil, fmt.Errorf("failed to execute nested function '%s': %v", arg, err)
			}
			args[i] = result
		} else if value, exists := data[arg]; exists {
			// 如果是数据字段
			args[i] = value
		} else {
			// 尝试解析为数字
			if val, err := strconv.ParseFloat(arg, 64); err == nil {
				args[i] = val
			} else {
				args[i] = arg
			}
		}
	}

	return args, nil
}

// smartSplitArgs 智能分割参数，考虑括号嵌套和引号
func (s *Stream) smartSplitArgs(argsStr string) ([]string, error) {
	var args []string
	var current strings.Builder
	parenDepth := 0
	inQuotes := false
	quoteChar := byte(0)

	for i := 0; i < len(argsStr); i++ {
		ch := argsStr[i]

		switch ch {
		case '\'':
			if !inQuotes {
				inQuotes = true
				quoteChar = ch
			} else if quoteChar == ch {
				inQuotes = false
				quoteChar = 0
			}
			current.WriteByte(ch)
		case '"':
			if !inQuotes {
				inQuotes = true
				quoteChar = ch
			} else if quoteChar == ch {
				inQuotes = false
				quoteChar = 0
			}
			current.WriteByte(ch)
		case '(':
			if !inQuotes {
				parenDepth++
			}
			current.WriteByte(ch)
		case ')':
			if !inQuotes {
				parenDepth--
			}
			current.WriteByte(ch)
		case ',':
			if !inQuotes && parenDepth == 0 {
				// 找到参数分隔符
				args = append(args, strings.TrimSpace(current.String()))
				current.Reset()
			} else {
				current.WriteByte(ch)
			}
		default:
			current.WriteByte(ch)
		}
	}

	// 添加最后一个参数
	if current.Len() > 0 {
		args = append(args, strings.TrimSpace(current.String()))
	}

	return args, nil
}

func (s *Stream) Emit(data interface{}) {
	atomic.AddInt64(&s.inputCount, 1)
	// 性能优化：直接调用预编译的函数指针，避免switch判断
	s.addDataFunc(data)
}

// addDataBlocking 阻塞模式添加数据，保证零数据丢失 (线程安全版本)
func (s *Stream) addDataBlocking(data interface{}) {
	if s.blockingTimeout <= 0 {
		// 无超时限制，永久阻塞直到成功
		dataChan := s.safeGetDataChan()
		dataChan <- data
		return
	}

	// 带超时的阻塞
	timer := time.NewTimer(s.blockingTimeout)
	defer timer.Stop()

	dataChan := s.safeGetDataChan()
	select {
	case dataChan <- data:
		// 成功添加数据
		return
	case <-timer.C:
		// 超时但不丢弃数据，记录错误但继续阻塞
		logger.Error("Data addition timeout, but continue waiting to avoid data loss")
		// 继续无限期阻塞，重新获取当前通道引用
		finalDataChan := s.safeGetDataChan()
		finalDataChan <- data
	}
}

// addDataWithExpansion 动态扩容模式 (线程安全版本)
func (s *Stream) addDataWithExpansion(data interface{}) {
	// 首次尝试添加数据
	if s.safeSendToDataChan(data) {
		return
	}

	// 通道满了，动态扩容
	s.expandDataChannel()

	// 扩容后重试，重新获取通道引用
	if s.safeSendToDataChan(data) {
		logger.Debug("Successfully added data after data channel expansion")
		return
	}

	// 如果扩容后仍然满，则阻塞等待
	dataChan := s.safeGetDataChan()
	dataChan <- data
}

// addDataWithPersistence 持久化模式（线程安全完整实现）
func (s *Stream) addDataWithPersistence(data interface{}) {
	// 首次尝试添加数据
	if s.safeSendToDataChan(data) {
		return
	}

	// 通道满了，持久化到磁盘
	if s.persistenceManager != nil {
		if err := s.persistenceManager.PersistData(data); err != nil {
			logger.Error("Failed to persist data: %v", err)
			atomic.AddInt64(&s.droppedCount, 1)
		} else {
			logger.Debug("Data has been persisted to disk")
		}
	} else {
		logger.Error("Persistence manager not initialized, data will be lost")
		atomic.AddInt64(&s.droppedCount, 1)
	}

	// 启动异步重试
	go s.persistAndRetryData(data)
}

// addDataWithDrop 原有的丢弃模式 (线程安全版本)
func (s *Stream) addDataWithDrop(data interface{}) {
	// 优化: 智能非阻塞添加，分层背压控制
	if s.safeSendToDataChan(data) {
		return
	}

	// 数据通道已满，使用分层背压策略，获取通道状态
	s.dataChanMux.RLock()
	chanLen := len(s.dataChan)
	chanCap := cap(s.dataChan)
	currentDataChan := s.dataChan
	s.dataChanMux.RUnlock()

	usage := float64(chanLen) / float64(chanCap)

	// 根据通道使用率和缓冲区大小调整策略
	var waitTime time.Duration
	var maxRetries int

	switch {
	case chanCap >= 100000: // 超大缓冲区（基准测试模式）
		switch {
		case usage > 0.99:
			waitTime = 1 * time.Millisecond // 更长等待
			maxRetries = 3
		case usage > 0.95:
			waitTime = 500 * time.Microsecond
			maxRetries = 2
		case usage > 0.90:
			waitTime = 100 * time.Microsecond
			maxRetries = 1
		default:
			// 立即丢弃
			logger.Warn("Data channel is full, dropping input data")
			atomic.AddInt64(&s.droppedCount, 1)
			return
		}

	case chanCap >= 50000: // 高性能模式
		switch {
		case usage > 0.99:
			waitTime = 500 * time.Microsecond
			maxRetries = 2
		case usage > 0.95:
			waitTime = 200 * time.Microsecond
			maxRetries = 1
		case usage > 0.90:
			waitTime = 50 * time.Microsecond
			maxRetries = 1
		default:
			logger.Warn("Data channel is full, dropping input data")
			atomic.AddInt64(&s.droppedCount, 1)
			return
		}

	default: // 默认模式
		switch {
		case usage > 0.99:
			waitTime = 100 * time.Microsecond
			maxRetries = 1
		case usage > 0.95:
			waitTime = 50 * time.Microsecond
			maxRetries = 1
		default:
			logger.Warn("Data channel is full, dropping input data")
			atomic.AddInt64(&s.droppedCount, 1)
			return
		}
	}

	// 多次重试添加数据，使用线程安全的方式
	for retry := 0; retry < maxRetries; retry++ {
		timer := time.NewTimer(waitTime)
		select {
		case currentDataChan <- data:
			// 重试成功
			timer.Stop()
			return
		case <-timer.C:
			// 超时，继续下一次重试或者丢弃
			if retry == maxRetries-1 {
				// 最后一次重试失败，记录丢弃
				logger.Warn("Data channel is full, dropping input data")
				atomic.AddInt64(&s.droppedCount, 1)
			}
		}
	}
}

// safeGetDataChan 线程安全地获取dataChan引用
func (s *Stream) safeGetDataChan() chan interface{} {
	s.dataChanMux.RLock()
	defer s.dataChanMux.RUnlock()
	return s.dataChan
}

// safeSendToDataChan 线程安全地向dataChan发送数据
func (s *Stream) safeSendToDataChan(data interface{}) bool {
	dataChan := s.safeGetDataChan()
	select {
	case dataChan <- data:
		return true
	default:
		return false
	}
}

func (s *Stream) AddSink(sink func(interface{})) {
	s.sinksMux.Lock()
	s.sinks = append(s.sinks, sink)
	s.sinksMux.Unlock()
}

func (s *Stream) GetResultsChan() <-chan interface{} {
	return s.resultChan
}

func NewStreamProcessor() (*Stream, error) {
	return NewStream(types.Config{})
}

// Stop 停止流处理
func (s *Stream) Stop() {
	close(s.done)

	// 停止持久化管理器
	if s.persistenceManager != nil {
		if err := s.persistenceManager.Stop(); err != nil {
			logger.Error("Failed to stop persistence manager: %v", err)
		}
	}
}

// GetStats 获取流处理统计信息 (线程安全版本)
func (s *Stream) GetStats() map[string]int64 {
	// 线程安全地获取dataChan状态
	s.dataChanMux.RLock()
	dataChanLen := int64(len(s.dataChan))
	dataChanCap := int64(cap(s.dataChan))
	s.dataChanMux.RUnlock()

	return map[string]int64{
		"input_count":     atomic.LoadInt64(&s.inputCount),
		"output_count":    atomic.LoadInt64(&s.outputCount),
		"dropped_count":   atomic.LoadInt64(&s.droppedCount),
		"data_chan_len":   dataChanLen,
		"data_chan_cap":   dataChanCap,
		"result_chan_len": int64(len(s.resultChan)),
		"result_chan_cap": int64(cap(s.resultChan)),
		"sink_pool_len":   int64(len(s.sinkWorkerPool)),
		"sink_pool_cap":   int64(cap(s.sinkWorkerPool)),
		"active_retries":  int64(atomic.LoadInt32(&s.activeRetries)),
		"expanding":       int64(atomic.LoadInt32(&s.expanding)),
	}
}

// GetDetailedStats 获取详细的性能统计信息
func (s *Stream) GetDetailedStats() map[string]interface{} {
	stats := s.GetStats()

	// 计算使用率
	dataUsage := float64(stats["data_chan_len"]) / float64(stats["data_chan_cap"]) * 100
	resultUsage := float64(stats["result_chan_len"]) / float64(stats["result_chan_cap"]) * 100
	sinkUsage := float64(stats["sink_pool_len"]) / float64(stats["sink_pool_cap"]) * 100

	// 计算效率指标
	var processRate float64 = 100.0
	var dropRate float64 = 0.0

	if stats["input_count"] > 0 {
		processRate = float64(stats["output_count"]) / float64(stats["input_count"]) * 100
		dropRate = float64(stats["dropped_count"]) / float64(stats["input_count"]) * 100
	}

	return map[string]interface{}{
		"basic_stats":       stats,
		"data_chan_usage":   dataUsage,
		"result_chan_usage": resultUsage,
		"sink_pool_usage":   sinkUsage,
		"process_rate":      processRate,
		"drop_rate":         dropRate,
		"performance_level": s.assessPerformanceLevel(dataUsage, dropRate),
	}
}

// assessPerformanceLevel 评估当前性能水平
func (s *Stream) assessPerformanceLevel(dataUsage, dropRate float64) string {
	switch {
	case dropRate > 50:
		return "CRITICAL" // 严重性能问题
	case dropRate > 20:
		return "WARNING" // 性能警告
	case dataUsage > 90:
		return "HIGH_LOAD" // 高负载
	case dataUsage > 70:
		return "MODERATE_LOAD" // 中等负载
	default:
		return "OPTIMAL" // 最佳状态
	}
}

// ResetStats 重置统计信息
func (s *Stream) ResetStats() {
	atomic.StoreInt64(&s.inputCount, 0)
	atomic.StoreInt64(&s.outputCount, 0)
	atomic.StoreInt64(&s.droppedCount, 0)
}

// expandDataChannel 动态扩容数据通道
func (s *Stream) expandDataChannel() {
	// 使用原子操作检查是否正在扩容，防止并发扩容
	if !atomic.CompareAndSwapInt32(&s.expanding, 0, 1) {
		logger.Debug("Channel expansion already in progress, skipping")
		return
	}
	defer atomic.StoreInt32(&s.expanding, 0)

	// 获取扩容锁，确保只有一个协程进行扩容
	s.expansionMux.Lock()
	defer s.expansionMux.Unlock()

	// 再次检查是否需要扩容（双重检查锁定模式）
	s.dataChanMux.RLock()
	oldCap := cap(s.dataChan)
	currentLen := len(s.dataChan)
	s.dataChanMux.RUnlock()

	// 如果当前通道使用率低于80%，则不需要扩容
	if float64(currentLen)/float64(oldCap) < 0.8 {
		logger.Debug("Channel usage below threshold, expansion not needed")
		return
	}

	newCap := int(float64(oldCap) * 1.5) // 扩容50%
	if newCap < oldCap+1000 {
		newCap = oldCap + 1000 // 至少增加1000
	}

	logger.Debug("Dynamic expansion of data channel: %d -> %d", oldCap, newCap)

	// 创建新的更大的通道
	newChan := make(chan interface{}, newCap)

	// 使用写锁安全地迁移数据
	s.dataChanMux.Lock()
	oldChan := s.dataChan

	// 将旧通道中的数据快速迁移到新通道
	migrationTimeout := time.NewTimer(5 * time.Second) // 5秒迁移超时
	defer migrationTimeout.Stop()

	migratedCount := 0
	for {
		select {
		case data := <-oldChan:
			select {
			case newChan <- data:
				migratedCount++
			case <-migrationTimeout.C:
				logger.Warn("Data migration timeout, some data may be lost during expansion")
				goto migration_done
			}
		case <-migrationTimeout.C:
			logger.Warn("Data migration timeout during channel drain")
			goto migration_done
		default:
			// 旧通道为空，迁移完成
			goto migration_done
		}
	}

migration_done:
	// 原子性地更新通道引用
	s.dataChan = newChan
	s.dataChanMux.Unlock()

	logger.Debug("Channel expansion completed: migrated %d items", migratedCount)
}

// persistAndRetryData 持久化数据并重试 (改进版本，具备指数退避和资源控制)
func (s *Stream) persistAndRetryData(data interface{}) {
	// 检查活跃重试协程数量，防止资源泄漏
	currentRetries := atomic.LoadInt32(&s.activeRetries)
	if currentRetries >= s.maxRetryRoutines {
		logger.Warn("Maximum retry routines reached (%d), dropping data", currentRetries)
		atomic.AddInt64(&s.droppedCount, 1)
		return
	}

	// 增加活跃重试计数
	atomic.AddInt32(&s.activeRetries, 1)
	defer atomic.AddInt32(&s.activeRetries, -1)

	// 使用指数退避策略
	baseInterval := 50 * time.Millisecond
	maxInterval := 2 * time.Second
	maxRetries := 10                 // 减少最大重试次数，防止长时间阻塞
	totalTimeout := 30 * time.Second // 总超时时间

	retryTimer := time.NewTimer(totalTimeout)
	defer retryTimer.Stop()

	for attempt := 0; attempt < maxRetries; attempt++ {
		// 计算当前重试间隔（指数退避）
		currentInterval := time.Duration(float64(baseInterval) * (1.5 * float64(attempt)))
		if currentInterval > maxInterval {
			currentInterval = maxInterval
		}

		// 等待重试间隔
		waitTimer := time.NewTimer(currentInterval)
		select {
		case <-waitTimer.C:
			// 继续重试
		case <-retryTimer.C:
			waitTimer.Stop()
			logger.Warn("Persistence retry timeout reached, dropping data")
			atomic.AddInt64(&s.droppedCount, 1)
			return
		case <-s.done:
			waitTimer.Stop()
			logger.Debug("Stream stopped during retry, dropping data")
			atomic.AddInt64(&s.droppedCount, 1)
			return
		}
		waitTimer.Stop()

		// 使用线程安全方式尝试发送数据
		s.dataChanMux.RLock()
		currentDataChan := s.dataChan
		s.dataChanMux.RUnlock()

		select {
		case currentDataChan <- data:
			logger.Debug("Persistence data retry successful: attempt %d", attempt+1)
			return
		case <-retryTimer.C:
			logger.Warn("Persistence retry timeout during send, dropping data")
			atomic.AddInt64(&s.droppedCount, 1)
			return
		case <-s.done:
			logger.Debug("Stream stopped during retry send, dropping data")
			atomic.AddInt64(&s.droppedCount, 1)
			return
		default:
			// 通道仍然满，继续下一次重试
			if attempt == maxRetries-1 {
				logger.Error("Persistence data retry failed after %d attempts, dropping data", maxRetries)
				atomic.AddInt64(&s.droppedCount, 1)
			} else {
				logger.Debug("Persistence retry attempt %d/%d failed, will retry with interval %v",
					attempt+1, maxRetries, currentInterval)
			}
		}
	}
}

// LoadAndReprocessPersistedData 加载并重新处理持久化数据
func (s *Stream) LoadAndReprocessPersistedData() error {
	if s.persistenceManager == nil {
		return fmt.Errorf("persistence manager not initialized")
	}

	// 加载持久化数据
	persistedData, err := s.persistenceManager.LoadPersistedData()
	if err != nil {
		return fmt.Errorf("failed to load persisted data: %w", err)
	}

	if len(persistedData) == 0 {
		logger.Info("No persistent data to recover")
		return nil
	}

	logger.Info("Start reprocessing %d persistent data records", len(persistedData))

	// 重新处理每条数据（线程安全版本）
	successCount := 0
	for i, data := range persistedData {
		// 使用线程安全方式尝试发送数据
		if s.safeSendToDataChan(data) {
			successCount++
			continue
		}

		// 如果通道还是满的，等待一小段时间再试
		time.Sleep(10 * time.Millisecond)
		if s.safeSendToDataChan(data) {
			successCount++
		} else {
			logger.Warn("Failed to recover data record %d, channel still full", i+1)
		}
	}

	logger.Info("Persistent data recovery completed: successful %d/%d records", successCount, len(persistedData))
	return nil
}

// GetPersistenceStats 获取持久化统计信息
func (s *Stream) GetPersistenceStats() map[string]interface{} {
	if s.persistenceManager == nil {
		return map[string]interface{}{
			"enabled": false,
			"message": "persistence not enabled",
		}
	}

	stats := s.persistenceManager.GetStats()
	stats["enabled"] = true
	return stats
}

// 向后兼容性函数

// NewStreamWithBuffers 创建带自定义缓冲区大小的Stream (已弃用，使用NewStreamWithCustomPerformance)
// Deprecated: 使用NewStreamWithCustomPerformance替代
func NewStreamWithBuffers(config types.Config, dataBufSize, resultBufSize, sinkPoolSize int) (*Stream, error) {
	perfConfig := types.DefaultPerformanceConfig()
	perfConfig.BufferConfig.DataChannelSize = dataBufSize
	perfConfig.BufferConfig.ResultChannelSize = resultBufSize
	perfConfig.WorkerConfig.SinkPoolSize = sinkPoolSize

	config.PerformanceConfig = perfConfig
	return newStreamWithUnifiedConfig(config)
}

// NewHighPerformanceStream 创建高性能配置的Stream (已弃用，使用NewStreamWithHighPerformance)
// Deprecated: 使用NewStreamWithHighPerformance替代
func NewHighPerformanceStream(config types.Config) (*Stream, error) {
	return NewStreamWithHighPerformance(config)
}

// NewStreamWithoutDataLoss 创建零数据丢失的流处理器 (已弃用，使用NewStreamWithZeroDataLoss)
// Deprecated: 使用NewStreamWithZeroDataLoss替代
func NewStreamWithoutDataLoss(config types.Config, strategy string) (*Stream, error) {
	perfConfig := types.ZeroDataLossConfig()

	// 应用用户指定的策略
	validStrategies := map[string]bool{
		"drop":    true,
		"block":   true,
		"expand":  true,
		"persist": true,
	}

	if validStrategies[strategy] {
		perfConfig.OverflowConfig.Strategy = strategy
		if strategy == "drop" {
			perfConfig.OverflowConfig.AllowDataLoss = true
		}
	}

	config.PerformanceConfig = perfConfig
	return newStreamWithUnifiedConfig(config)
}

// NewStreamWithLossPolicy 创建带数据丢失策略的流处理器 (已弃用，使用NewStreamWithCustomPerformance)
// Deprecated: 使用NewStreamWithCustomPerformance替代
func NewStreamWithLossPolicy(config types.Config, dataBufSize, resultBufSize, sinkPoolSize int,
	overflowStrategy string, timeout time.Duration) (*Stream, error) {

	perfConfig := types.DefaultPerformanceConfig()
	perfConfig.BufferConfig.DataChannelSize = dataBufSize
	perfConfig.BufferConfig.ResultChannelSize = resultBufSize
	perfConfig.WorkerConfig.SinkPoolSize = sinkPoolSize
	perfConfig.OverflowConfig.Strategy = overflowStrategy
	perfConfig.OverflowConfig.BlockTimeout = timeout
	perfConfig.OverflowConfig.AllowDataLoss = (overflowStrategy == "drop")

	config.PerformanceConfig = perfConfig
	return newStreamWithUnifiedConfig(config)
}

// NewStreamWithLossPolicyAndPersistence 创建带数据丢失策略和持久化配置的流处理器 (已弃用，使用NewStreamWithCustomPerformance)
// Deprecated: 使用NewStreamWithCustomPerformance替代
func NewStreamWithLossPolicyAndPersistence(config types.Config, dataBufSize, resultBufSize, sinkPoolSize int,
	overflowStrategy string, timeout time.Duration, persistDataDir string, persistMaxFileSize int64, persistFlushInterval time.Duration) (*Stream, error) {

	perfConfig := types.DefaultPerformanceConfig()
	perfConfig.BufferConfig.DataChannelSize = dataBufSize
	perfConfig.BufferConfig.ResultChannelSize = resultBufSize
	perfConfig.WorkerConfig.SinkPoolSize = sinkPoolSize
	perfConfig.OverflowConfig.Strategy = overflowStrategy
	perfConfig.OverflowConfig.BlockTimeout = timeout
	perfConfig.OverflowConfig.AllowDataLoss = (overflowStrategy == "drop")

	// 设置持久化配置
	if overflowStrategy == "persist" {
		perfConfig.OverflowConfig.PersistenceConfig = &types.PersistenceConfig{
			DataDir:       persistDataDir,
			MaxFileSize:   persistMaxFileSize,
			FlushInterval: persistFlushInterval,
			MaxRetries:    5,
			RetryInterval: 1 * time.Second,
		}
	}

	config.PerformanceConfig = perfConfig
	return newStreamWithUnifiedConfig(config)
}
