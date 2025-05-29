package stream

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rulego/streamsql/condition"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/expr"
	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/window"
)

type Stream struct {
	dataChan    chan interface{}
	filter      condition.Condition
	Window      window.Window
	aggregator  aggregator.Aggregator
	config      types.Config
	sinks       []func(interface{})
	resultChan  chan interface{} // 结果通道
	seenResults *sync.Map
	done        chan struct{} // 用于关闭处理协程
}

func NewStream(config types.Config) (*Stream, error) {
	var win window.Window
	var err error

	// 只有在需要窗口时才创建窗口
	if config.NeedWindow {
		win, err = window.CreateWindow(config.WindowConfig)
		if err != nil {
			return nil, err
		}
	}

	return &Stream{
		dataChan:    make(chan interface{}, 1000),
		config:      config,
		Window:      win,
		resultChan:  make(chan interface{}, 10),
		seenResults: &sync.Map{},
		done:        make(chan struct{}),
	}, nil
}

func (s *Stream) RegisterFilter(conditionStr string) error {
	if strings.TrimSpace(conditionStr) == "" {
		return nil
	}
	filter, err := condition.NewExprCondition(conditionStr)
	if err != nil {
		return fmt.Errorf("compile filter error: %w", err)
	}
	s.filter = filter
	return nil
}

func (s *Stream) Start() {
	// 启动处理协程
	go s.process()
}

func (s *Stream) process() {
	// 初始化聚合器，用于窗口模式
	if s.config.NeedWindow {
		s.aggregator = aggregator.NewGroupAggregator(s.config.GroupFields, s.config.SelectFields, s.config.FieldAlias)

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

					// 使用桥接器计算表达式，支持字符串拼接
					bridge := functions.GetExprBridge()
					result, err := bridge.EvaluateExpression(currentFieldExpr.Expression, dataMap)
					if err != nil {
						// 如果桥接器失败，回退到原来的表达式引擎
						expression, parseErr := expr.NewExpression(currentFieldExpr.Expression)
						if parseErr != nil {
							return nil, fmt.Errorf("expression parse failed: %w", parseErr)
						}

						// 计算表达式
						numResult, evalErr := expression.Evaluate(dataMap)
						if evalErr != nil {
							return nil, fmt.Errorf("expression evaluation failed: %w", evalErr)
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
					s.aggregator.Put("window_start", item.Slot.WindowStart())
					s.aggregator.Put("window_end", item.Slot.WindowEnd())
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
						// 创建 HAVING 条件
						havingFilter, err := condition.NewExprCondition(s.config.Having)
						if err != nil {
							logger.Error("having filter error: %v", err)
						} else {
							// 应用 HAVING 过滤
							var filteredResults []map[string]interface{}
							for _, result := range finalResults {
								if havingFilter.Evaluate(result) {
									filteredResults = append(filteredResults, result)
								}
							}
							finalResults = filteredResults
						}
					}

					// 应用 LIMIT 限制
					if s.config.Limit > 0 && len(finalResults) > s.config.Limit {
						finalResults = finalResults[:s.config.Limit]
					}

					// 发送结果到结果通道和 Sink 函数
					if len(finalResults) > 0 {
						s.resultChan <- finalResults
						for _, sink := range s.sinks {
							sink(finalResults)
						}
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
		select {
		case data, ok := <-s.dataChan:
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

// processDirectData 直接处理非窗口数据
func (s *Stream) processDirectData(data interface{}) {

	// 简化：直接将数据作为map处理
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		logger.Error("不支持的数据类型: %T", data)
		return
	}

	// 创建结果map
	result := make(map[string]interface{})

	// 如果指定了字段，只保留这些字段
	if len(s.config.SimpleFields) > 0 {
		for _, fieldSpec := range s.config.SimpleFields {
			// 处理别名
			parts := strings.Split(fieldSpec, ":")
			fieldName := parts[0]
			outputName := fieldName
			if len(parts) > 1 {
				outputName = parts[1]
			}

			// 检查是否是函数调用
			if strings.Contains(fieldName, "(") && strings.Contains(fieldName, ")") {
				// 执行函数调用
				if funcResult, err := s.executeFunction(fieldName, dataMap); err == nil {
					result[outputName] = funcResult
				} else {
					logger.Error("函数执行错误 %s: %v", fieldName, err)
					result[outputName] = nil
				}
			} else {
				// 普通字段
				if value, exists := dataMap[fieldName]; exists {
					result[outputName] = value
				}
			}
		}
	} else {
		// 如果没有指定字段，保留所有字段
		for k, v := range dataMap {
			result[k] = v
		}
	}

	// 将结果包装为数组
	results := []map[string]interface{}{result}

	// 发送结果
	s.resultChan <- results
	for _, sink := range s.sinks {
		sink(results)
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

func (s *Stream) AddData(data interface{}) {
	s.dataChan <- data
}

func (s *Stream) AddSink(sink func(interface{})) {
	s.sinks = append(s.sinks, sink)
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
}
