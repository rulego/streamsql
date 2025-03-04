package rsql

import (
	"fmt"
	"strings"
	"time"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/stream"
)

type SelectStatement struct {
	Fields    []Field
	Source    string
	Condition string
	Window    WindowDefinition
	GroupBy   []string
}

type Field struct {
	Expression string
	Alias      string
}

type WindowDefinition struct {
	Type   string
	Params map[string]interface{}
}

// ToStreamConfig 将AST转换为Stream配置
func (s *SelectStatement) ToStreamConfig() (*stream.Config, string, error) {
	if s.Source == "" {
		return nil, "", fmt.Errorf("missing FROM clause")
	}
	// 解析窗口配置
	windowType := "tumbling"
	if s.Window.Type == "Sliding" {
		windowType = "sliding"
	}

	params, err := parseWindowParams(s.Window.Params)
	if err != nil {
		return nil, "", fmt.Errorf("解析窗口参数失败: %w", err)
	}

	// 构建Stream配置
	config := stream.Config{
		WindowConfig: stream.WindowConfig{
			Type:   windowType,
			Params: params,
		},
		GroupFields:  extractGroupFields(s),
		SelectFields: buildSelectFields(s.Fields),
	}

	return &config, s.Condition, nil
}

func extractGroupFields(s *SelectStatement) []string {
	var fields []string
	for _, f := range s.GroupBy {
		if !strings.Contains(f, "(") { // 排除聚合函数
			fields = append(fields, f)
		}
	}
	return fields
}

func buildSelectFields(fields []Field) map[string]aggregator.AggregateType {
	selectFields := make(map[string]aggregator.AggregateType)
	for _, f := range fields {
		if alias := f.Alias; alias != "" {
			selectFields[alias] = parseAggregateType(f.Expression)
		}
	}
	return selectFields
}

func parseAggregateType(expr string) aggregator.AggregateType {
	if strings.Contains(expr, "avg(") {
		return "avg"
	}
	if strings.Contains(expr, "sum(") {
		return "sum"
	}
	if strings.Contains(expr, "max(") {
		return "max"
	}
	if strings.Contains(expr, "min(") {
		return "min"
	}
	return ""
}

func parseWindowParams(params map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	for k, v := range params {
		switch k {
		case "size", "slide":
			if s, ok := v.(string); ok {
				dur, err := time.ParseDuration(s)
				if err != nil {
					return nil, fmt.Errorf("invalid %s duration: %w", k, err)
				}
				result[k] = dur
			} else {
				return nil, fmt.Errorf("%s参数必须为字符串格式(如'5s')", k)
			}
		default:
			result[k] = v
		}
	}

	return result, nil
}

func parseAggregateExpression(expr string) string {
	if strings.Contains(expr, "avg(") {
		return "avg"
	}
	if strings.Contains(expr, "sum(") {
		return "sum"
	}
	if strings.Contains(expr, "max(") {
		return "max"
	}
	if strings.Contains(expr, "min(") {
		return "min"
	}
	return ""
}
