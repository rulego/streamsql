package main

import (
	"fmt"
	"strings"

	"github.com/rulego/streamsql/functions"
)

func main() {
	fmt.Println("🔧 StreamSQL 函数系统整合演示")
	fmt.Println(strings.Repeat("=", 50))

	// 1. Get the bridge
	bridge := functions.GetExprBridge()

	// 2. Prepare test data
	data := map[string]any{
		"temperature": -15.5,
		"humidity":    65.8,
		"device":      "sensor_001",
		"values":      []float64{1.2, -3.4, 5.6, -7.8, 9.0},
		"tags":        []string{"outdoor", "weather", "monitoring"},
		"metadata": map[string]any{
			"location": "北京",
			"type":     "温湿度传感器",
		},
	}

	fmt.Printf("📊 测试数据: %+v\n\n", data)

	// 3. Demonstrate the StreamSQL function
	fmt.Println("🎯 StreamSQL 内置函数演示:")
	testStreamSQLFunctions(bridge, data)

	// 4. Demonstrate the expr-lang function
	fmt.Println("\n🚀 expr-lang 内置函数演示:")
	testExprLangFunctions(bridge, data)

	// 5. Demonstrate mixed use
	fmt.Println("\n🔀 混合函数使用演示:")
	testMixedFunctions(bridge, data)

	// 6. Demonstration of function conflict resolution
	fmt.Println("\n⚖️ 函数冲突解决演示:")
	testFunctionConflicts(bridge, data)

	// 7. Display all available functions
	fmt.Println("\n📋 所有可用函数:")
	showAllFunctions()
}

func testStreamSQLFunctions(bridge *functions.ExprBridge, data map[string]any) {
	tests := []struct {
		name       string
		expression string
		expected   string
	}{
		{"绝对值", "abs(temperature)", "15.5"},
		{"平方根", "sqrt(64)", "8"},
		{"字符串长度", "length(device)", "10"},
		{"字符串连接", "concat(device, \"_processed\")", "sensor_001_processed"},
		{"转大写", "upper(device)", "SENSOR_001"},
		{"当前日期时间", "now()", "日期时间"},
		{"编码", "encode(\"hello\", \"base64\")", "aGVsbG8="},
		{"解码", "decode(\"aGVsbG8=\", \"base64\")", "hello"},
		{"十六进制转换", "hex2dec(\"ff\")", "255"},
		{"数学计算", "power(2, 3)", "8"},
		{"三角函数", "cos(0)", "1"},
	}

	for _, test := range tests {
		result, err := bridge.EvaluateExpression(test.expression, data)
		if err != nil {
			fmt.Printf("   ❌ %s: %s -> 错误: %v\n", test.name, test.expression, err)
		} else {
			fmt.Printf("   ✅ %s: %s -> %v\n", test.name, test.expression, result)
		}
	}
}

func testExprLangFunctions(bridge *functions.ExprBridge, data map[string]any) {
	tests := []struct {
		name       string
		expression string
	}{
		{"数组长度", "len(values)"},
		{"数组过滤", "filter(values, # > 0)"},
		{"数组映射", "map(values, abs(#))"},
		{"字符串处理", "trim(\"  hello world  \")"},
		{"字符串分割", "split(device, \"_\")"},
		{"类型转换", "int(humidity)"},
		{"最大值", "max(values)"},
		{"最小值", "min(values)"},
		{"字符串包含", "\"sensor\" in device"},
		{"条件表达式", "temperature < 0 ? \"冷\" : \"热\""},
	}

	for _, test := range tests {
		result, err := bridge.EvaluateExpression(test.expression, data)
		if err != nil {
			fmt.Printf("   ❌ %s: %s -> 错误: %v\n", test.name, test.expression, err)
		} else {
			fmt.Printf("   ✅ %s: %s -> %v\n", test.name, test.expression, result)
		}
	}
}

func testMixedFunctions(bridge *functions.ExprBridge, data map[string]any) {
	tests := []struct {
		name       string
		expression string
	}{
		{"混合计算1", "abs(temperature) + len(device)"},
		{"混合计算2", "upper(concat(device, \"_\", string(int(humidity))))"},
		{"复杂条件", "len(filter(values, abs(#) > 5)) > 0"},
		{"字符串处理", "length(trim(upper(device)))"},
		{"数值处理", "sqrt(abs(temperature)) + max(values)"},
	}

	for _, test := range tests {
		result, err := bridge.EvaluateExpression(test.expression, data)
		if err != nil {
			fmt.Printf("   ❌ %s: %s -> 错误: %v\n", test.name, test.expression, err)
		} else {
			fmt.Printf("   ✅ %s: %s -> %v\n", test.name, test.expression, result)
		}
	}
}

func testFunctionConflicts(bridge *functions.ExprBridge, data map[string]any) {
	// Test the analysis of the conflict function
	conflictFunctions := []string{"abs", "max", "min", "upper", "lower"}

	for _, funcName := range conflictFunctions {
		_, exists, source := bridge.ResolveFunction(funcName)
		if exists {
			fmt.Printf("   🔍 函数 '%s' 来源: %s\n", funcName, source)
		}
	}

	// Test accessing the StreamSQL version using aliases
	fmt.Println("\n   📝 使用别名访问StreamSQL函数:")
	env := bridge.CreateEnhancedExprEnvironment(data)
	if _, exists := env["streamsql_abs"]; exists {
		fmt.Println("   ✅ streamsql_abs 别名可用")
	}
	if _, exists := env["streamsql_max"]; exists {
		fmt.Println("   ✅ streamsql_max 别名可用")
	}
}

func showAllFunctions() {
	info := functions.GetAllAvailableFunctions()

	// StreamSQL function
	if streamSQLFuncs, ok := info["streamsql"].(map[string]any); ok {
		fmt.Printf("   📦 StreamSQL 函数 (%d个):\n", len(streamSQLFuncs))
		categories := make(map[string][]string)

		for name, funcInfo := range streamSQLFuncs {
			if info, ok := funcInfo.(map[string]any); ok {
				if category, ok := info["type"].(functions.FunctionType); ok {
					categories[string(category)] = append(categories[string(category)], name)
				}
			}
		}

		for category, funcs := range categories {
			fmt.Printf("     %s: %v\n", category, funcs)
		}
	}

	// expr-lang function
	if exprLangFuncs, ok := info["expr-lang"].(map[string]any); ok {
		fmt.Printf("\n   🚀 expr-lang 函数 (%d个):\n", len(exprLangFuncs))
		categories := make(map[string][]string)

		for name, funcInfo := range exprLangFuncs {
			if info, ok := funcInfo.(map[string]any); ok {
				if category, ok := info["category"].(string); ok {
					categories[category] = append(categories[category], name)
				}
			}
		}

		for category, funcs := range categories {
			fmt.Printf("     %s: %v\n", category, funcs)
		}
	}

	fmt.Printf("\n   📊 总计: StreamSQL %d个 + expr-lang %d个 函数\n",
		len(info["streamsql"].(map[string]any)),
		len(info["expr-lang"].(map[string]any)))
}
