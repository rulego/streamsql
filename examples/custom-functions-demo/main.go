package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"time"

	"github.com/rulego/streamsql/utils/cast"

	"github.com/rulego/streamsql"
	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/functions"
)

func main() {
	fmt.Println("🚀 StreamSQL 自定义函数完整演示")
	fmt.Println("=======================================")

	// 注册各种类型的自定义函数
	registerCustomFunctions()

	// 演示自定义函数在SQL中的使用
	demonstrateCustomFunctions()

	// 展示函数管理功能
	demonstrateFunctionManagement()

	fmt.Println("\n✅ 演示完成！")
}

// 注册各种类型的自定义函数
func registerCustomFunctions() {
	fmt.Println("\n📋 注册自定义函数...")

	// 1. 注册数学函数
	registerMathFunctions()

	// 2. 注册字符串函数
	registerStringFunctions()

	// 3. 注册转换函数
	registerConversionFunctions()

	// 4. 注册时间日期函数
	registerDateTimeFunctions()

	// 5. 注册聚合函数
	registerAggregateFunctions()

	// 6. 注册分析函数
	registerAnalyticalFunctions()

	fmt.Println("✅ 所有自定义函数注册完成")
}

// 注册数学函数
func registerMathFunctions() {
	// 距离计算函数
	err := functions.RegisterCustomFunction(
		"distance",
		functions.TypeMath,
		"几何数学",
		"计算两点间距离",
		4, 4,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			x1 := cast.ToFloat64(args[0])
			y1 := cast.ToFloat64(args[1])
			x2 := cast.ToFloat64(args[2])
			y2 := cast.ToFloat64(args[3])

			distance := math.Sqrt(math.Pow(x2-x1, 2) + math.Pow(y2-y1, 2))
			return distance, nil
		},
	)
	checkError("注册distance函数", err)

	// 华氏度转摄氏度函数
	err = functions.RegisterCustomFunction(
		"fahrenheit_to_celsius",
		functions.TypeMath,
		"温度转换",
		"华氏度转摄氏度",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			fahrenheit := cast.ToFloat64(args[0])
			celsius := (fahrenheit - 32) * 5 / 9
			return celsius, nil
		},
	)
	checkError("注册fahrenheit_to_celsius函数", err)

	// 圆面积计算函数
	err = functions.RegisterCustomFunction(
		"circle_area",
		functions.TypeMath,
		"几何计算",
		"计算圆的面积",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			radius := cast.ToFloat64(args[0])
			if radius < 0 {
				return nil, fmt.Errorf("半径必须为正数")
			}
			area := math.Pi * radius * radius
			return area, nil
		},
	)
	checkError("注册circle_area函数", err)

	fmt.Println("  ✓ 数学函数: distance, fahrenheit_to_celsius, circle_area")
}

// 注册字符串函数
func registerStringFunctions() {
	// JSON提取函数
	err := functions.RegisterCustomFunction(
		"json_extract",
		functions.TypeString,
		"JSON处理",
		"从JSON字符串中提取字段值",
		2, 2,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			jsonStr := cast.ToString(args[0])

			path := cast.ToString(args[1])

			var data map[string]any
			if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
				return nil, fmt.Errorf("invalid JSON: %v", err)
			}

			value, exists := data[path]
			if !exists {
				return nil, nil
			}

			return value, nil
		},
	)
	checkError("注册json_extract函数", err)

	// 字符串反转函数
	err = functions.RegisterCustomFunction(
		"reverse_string",
		functions.TypeString,
		"字符串操作",
		"反转字符串",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			str := cast.ToString(args[0])

			runes := []rune(str)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}

			return string(runes), nil
		},
	)
	checkError("注册reverse_string函数", err)

	// 字符串重复函数
	err = functions.RegisterCustomFunction(
		"repeat_string",
		functions.TypeString,
		"字符串操作",
		"重复字符串N次",
		2, 2,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			str := cast.ToString(args[0])

			count := cast.ToInt64(args[1])

			if count < 0 {
				return nil, fmt.Errorf("重复次数不能为负数")
			}

			result := ""
			for i := int64(0); i < count; i++ {
				result += str
			}

			return result, nil
		},
	)
	checkError("注册repeat_string函数", err)

	fmt.Println("  ✓ 字符串函数: json_extract, reverse_string, repeat_string")
}

// 注册转换函数
func registerConversionFunctions() {
	// IP地址转整数函数
	err := functions.RegisterCustomFunction(
		"ip_to_int",
		functions.TypeConversion,
		"网络转换",
		"将IP地址转换为整数",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			ipStr := cast.ToString(args[0])

			ip := net.ParseIP(ipStr)
			if ip == nil {
				return nil, fmt.Errorf("invalid IP address: %s", ipStr)
			}

			ip = ip.To4()
			if ip == nil {
				return nil, fmt.Errorf("not an IPv4 address: %s", ipStr)
			}

			return int64(ip[0])<<24 + int64(ip[1])<<16 + int64(ip[2])<<8 + int64(ip[3]), nil
		},
	)
	checkError("注册ip_to_int函数", err)

	// 字节大小格式化函数
	err = functions.RegisterCustomFunction(
		"format_bytes",
		functions.TypeConversion,
		"数据格式化",
		"格式化字节大小为人类可读格式",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			bytes := cast.ToFloat64(args[0])

			units := []string{"B", "KB", "MB", "GB", "TB"}
			i := 0
			for bytes >= 1024 && i < len(units)-1 {
				bytes /= 1024
				i++
			}

			return fmt.Sprintf("%.2f %s", bytes, units[i]), nil
		},
	)
	checkError("注册format_bytes函数", err)

	fmt.Println("  ✓ 转换函数: ip_to_int, format_bytes")
}

// 注册时间日期函数
func registerDateTimeFunctions() {
	// 时间格式化函数
	err := functions.RegisterCustomFunction(
		"date_format",
		functions.TypeDateTime,
		"时间格式化",
		"格式化时间戳为指定格式",
		2, 2,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			timestamp := cast.ToInt64(args[0])

			format := cast.ToString(args[1])

			t := time.Unix(timestamp, 0)

			switch format {
			case "YYYY-MM-DD":
				return t.Format("2006-01-02"), nil
			case "YYYY-MM-DD HH:mm:ss":
				return t.Format("2006-01-02 15:04:05"), nil
			case "RFC3339":
				return t.Format(time.RFC3339), nil
			default:
				return t.Format(format), nil
			}
		},
	)
	checkError("注册date_format函数", err)

	// 时间差计算函数
	err = functions.RegisterCustomFunction(
		"time_diff",
		functions.TypeDateTime,
		"时间计算",
		"计算两个时间戳的差值（秒）",
		2, 2,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			timestamp1 := cast.ToInt64(args[0])

			timestamp2 := cast.ToInt64(args[1])

			diff := timestamp2 - timestamp1
			return diff, nil
		},
	)
	checkError("注册time_diff函数", err)

	fmt.Println("  ✓ 时间日期函数: date_format, time_diff")
}

// 注册聚合函数
func registerAggregateFunctions() {
	// 注册几何平均数聚合函数到functions模块
	functions.Register(NewGeometricMeanFunction())
	functions.RegisterAggregatorAdapter("geometric_mean")

	// 注册众数聚合函数到functions模块
	functions.Register(NewModeFunction())
	functions.RegisterAggregatorAdapter("mode_agg")

	// 保留原有的aggregator注册用于兼容性
	aggregator.Register("geometric_mean", func() aggregator.AggregatorFunction {
		return &GeometricMeanAggregator{}
	})
	aggregator.Register("mode_agg", func() aggregator.AggregatorFunction {
		return &ModeAggregator{}
	})

	fmt.Println("  ✓ 聚合函数: geometric_mean, mode_agg")
}

// 注册分析函数
func registerAnalyticalFunctions() {
	// 移动平均函数
	err := functions.RegisterCustomFunction(
		"moving_avg",
		functions.TypeAnalytical,
		"移动统计",
		"计算移动平均值",
		2, 2,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			// 这个函数需要状态管理，实际实现会比较复杂
			// 这里只是一个示例
			current := cast.ToFloat64(args[0])

			window := cast.ToInt64(args[1])

			// 简化实现：直接返回当前值
			// 实际实现需要维护历史数据窗口
			_ = window
			return current, nil
		},
	)
	checkError("注册moving_avg函数", err)

	fmt.Println("  ✓ 分析函数: moving_avg")
}

// 几何平均数聚合函数
type GeometricMeanFunction struct {
	*functions.BaseFunction
	product float64
	count   int
}

func NewGeometricMeanFunction() *GeometricMeanFunction {
	return &GeometricMeanFunction{
		BaseFunction: functions.NewBaseFunction(
			"geometric_mean",
			functions.TypeAggregation,
			"统计聚合",
			"计算几何平均数",
			1, -1,
		),
	}
}

func (f *GeometricMeanFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *GeometricMeanFunction) Execute(ctx *functions.FunctionContext, args []any) (any, error) {
	// 批量执行模式
	product := 1.0
	for _, arg := range args {
		val := cast.ToFloat64(arg)
		if val > 0 {
			product *= val
		}
	}
	if len(args) == 0 {
		return 0.0, nil
	}
	return math.Pow(product, 1.0/float64(len(args))), nil
}

// 实现AggregatorFunction接口以支持增量计算
func (f *GeometricMeanFunction) New() functions.AggregatorFunction {
	return &GeometricMeanFunction{
		BaseFunction: f.BaseFunction,
		product:      1.0,
		count:        0,
	}
}

func (f *GeometricMeanFunction) Add(value any) {
	val := cast.ToFloat64(value)
	if val > 0 {
		f.product *= val
		f.count++
	}
}

func (f *GeometricMeanFunction) Result() any {
	if f.count == 0 {
		return 0.0
	}
	return math.Pow(f.product, 1.0/float64(f.count))
}

func (f *GeometricMeanFunction) Reset() {
	f.product = 1.0
	f.count = 0
}

func (f *GeometricMeanFunction) Clone() functions.AggregatorFunction {
	return &GeometricMeanFunction{
		BaseFunction: f.BaseFunction,
		product:      f.product,
		count:        f.count,
	}
}

// 几何平均数聚合器（保留用于兼容性）
type GeometricMeanAggregator struct {
	values []float64
}

func (g *GeometricMeanAggregator) New() aggregator.AggregatorFunction {
	return &GeometricMeanAggregator{
		values: make([]float64, 0),
	}
}

func (g *GeometricMeanAggregator) Add(value any) {
	if val, err := cast.ToFloat64E(value); err == nil && val > 0 {
		g.values = append(g.values, val)
	}
}

func (g *GeometricMeanAggregator) Result() any {
	if len(g.values) == 0 {
		return 0.0
	}

	product := 1.0
	for _, v := range g.values {
		product *= v
	}

	return math.Pow(product, 1.0/float64(len(g.values)))
}

// 众数聚合函数
type ModeFunction struct {
	*functions.BaseFunction
	counts map[string]int
}

func NewModeFunction() *ModeFunction {
	return &ModeFunction{
		BaseFunction: functions.NewBaseFunction(
			"mode_agg",
			functions.TypeAggregation,
			"统计聚合",
			"计算众数",
			1, -1,
		),
		counts: make(map[string]int),
	}
}

func (f *ModeFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ModeFunction) Execute(ctx *functions.FunctionContext, args []any) (any, error) {
	// 批量执行模式
	counts := make(map[string]int)
	for _, arg := range args {
		key := fmt.Sprintf("%v", arg)
		counts[key]++
	}

	if len(counts) == 0 {
		return nil, nil
	}

	maxCount := 0
	var mode any
	for key, count := range counts {
		if count > maxCount {
			maxCount = count
			mode = key
		}
	}
	return mode, nil
}

// 实现AggregatorFunction接口以支持增量计算
func (f *ModeFunction) New() functions.AggregatorFunction {
	return &ModeFunction{
		BaseFunction: f.BaseFunction,
		counts:       make(map[string]int),
	}
}

func (f *ModeFunction) Add(value any) {
	key := fmt.Sprintf("%v", value)
	f.counts[key]++
}

func (f *ModeFunction) Result() any {
	if len(f.counts) == 0 {
		return nil
	}

	maxCount := 0
	var mode any
	for key, count := range f.counts {
		if count > maxCount {
			maxCount = count
			mode = key
		}
	}
	return mode
}

func (f *ModeFunction) Reset() {
	f.counts = make(map[string]int)
}

func (f *ModeFunction) Clone() functions.AggregatorFunction {
	clone := &ModeFunction{
		BaseFunction: f.BaseFunction,
		counts:       make(map[string]int),
	}
	for k, v := range f.counts {
		clone.counts[k] = v
	}
	return clone
}

// 众数聚合器（保留用于兼容性）
type ModeAggregator struct {
	counts map[string]int
}

func (m *ModeAggregator) New() aggregator.AggregatorFunction {
	return &ModeAggregator{
		counts: make(map[string]int),
	}
}

func (m *ModeAggregator) Add(value any) {
	key := fmt.Sprintf("%v", value)
	m.counts[key]++
}

func (m *ModeAggregator) Result() any {
	if len(m.counts) == 0 {
		return nil
	}

	maxCount := 0
	var mode any
	for key, count := range m.counts {
		if count > maxCount {
			maxCount = count
			mode = key
		}
	}
	return mode
}

// 演示自定义函数在SQL中的使用
func demonstrateCustomFunctions() {
	fmt.Println("\n🎯 演示自定义函数在SQL中的使用")
	fmt.Println("================================")

	ssql := streamsql.New()
	defer ssql.Stop()

	// 测试数学函数
	testMathFunctions(ssql)

	// 测试字符串函数
	testStringFunctions(ssql)

	// 测试转换函数
	testConversionFunctions(ssql)

	// 测试聚合函数
	testAggregateFunctions(ssql)
}

func testMathFunctions(ssql *streamsql.Streamsql) {
	fmt.Println("\n📐 测试数学函数...")

	sql := `
		SELECT 
			device,
			AVG(fahrenheit_to_celsius(temperature)) as avg_celsius,
			AVG(circle_area(radius)) as avg_area,
			AVG(distance(x1, y1, x2, y2)) as avg_distance
		FROM stream 
		GROUP BY device, TumblingWindow('1s')
	`

	err := ssql.Execute(sql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	// 添加测试数据
	testData := []map[string]any{
		{
			"device":      "sensor1",
			"temperature": 68.0, // 华氏度
			"radius":      5.0,
			"x1":          0.0, "y1": 0.0, "x2": 3.0, "y2": 4.0, // 距离=5
		},
		{
			"device":      "sensor1",
			"temperature": 86.0, // 华氏度
			"radius":      10.0,
			"x1":          0.0, "y1": 0.0, "x2": 6.0, "y2": 8.0, // 距离=10
		},
	}

	// 添加结果监听器
	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  📊 数学函数结果: %v\n", result)
	})

	for _, data := range testData {
		ssql.Emit(data)
	}

	time.Sleep(1 * time.Second)
	ssql.Stream().Window.Trigger()
	time.Sleep(500 * time.Millisecond)

	fmt.Println("  ✅ 数学函数测试完成")
}

func testStringFunctions(ssql *streamsql.Streamsql) {
	fmt.Println("\n📝 测试字符串函数...")

	sql := `
		SELECT 
			device,
			json_extract(metadata, 'version') as version,
			reverse_string(device) as reversed_device,
			repeat_string('*', level) as stars
		FROM stream
	`

	err := ssql.Execute(sql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	// 添加测试数据
	testData := []map[string]any{
		{
			"device":   "sensor1",
			"metadata": `{"version":"1.0","type":"temperature"}`,
			"level":    3,
		},
		{
			"device":   "sensor2",
			"metadata": `{"version":"2.0","type":"humidity"}`,
			"level":    5,
		},
	}

	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  📊 字符串函数结果: %v\n", result)
	})

	for _, data := range testData {
		ssql.Emit(data)
	}

	time.Sleep(500 * time.Millisecond)
	fmt.Println("  ✅ 字符串函数测试完成")
}

func testConversionFunctions(ssql *streamsql.Streamsql) {
	fmt.Println("\n🔄 测试转换函数...")

	sql := `
		SELECT 
			device,
			ip_to_int(client_ip) as ip_int,
			format_bytes(memory_usage) as formatted_memory
		FROM stream
	`

	err := ssql.Execute(sql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	// 添加测试数据
	testData := []map[string]any{
		{
			"device":       "server1",
			"client_ip":    "192.168.1.100",
			"memory_usage": 1073741824, // 1GB
		},
		{
			"device":       "server2",
			"client_ip":    "10.0.0.50",
			"memory_usage": 2147483648, // 2GB
		},
	}

	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  📊 转换函数结果: %v\n", result)
	})

	for _, data := range testData {
		ssql.Emit(data)
	}

	time.Sleep(500 * time.Millisecond)
	fmt.Println("  ✅ 转换函数测试完成")
}

func testAggregateFunctions(ssql *streamsql.Streamsql) {
	fmt.Println("\n📈 测试聚合函数...")

	sql := `
		SELECT 
			device,
			geometric_mean(value) as geo_mean,
			mode_agg(category) as most_common
		FROM stream 
		GROUP BY device, TumblingWindow('1s')
	`

	err := ssql.Execute(sql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	// 添加测试数据
	testData := []map[string]any{
		{"device": "sensor1", "value": 2.0, "category": "A"},
		{"device": "sensor1", "value": 8.0, "category": "A"},
		{"device": "sensor1", "value": 32.0, "category": "B"},
		{"device": "sensor1", "value": 128.0, "category": "A"},
	}

	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  📊 聚合函数结果: %v\n", result)
	})

	for _, data := range testData {
		ssql.Emit(data)
	}

	time.Sleep(1 * time.Second)
	ssql.Stream().Window.Trigger()
	time.Sleep(500 * time.Millisecond)

	fmt.Println("  ✅ 聚合函数测试完成")
}

// 展示函数管理功能
func demonstrateFunctionManagement() {
	fmt.Println("\n🔧 演示函数管理功能")
	fmt.Println("====================")

	// 列出所有函数
	fmt.Println("\n📋 所有已注册函数:")
	allFunctions := functions.ListAll()

	// 按类型分组显示
	typeMap := make(map[functions.FunctionType][]functions.Function)
	for _, fn := range allFunctions {
		fnType := fn.GetType()
		typeMap[fnType] = append(typeMap[fnType], fn)
	}

	for fnType, funcs := range typeMap {
		fmt.Printf("\n  📂 %s:\n", fnType)
		for _, fn := range funcs {
			fmt.Printf("    • %s - %s\n", fn.GetName(), fn.GetDescription())
		}
	}

	// 演示函数查找
	fmt.Println("\n🔍 函数查找示例:")
	if fn, exists := functions.Get("fahrenheit_to_celsius"); exists {
		fmt.Printf("  ✓ 找到函数: %s (%s)\n", fn.GetName(), fn.GetDescription())
	}

	// 演示按类型获取函数
	fmt.Println("\n📊 数学函数列表:")
	mathFunctions := functions.GetByType(functions.TypeMath)
	for _, fn := range mathFunctions {
		fmt.Printf("  • %s\n", fn.GetName())
	}

	fmt.Println("\n📈 聚合函数列表:")
	aggFunctions := functions.GetByType(functions.TypeAggregation)
	for _, fn := range aggFunctions {
		fmt.Printf("  • %s\n", fn.GetName())
	}
}

// 辅助函数
func checkError(operation string, err error) {
	if err != nil {
		fmt.Printf("❌ %s失败: %v\n", operation, err)
	}
}
