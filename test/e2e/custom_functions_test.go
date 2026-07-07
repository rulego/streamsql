package streamsql

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	"github.com/rulego/streamsql/utils/cast"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/expr"
	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/rsql"
	"github.com/stretchr/testify/assert"
)

// TestCustomMathFunctions 测试自定义数学函数
func TestCustomMathFunctions(t *testing.T) {
	// 注册平方函数
	err := functions.RegisterCustomFunction(
		"square",
		functions.TypeMath,
		"数学函数",
		"计算平方",
		1, 1,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
			val := cast.ToFloat64(args[0])
			return val * val, nil
		},
	)
	assert.NoError(t, err)
	defer functions.Unregister("square")

	// 注册距离计算函数
	err = functions.RegisterCustomFunction(
		"distance",
		functions.TypeMath,
		"几何数学",
		"计算两点间距离",
		4, 4,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
			x1 := cast.ToFloat64(args[0])
			y1 := cast.ToFloat64(args[1])
			x2 := cast.ToFloat64(args[2])
			y2 := cast.ToFloat64(args[3])

			distance := math.Sqrt(math.Pow(x2-x1, 2) + math.Pow(y2-y1, 2))
			return distance, nil
		},
	)
	assert.NoError(t, err)
	defer functions.Unregister("distance")

	// 测试在SQL中使用
	streamsql := New()
	defer streamsql.Stop()

	sql := `
		SELECT 
			device,
			AVG(square(value)) as squared_value,
			AVG(distance(x1, y1, x2, y2)) as calculated_distance
		FROM stream 
		GROUP BY device, TumblingWindow('1s')
	`

	err = streamsql.Execute(sql)
	assert.NoError(t, err)

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)
	streamsql.AddSink(func(result []map[string]interface{}) {
		resultChan <- result
	})

	// 添加测试数据
	testData := map[string]interface{}{
		"device": "sensor1",
		"value":  5.0,
		"x1":     0.0,
		"y1":     0.0,
		"x2":     3.0,
		"y2":     4.0, // 距离应该是5
	}

	streamsql.Emit(testData)

	// 等待窗口触发
	time.Sleep(1 * time.Second)
	streamsql.Stream().Window.Trigger()
	time.Sleep(500 * time.Millisecond)

	// 验证结果
	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]interface{})
		assert.True(t, ok)
		assert.Len(t, resultSlice, 1)

		item := resultSlice[0]
		assert.Equal(t, "sensor1", item["device"])
		assert.Equal(t, 25.0, item["squared_value"])      // 5^2 = 25
		assert.Equal(t, 5.0, item["calculated_distance"]) // sqrt((3-0)^2 + (4-0)^2) = 5
	case <-time.After(2 * time.Second):
		t.Fatal("测试超时")
	}
}

// TestCustomStringFunctions 测试自定义字符串函数
func TestCustomStringFunctions(t *testing.T) {
	// 注册字符串反转函数
	err := functions.RegisterCustomFunction(
		"reverse_str",
		functions.TypeString,
		"字符串函数",
		"反转字符串",
		1, 1,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
			str := cast.ToString(args[0])

			runes := []rune(str)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}

			return string(runes), nil
		},
	)
	assert.NoError(t, err)
	defer functions.Unregister("reverse_str")

	// 注册JSON提取函数
	err = functions.RegisterCustomFunction(
		"json_get",
		functions.TypeString,
		"JSON处理",
		"从JSON字符串中提取字段值",
		2, 2,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
			jsonStr := cast.ToString(args[0])

			key := cast.ToString(args[1])

			var data map[string]interface{}
			if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
				return nil, fmt.Errorf("invalid JSON: %v", err)
			}

			value, exists := data[key]
			if !exists {
				return nil, nil
			}

			return value, nil
		},
	)
	assert.NoError(t, err)
	defer functions.Unregister("json_get")

	// 测试在SQL中使用
	streamsql := New()
	defer streamsql.Stop()

	sql := `
		SELECT 
			device,
			reverse_str(device) as reversed_device,
			json_get(metadata, 'version') as version
		FROM stream
	`

	err = streamsql.Execute(sql)
	assert.NoError(t, err)

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)
	streamsql.AddSink(func(result []map[string]interface{}) {
		resultChan <- result
	})

	// 添加测试数据
	testData := map[string]interface{}{
		"device":   "sensor1",
		"metadata": `{"version":"1.0","type":"temperature"}`,
	}

	streamsql.Emit(testData)
	time.Sleep(200 * time.Millisecond)

	// 验证结果
	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]interface{})
		assert.True(t, ok)
		assert.Len(t, resultSlice, 1)

		item := resultSlice[0]
		assert.Equal(t, "sensor1", item["device"])
		assert.Equal(t, "1rosnes", item["reversed_device"]) // "sensor1" 反转
		assert.Equal(t, "1.0", item["version"])
	case <-time.After(2 * time.Second):
		t.Fatal("测试超时")
	}
}

// TestCustomConversionFunctions 测试自定义转换函数
func TestCustomConversionFunctions(t *testing.T) {
	// 注册IP地址转换函数
	err := functions.RegisterCustomFunction(
		"ip_to_num",
		functions.TypeConversion,
		"网络转换",
		"将IP地址转换为整数",
		1, 1,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
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
	assert.NoError(t, err)
	defer functions.Unregister("ip_to_num")

	// 注册字节大小格式化函数
	err = functions.RegisterCustomFunction(
		"format_bytes",
		functions.TypeConversion,
		"数据格式化",
		"格式化字节大小为人类可读格式",
		1, 1,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
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
	assert.NoError(t, err)
	defer functions.Unregister("format_bytes")

	// 测试函数直接调用
	ctx := &functions.FunctionContext{Data: make(map[string]interface{})}

	// 测试IP转换
	ipFunc, exists := functions.Get("ip_to_num")
	assert.True(t, exists)

	result, err := ipFunc.Execute(ctx, []interface{}{"192.168.1.100"})
	assert.NoError(t, err)
	expectedIP := int64(192)<<24 + int64(168)<<16 + int64(1)<<8 + int64(100)
	assert.Equal(t, expectedIP, result)

	// 测试字节格式化
	bytesFunc, exists := functions.Get("format_bytes")
	assert.True(t, exists)

	result, err = bytesFunc.Execute(ctx, []interface{}{1073741824}) // 1GB
	assert.NoError(t, err)
	assert.Equal(t, "1.00 GB", result)
}

// TestCustomAggregateFunctions 测试自定义聚合函数
func TestCustomAggregateFunctions(t *testing.T) {
	// 注册几何平均数聚合函数
	functions.Register(NewGeometricMeanFunction())
	aggregator.Register("geometric_mean", func() aggregator.AggregatorFunction {
		return &GeometricMeanAggregator{}
	})
	defer functions.Unregister("geometric_mean")

	// 注册众数聚合函数
	functions.Register(NewModeFunction())
	aggregator.Register("mode_value", func() aggregator.AggregatorFunction {
		return &ModeAggregator{}
	})
	defer functions.Unregister("mode_value")

	// 测试在SQL中使用
	streamsql := New()
	defer streamsql.Stop()

	sql := `
		SELECT 
			device,
			geometric_mean(value) as geo_mean,
			mode_value(category) as most_common
		FROM stream 
		GROUP BY device, TumblingWindow('1s')
	`

	err := streamsql.Execute(sql)
	assert.NoError(t, err)

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)
	streamsql.AddSink(func(result []map[string]interface{}) {
		resultChan <- result
	})

	// 添加测试数据
	testData := []map[string]interface{}{
		{"device": "sensor1", "value": 2.0, "category": "A"},
		{"device": "sensor1", "value": 8.0, "category": "A"},
		{"device": "sensor1", "value": 32.0, "category": "B"},
		{"device": "sensor1", "value": 128.0, "category": "A"},
	}

	for _, data := range testData {
		streamsql.Emit(data)
	}

	time.Sleep(1 * time.Second)
	streamsql.Stream().Window.Trigger()
	time.Sleep(500 * time.Millisecond)

	// 验证结果
	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]interface{})
		assert.True(t, ok)
		assert.Len(t, resultSlice, 1)

		item := resultSlice[0]
		assert.Equal(t, "sensor1", item["device"])

		// 几何平均数: (2 * 8 * 32 * 128) ^ (1/4) = 16
		geoMean, ok := item["geo_mean"].(float64)
		assert.True(t, ok)
		assert.InEpsilon(t, 16.0, geoMean, 0.01)

		// 众数: A出现3次，B出现1次，所以众数是A
		mode := item["most_common"]
		assert.Equal(t, "A", mode)

	case <-time.After(3 * time.Second):
		t.Fatal("测试超时")
	}
}

// GeometricMeanFunction 几何平均数函数
type GeometricMeanFunction struct {
	*functions.BaseFunction
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

func (f *GeometricMeanFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *GeometricMeanFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
	return nil, nil // 实际逻辑在聚合器中
}

// GeometricMeanAggregator 几何平均数聚合器
type GeometricMeanAggregator struct {
	values []float64
}

func (g *GeometricMeanAggregator) New() aggregator.AggregatorFunction {
	return &GeometricMeanAggregator{values: make([]float64, 0)}
}

func (g *GeometricMeanAggregator) Add(value interface{}) {
	if val := cast.ToFloat64(value); val > 0 {
		g.values = append(g.values, val)
	}
}

func (g *GeometricMeanAggregator) Result() interface{} {
	if len(g.values) == 0 {
		return 0.0
	}

	product := 1.0
	for _, v := range g.values {
		product *= v
	}

	return math.Pow(product, 1.0/float64(len(g.values)))
}

// ModeFunction 众数函数
type ModeFunction struct {
	*functions.BaseFunction
}

func NewModeFunction() *ModeFunction {
	return &ModeFunction{
		BaseFunction: functions.NewBaseFunction(
			"mode_value",
			functions.TypeAggregation,
			"统计聚合",
			"计算众数",
			1, -1,
		),
	}
}

func (f *ModeFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ModeFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
	return nil, nil // 实际逻辑在聚合器中
}

// ModeAggregator 众数聚合器
type ModeAggregator struct {
	counts map[string]int
}

func (m *ModeAggregator) New() aggregator.AggregatorFunction {
	return &ModeAggregator{counts: make(map[string]int)}
}

func (m *ModeAggregator) Add(value interface{}) {
	key := fmt.Sprintf("%v", value)
	m.counts[key]++
}

func (m *ModeAggregator) Result() interface{} {
	if len(m.counts) == 0 {
		return nil
	}

	maxCount := 0
	var mode interface{}

	for key, count := range m.counts {
		if count > maxCount {
			maxCount = count
			mode = key
		}
	}

	return mode
}

// TestFunctionManagement 测试函数管理功能
func TestFunctionManagement(t *testing.T) {
	// 注册测试函数
	err := functions.RegisterCustomFunction(
		"test_func",
		functions.TypeCustom,
		"测试函数",
		"用于测试的函数",
		1, 1,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
			return args[0], nil
		},
	)
	assert.NoError(t, err)

	// 测试函数查找
	fn, exists := functions.Get("test_func")
	assert.True(t, exists)
	assert.Equal(t, "test_func", fn.GetName())
	assert.Equal(t, functions.TypeCustom, fn.GetType())

	// 测试函数列表
	allFunctions := functions.ListAll()
	assert.Contains(t, allFunctions, "test_func")

	// 测试按类型获取
	customFunctions := functions.GetByType(functions.TypeCustom)
	found := false
	for _, f := range customFunctions {
		if f.GetName() == "test_func" {
			found = true
			break
		}
	}
	assert.True(t, found)

	// 测试函数注销
	success := functions.Unregister("test_func")
	assert.True(t, success)

	// 验证函数已被注销
	_, exists = functions.Get("test_func")
	assert.False(t, exists)
}

// TestCustomFunctionWithAggregation 测试自定义函数与聚合函数结合使用
func TestCustomFunctionWithAggregation(t *testing.T) {
	// 注册温度转换函数
	err := functions.RegisterCustomFunction(
		"celsius_to_fahrenheit",
		functions.TypeConversion,
		"温度转换",
		"摄氏度转华氏度",
		1, 1,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
			celsius := cast.ToFloat64(args[0])
			fahrenheit := celsius*9/5 + 32
			return fahrenheit, nil
		},
	)
	assert.NoError(t, err)
	defer functions.Unregister("celsius_to_fahrenheit")

	// 测试在聚合SQL中使用
	streamsql := New()
	defer streamsql.Stop()

	sql := `
		SELECT 
			device,
			AVG(celsius_to_fahrenheit(temperature)) as avg_fahrenheit,
			MAX(celsius_to_fahrenheit(temperature)) as max_fahrenheit
		FROM stream 
		GROUP BY device, TumblingWindow('1s')
	`

	err = streamsql.Execute(sql)
	assert.NoError(t, err)

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)
	streamsql.AddSink(func(result []map[string]interface{}) {
		resultChan <- result
	})

	// 添加测试数据（摄氏度）
	testData := []map[string]interface{}{
		{"device": "thermometer", "temperature": 0.0},   // 32°F
		{"device": "thermometer", "temperature": 100.0}, // 212°F
	}

	for _, data := range testData {
		streamsql.Emit(data)
	}

	time.Sleep(1 * time.Second)
	streamsql.Stream().Window.Trigger()
	time.Sleep(500 * time.Millisecond)

	// 验证结果
	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]interface{})
		assert.True(t, ok)
		assert.Len(t, resultSlice, 1)

		item := resultSlice[0]
		assert.Equal(t, "thermometer", item["device"])

		// 平均华氏度: (32 + 212) / 2 = 122
		avgFahrenheit, ok := item["avg_fahrenheit"].(float64)
		assert.True(t, ok)
		assert.InEpsilon(t, 122.0, avgFahrenheit, 0.01)

		// 最大华氏度: 212
		maxFahrenheit, ok := item["max_fahrenheit"].(float64)
		assert.True(t, ok)
		assert.InEpsilon(t, 212.0, maxFahrenheit, 0.01)

	case <-time.After(3 * time.Second):
		t.Fatal("测试超时")
	}
}

// TestDebugCustomFunctions 调试自定义函数问题
func TestDebugCustomFunctions(t *testing.T) {
	// 注册简单的平方函数
	err := functions.RegisterCustomFunction(
		"square",
		functions.TypeMath,
		"数学函数",
		"计算平方",
		1, 1,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
			val := cast.ToFloat64(args[0])
			fmt.Printf("Square function called with: %v, result: %v\n", val, val*val)
			return val * val, nil
		},
	)
	assert.NoError(t, err)
	defer functions.Unregister("square")

	// 测试函数是否能被找到
	fn, exists := functions.Get("square")
	assert.True(t, exists)
	fmt.Printf("Function found: %s, type: %s\n", fn.GetName(), fn.GetType())

	// 测试表达式解析
	expr, err := expr.NewExpression("square(value)")
	assert.NoError(t, err)

	// 获取表达式字段
	fields := expr.GetFields()
	fmt.Printf("Expression fields: %v\n", fields)

	// 测试表达式计算
	data := map[string]interface{}{"value": 5.0}
	result, err := expr.Evaluate(data)
	assert.NoError(t, err)
	fmt.Printf("Expression result: %v\n", result)
	assert.Equal(t, 25.0, result)

	// 测试SQL解析
	parser := rsql.NewParser("SELECT square(value) as squared FROM stream")
	stmt, err := parser.Parse()
	assert.NoError(t, err)

	_, _, err = stmt.ToStreamConfig()
	assert.NoError(t, err)
}

// TestDebugMultiParameterFunction 测试多参数自定义函数
func TestDebugMultiParameterFunction(t *testing.T) {
	// 注册距离计算函数
	err := functions.RegisterCustomFunction(
		"distance",
		functions.TypeMath,
		"几何数学",
		"计算两点间距离",
		4, 4,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
			x1 := cast.ToFloat64(args[0])
			y1 := cast.ToFloat64(args[1])
			x2 := cast.ToFloat64(args[2])
			y2 := cast.ToFloat64(args[3])

			distance := math.Sqrt(math.Pow(x2-x1, 2) + math.Pow(y2-y1, 2))
			fmt.Printf("Distance function called with: (%v,%v) to (%v,%v), result: %v\n", x1, y1, x2, y2, distance)
			return distance, nil
		},
	)
	assert.NoError(t, err)
	defer functions.Unregister("distance")

	// 测试表达式解析
	expr, err := expr.NewExpression("distance(x1, y1, x2, y2)")
	assert.NoError(t, err)

	// 测试表达式计算
	data := map[string]interface{}{
		"x1": 0.0,
		"y1": 0.0,
		"x2": 3.0,
		"y2": 4.0,
	}
	result, err := expr.Evaluate(data)
	assert.NoError(t, err)
	assert.Equal(t, 5.0, result)

	// 测试SQL解析
	parser := rsql.NewParser("SELECT AVG(distance(x1, y1, x2, y2)) as avg_distance FROM stream GROUP BY device, TumblingWindow('1s')")
	stmt, err := parser.Parse()
	assert.NoError(t, err)

	_, _, err = stmt.ToStreamConfig()
	assert.NoError(t, err)
}
