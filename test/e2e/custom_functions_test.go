package e2e

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/rulego/streamsql/utils/cast"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/expr"
	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/rsql"
	"github.com/stretchr/testify/assert"
)

// This file tests serial execution (without t.Parallel): registering a custom function in the global function registry,
// There are duplicate registrations across tests/files (square/distance/get_type, etc.), and parallelism will cause "already registered" conflicts.

// TestCustomMathFunctions Test custom math functions
func TestCustomMathFunctions(t *testing.T) {
	// Register the square function
	err := functions.RegisterCustomFunction(
		"square",
		functions.TypeMath,
		"数学函数",
		"计算平方",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			val := cast.ToFloat64(args[0])
			return val * val, nil
		},
	)
	assert.NoError(t, err)
	defer functions.Unregister("square")

	// Register the distance calculation function
	err = functions.RegisterCustomFunction(
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
	assert.NoError(t, err)
	defer functions.Unregister("distance")

	// Testing is used in SQL
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
		SELECT 
			device,
			AVG(square(value)) as squared_value,
			AVG(distance(x1, y1, x2, y2)) as calculated_distance
		FROM stream 
		GROUP BY device, TumblingWindow('1s')
	`

	err = ssql.Execute(sql)
	assert.NoError(t, err)

	// Create a result receiving channel
	resultChan := make(chan any, 10)
	ssql.AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add test data
	testData := map[string]any{
		"device": "sensor1",
		"value":  5.0,
		"x1":     0.0,
		"y1":     0.0,
		"x2":     3.0,
		"y2":     4.0, // The distance should be 5
	}

	ssql.Emit(testData)

	// Wait for the window to trigger
	time.Sleep(1 * time.Second)
	ssql.TriggerWindow()
	time.Sleep(500 * time.Millisecond)

	// Verify the results
	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]any)
		assert.True(t, ok)
		assert.Len(t, resultSlice, 1)

		item := resultSlice[0]
		assert.Equal(t, "sensor1", item["device"])
		assert.Equal(t, 25.0, item["squared_value"])      // 5^2 = 25
		assert.Equal(t, 5.0, item["calculated_distance"]) // sqrt((3-0)^2 + (4-0)^2) = 5
	case <-time.After(2 * time.Second):
		t.Fatal("Test timeout")
	}
}

// TestCustomStringFunctions Tests custom string functions
func TestCustomStringFunctions(t *testing.T) {
	// Register the string invert function
	err := functions.RegisterCustomFunction(
		"reverse_str",
		functions.TypeString,
		"字符串函数",
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
	assert.NoError(t, err)
	defer functions.Unregister("reverse_str")

	// Register the JSON extraction function
	err = functions.RegisterCustomFunction(
		"json_get",
		functions.TypeString,
		"JSON处理",
		"从JSON字符串中提取字段值",
		2, 2,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			jsonStr := cast.ToString(args[0])

			key := cast.ToString(args[1])

			var data map[string]any
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

	// Testing is used in SQL
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
		SELECT 
			device,
			reverse_str(device) as reversed_device,
			json_get(metadata, 'version') as version
		FROM stream
	`

	err = ssql.Execute(sql)
	assert.NoError(t, err)

	// Create a result receiving channel
	resultChan := make(chan any, 10)
	ssql.AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add test data
	testData := map[string]any{
		"device":   "sensor1",
		"metadata": `{"version":"1.0","type":"temperature"}`,
	}

	ssql.Emit(testData)
	time.Sleep(200 * time.Millisecond)

	// Verify the results
	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]any)
		assert.True(t, ok)
		assert.Len(t, resultSlice, 1)

		item := resultSlice[0]
		assert.Equal(t, "sensor1", item["device"])
		assert.Equal(t, "1rosnes", item["reversed_device"]) // "sensor1" reverses
		assert.Equal(t, "1.0", item["version"])
	case <-time.After(2 * time.Second):
		t.Fatal("Test timeout")
	}
}

// TestCustomConversionFunctions Test the custom conversion function
func TestCustomConversionFunctions(t *testing.T) {
	// Register the IP address translation function
	err := functions.RegisterCustomFunction(
		"ip_to_num",
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
	assert.NoError(t, err)
	defer functions.Unregister("ip_to_num")

	// Register byte size formatting function
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
	assert.NoError(t, err)
	defer functions.Unregister("format_bytes")

	// The test function is called directly
	ctx := &functions.FunctionContext{Data: make(map[string]any)}

	// Testing IP conversion
	ipFunc, exists := functions.Get("ip_to_num")
	assert.True(t, exists)

	result, err := ipFunc.Execute(ctx, []any{"192.168.1.100"})
	assert.NoError(t, err)
	expectedIP := int64(192)<<24 + int64(168)<<16 + int64(1)<<8 + int64(100)
	assert.Equal(t, expectedIP, result)

	// Test byte formatting
	bytesFunc, exists := functions.Get("format_bytes")
	assert.True(t, exists)

	result, err = bytesFunc.Execute(ctx, []any{1073741824}) // 1GB
	assert.NoError(t, err)
	assert.Equal(t, "1.00 GB", result)
}

// TestCustomAggregateFunctions Test custom aggregate functions
func TestCustomAggregateFunctions(t *testing.T) {
	// Register the geometric mean aggregation function
	functions.Register(NewGeometricMeanFunction())
	aggregator.Register("geometric_mean", func() aggregator.AggregatorFunction {
		return &GeometricMeanAggregator{}
	})
	defer functions.Unregister("geometric_mean")

	// Register the mode aggregation function
	functions.Register(NewModeFunction())
	aggregator.Register("mode_value", func() aggregator.AggregatorFunction {
		return &ModeAggregator{}
	})
	defer functions.Unregister("mode_value")

	// Testing is used in SQL
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
		SELECT 
			device,
			geometric_mean(value) as geo_mean,
			mode_value(category) as most_common
		FROM stream 
		GROUP BY device, TumblingWindow('1s')
	`

	err := ssql.Execute(sql)
	assert.NoError(t, err)

	// Create a result receiving channel
	resultChan := make(chan any, 10)
	ssql.AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add test data
	testData := []map[string]any{
		{"device": "sensor1", "value": 2.0, "category": "A"},
		{"device": "sensor1", "value": 8.0, "category": "A"},
		{"device": "sensor1", "value": 32.0, "category": "B"},
		{"device": "sensor1", "value": 128.0, "category": "A"},
	}

	for _, data := range testData {
		ssql.Emit(data)
	}

	time.Sleep(1 * time.Second)
	ssql.TriggerWindow()
	time.Sleep(500 * time.Millisecond)

	// Verify the results
	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]any)
		assert.True(t, ok)
		assert.Len(t, resultSlice, 1)

		item := resultSlice[0]
		assert.Equal(t, "sensor1", item["device"])

		// Geometric mean: (2 * 8 * 32 * 128) ^ (1/4) = 16
		geoMean, ok := item["geo_mean"].(float64)
		assert.True(t, ok)
		assert.InEpsilon(t, 16.0, geoMean, 0.01)

		// Mode: A appears 3 times, B appears 1 time, so the mode is A
		mode := item["most_common"]
		assert.Equal(t, "A", mode)

	case <-time.After(3 * time.Second):
		t.Fatal("Test timeout")
	}
}

// GeometricMeanFunction Geometric mean function
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

func (f *GeometricMeanFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *GeometricMeanFunction) Execute(ctx *functions.FunctionContext, args []any) (any, error) {
	return nil, nil // The actual logic is in the aggregator
}

// GeometricMeanAggregator: Geometric mean aggregator
type GeometricMeanAggregator struct {
	values []float64
}

func (g *GeometricMeanAggregator) New() aggregator.AggregatorFunction {
	return &GeometricMeanAggregator{values: make([]float64, 0)}
}

func (g *GeometricMeanAggregator) Add(value any) {
	if val := cast.ToFloat64(value); val > 0 {
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

// ModeFunction mode function
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

func (f *ModeFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ModeFunction) Execute(ctx *functions.FunctionContext, args []any) (any, error) {
	return nil, nil // The actual logic is in the aggregator
}

// ModeAggregator
type ModeAggregator struct {
	counts map[string]int
}

func (m *ModeAggregator) New() aggregator.AggregatorFunction {
	return &ModeAggregator{counts: make(map[string]int)}
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

// TestFunctionManagement is a test function management feature
func TestFunctionManagement(t *testing.T) {
	// Register the test function
	err := functions.RegisterCustomFunction(
		"test_func",
		functions.TypeCustom,
		"测试函数",
		"用于测试的函数",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			return args[0], nil
		},
	)
	assert.NoError(t, err)

	// Test function search
	fn, exists := functions.Get("test_func")
	assert.True(t, exists)
	assert.Equal(t, "test_func", fn.GetName())
	assert.Equal(t, functions.TypeCustom, fn.GetType())

	// Test the function list
	allFunctions := functions.ListAll()
	assert.Contains(t, allFunctions, "test_func")

	// Tests are obtained by type
	customFunctions := functions.GetByType(functions.TypeCustom)
	found := false
	for _, f := range customFunctions {
		if f.GetName() == "test_func" {
			found = true
			break
		}
	}
	assert.True(t, found)

	// Test function logout
	success := functions.Unregister("test_func")
	assert.True(t, success)

	// The validation function has been logged off
	_, exists = functions.Get("test_func")
	assert.False(t, exists)
}

// TestCustomFunctionWithAggregation Test custom functions are used together with aggregation functions
func TestCustomFunctionWithAggregation(t *testing.T) {
	// Register the temperature conversion function
	err := functions.RegisterCustomFunction(
		"celsius_to_fahrenheit",
		functions.TypeConversion,
		"温度转换",
		"摄氏度转华氏度",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			celsius := cast.ToFloat64(args[0])
			fahrenheit := celsius*9/5 + 32
			return fahrenheit, nil
		},
	)
	assert.NoError(t, err)
	defer functions.Unregister("celsius_to_fahrenheit")

	// Testing is used in aggregated SQL
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
		SELECT 
			device,
			AVG(celsius_to_fahrenheit(temperature)) as avg_fahrenheit,
			MAX(celsius_to_fahrenheit(temperature)) as max_fahrenheit
		FROM stream 
		GROUP BY device, TumblingWindow('1s')
	`

	err = ssql.Execute(sql)
	assert.NoError(t, err)

	// Create a result receiving channel
	resultChan := make(chan any, 10)
	ssql.AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add test data (Celsius)
	testData := []map[string]any{
		{"device": "thermometer", "temperature": 0.0},   // 32°F
		{"device": "thermometer", "temperature": 100.0}, // 212°F
	}

	for _, data := range testData {
		ssql.Emit(data)
	}

	time.Sleep(1 * time.Second)
	ssql.TriggerWindow()
	time.Sleep(500 * time.Millisecond)

	// Verify the results
	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]any)
		assert.True(t, ok)
		assert.Len(t, resultSlice, 1)

		item := resultSlice[0]
		assert.Equal(t, "thermometer", item["device"])

		// Average Fahrenheit: (32 + 212) / 2 = 122
		avgFahrenheit, ok := item["avg_fahrenheit"].(float64)
		assert.True(t, ok)
		assert.InEpsilon(t, 122.0, avgFahrenheit, 0.01)

		// Maximum Fahrenheit: 212
		maxFahrenheit, ok := item["max_fahrenheit"].(float64)
		assert.True(t, ok)
		assert.InEpsilon(t, 212.0, maxFahrenheit, 0.01)

	case <-time.After(3 * time.Second):
		t.Fatal("Test timeout")
	}
}

// TestDebugCustomFunctions Debugging custom function issues
func TestDebugCustomFunctions(t *testing.T) {
	// Register simple square functions
	err := functions.RegisterCustomFunction(
		"square",
		functions.TypeMath,
		"数学函数",
		"计算平方",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			val := cast.ToFloat64(args[0])
			fmt.Printf("Square function called with: %v, result: %v\n", val, val*val)
			return val * val, nil
		},
	)
	assert.NoError(t, err)
	defer functions.Unregister("square")

	// Test whether the function can be found
	fn, exists := functions.Get("square")
	assert.True(t, exists)
	fmt.Printf("Function found: %s, type: %s\n", fn.GetName(), fn.GetType())

	// Test expression analysis
	expr, err := expr.NewExpression("square(value)")
	assert.NoError(t, err)

	// Get the expression field
	fields := expr.GetFields()
	fmt.Printf("Expression fields: %v\n", fields)

	// Test expression calculation
	data := map[string]any{"value": 5.0}
	result, err := expr.Evaluate(data)
	assert.NoError(t, err)
	fmt.Printf("Expression result: %v\n", result)
	assert.Equal(t, 25.0, result)

	// Test SQL parsing
	parser := rsql.NewParser("SELECT square(value) as squared FROM stream")
	stmt, err := parser.Parse()
	assert.NoError(t, err)

	_, _, err = stmt.ToStreamConfig()
	assert.NoError(t, err)
}

// TestDebugMultiParameterFunction is a custom multi-parameter test
func TestDebugMultiParameterFunction(t *testing.T) {
	// Register the distance calculation function
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
			fmt.Printf("Distance function called with: (%v,%v) to (%v,%v), result: %v\n", x1, y1, x2, y2, distance)
			return distance, nil
		},
	)
	assert.NoError(t, err)
	defer functions.Unregister("distance")

	// Test expression analysis
	expr, err := expr.NewExpression("distance(x1, y1, x2, y2)")
	assert.NoError(t, err)

	// Test expression calculation
	data := map[string]any{
		"x1": 0.0,
		"y1": 0.0,
		"x2": 3.0,
		"y2": 4.0,
	}
	result, err := expr.Evaluate(data)
	assert.NoError(t, err)
	assert.Equal(t, 5.0, result)

	// Test SQL parsing
	parser := rsql.NewParser("SELECT AVG(distance(x1, y1, x2, y2)) as avg_distance FROM stream GROUP BY device, TumblingWindow('1s')")
	stmt, err := parser.Parse()
	assert.NoError(t, err)

	_, _, err = stmt.ToStreamConfig()
	assert.NoError(t, err)
}
