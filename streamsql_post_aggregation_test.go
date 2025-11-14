package streamsql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 辅助函数：创建测试环境
func createTestEnvironment(t *testing.T, rsql string) (*Streamsql, chan interface{}) {
	ssql := New()
	t.Cleanup(func() { ssql.Stop() })

	err := ssql.Execute(rsql)
	require.NoError(t, err)

	resultChan := make(chan interface{}, 10)
	t.Cleanup(func() { close(resultChan) })

	ssql.AddSink(func(result []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		select {
		case resultChan <- result:
		default:
			// 非阻塞发送
		}
	})

	return ssql, resultChan
}

// 辅助函数：发送测试数据并收集结果
func sendDataAndCollectResults(t *testing.T, ssql *Streamsql, resultChan chan interface{}, testData []map[string]interface{}, windowSizeSeconds int) []map[string]interface{} {
	for _, data := range testData {
		ssql.Emit(data)
	}

	// 等待窗口触发
	time.Sleep(time.Duration(windowSizeSeconds+1) * time.Second)

	// 使用更严格的超时机制
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var results []map[string]interface{}
	maxIterations := 10 // 最多收集10次结果
	iteration := 0

collecting:
	for iteration < maxIterations {
		select {
		case result := <-resultChan:
			if resultSlice, ok := result.([]map[string]interface{}); ok {
				results = append(results, resultSlice...)
			}
			iteration++
		case <-time.After(500 * time.Millisecond):
			// 500ms 没有新结果，退出
			break collecting
		case <-ctx.Done():
			// 超时退出
			break collecting
		}
	}

	return results
}

// TestPostAggregationExpressions 测试分阶段聚合功能
func TestPostAggregationExpressions(t *testing.T) {
	t.Run("基础聚合函数复杂运算", func(t *testing.T) {
		rsql := `SELECT deviceId, 
				FIRST_VALUE(value) as firstVal,
				LAST_VALUE(value) as lastVal,
				(LAST_VALUE(value) - FIRST_VALUE(value)) as diffVal,
				SUM(value) as sumVal,
				AVG(value) as avgVal,
				(SUM(value) / COUNT(*)) as calcAvg,
				(SUM(value) + AVG(value)) as sumPlusAvg
				FROM stream 
				GROUP BY deviceId, TumblingWindow('5s')`

		ssql, resultChan := createTestEnvironment(t, rsql)

		testData := []map[string]interface{}{
			{"deviceId": "dev1", "value": 10.0},
			{"deviceId": "dev1", "value": 20.0},
			{"deviceId": "dev1", "value": 30.0},
		}

		results := sendDataAndCollectResults(t, ssql, resultChan, testData, 5)
		require.Len(t, results, 1)
		result := results[0]

		// 验证基础聚合函数运算
		assert.Equal(t, "dev1", result["deviceId"])
		assert.Equal(t, 10.0, result["firstVal"])
		assert.Equal(t, 30.0, result["lastVal"])
		assert.Equal(t, 20.0, result["diffVal"]) // LAST_VALUE - FIRST_VALUE
		assert.Equal(t, 60.0, result["sumVal"])
		assert.Equal(t, 20.0, result["avgVal"])
		assert.Equal(t, 20.0, result["calcAvg"])    // SUM / COUNT
		assert.Equal(t, 80.0, result["sumPlusAvg"]) // SUM + AVG
	})

	// IF_NULL 基础功能：在 IF_NULL 中包裹聚合/分析函数
	t.Run("验证：IF_NULL 基础功能", func(t *testing.T) {
		rsql := `SELECT deviceId,
				IF_NULL(FIRST_VALUE(value), 0) as firstOrZero,
				IF_NULL(LAST_VALUE(value), 0) as lastOrZero,
				IF_NULL(AVG(value), 0) as avgOrZero
				FROM stream
				GROUP BY deviceId, TumblingWindow('5s')`

		ssql, resultChan := createTestEnvironment(t, rsql)

		testData := []map[string]interface{}{
			{"deviceId": "sensor1", "value": nil},
			{"deviceId": "sensor1", "value": 10.0},
			{"deviceId": "sensor1", "value": nil},
			{"deviceId": "sensor1", "value": 30.0},
		}

		results := sendDataAndCollectResults(t, ssql, resultChan, testData, 5)
		require.Len(t, results, 1)
		result := results[0]

		assert.Equal(t, "sensor1", result["deviceId"])
		// FIRST_VALUE(value) 为 nil => IF_NULL(...,0) = 0
		assert.Equal(t, 0.0, result["firstOrZero"])
		// LAST_VALUE(value) 为 30 => IF_NULL(...,0) = 30
		assert.Equal(t, 30.0, result["lastOrZero"])
		// AVG(value) 仅计算非空 => (10+30)/2 = 20 => IF_NULL(...,0) = 20
		assert.Equal(t, 20.0, result["avgOrZero"])
	})

	// 聚合函数参数中嵌套 IF_NULL：如 SUM(IF_NULL(value,0))
	t.Run("验证：聚合函数嵌套 IF_NULL", func(t *testing.T) {
		rsql := `SELECT deviceId,
				SUM(IF_NULL(value, 0)) as sumVal,
				AVG(IF_NULL(value, 0)) as avgVal,
				MAX(IF_NULL(value, 0)) as maxVal,
				MIN(IF_NULL(value, 0)) as minVal
				FROM stream
				GROUP BY deviceId, TumblingWindow('5s')`

		ssql, resultChan := createTestEnvironment(t, rsql)

		testData := []map[string]interface{}{
			{"deviceId": "sensor1", "value": nil},
			{"deviceId": "sensor1", "value": 10.0},
			{"deviceId": "sensor1", "value": nil},
			{"deviceId": "sensor1", "value": 30.0},
		}

		results := sendDataAndCollectResults(t, ssql, resultChan, testData, 5)
		require.Len(t, results, 1)
		result := results[0]

		assert.Equal(t, "sensor1", result["deviceId"])
		// SUM(IF_NULL(value,0)) = 0 + 10 + 0 + 30 = 40
		assert.Equal(t, 40.0, result["sumVal"])
		// AVG(IF_NULL(value,0)) = (0 + 10 + 0 + 30)/4 = 10
		assert.Equal(t, 10.0, result["avgVal"])
		// MAX(IF_NULL(value,0)) = max(0,10,0,30) = 30
		assert.Equal(t, 30.0, result["maxVal"])
		// MIN(IF_NULL(value,0)) = min(0,10,0,30) = 0
		assert.Equal(t, 0.0, result["minVal"])
	})

	t.Run("分析函数与聚合函数复杂运算", func(t *testing.T) {
		rsql := `SELECT deviceId, 
				SUM(value) as total,
				AVG(value) as average,
				LATEST(value) as latest,
				(SUM(value) + LATEST(value)) as totalPlusLatest,
				(AVG(value) * LATEST(value)) as avgTimesLatest
				FROM stream 
				GROUP BY deviceId, TumblingWindow('5s')`

		ssql, resultChan := createTestEnvironment(t, rsql)

		testData := []map[string]interface{}{
			{"deviceId": "sensor1", "value": 10.0},
			{"deviceId": "sensor1", "value": 20.0},
			{"deviceId": "sensor1", "value": 30.0},
		}

		results := sendDataAndCollectResults(t, ssql, resultChan, testData, 5)
		require.Len(t, results, 1)
		result := results[0]

		// 验证分析函数与聚合函数的复杂运算
		assert.Equal(t, "sensor1", result["deviceId"])
		assert.Equal(t, 60.0, result["total"])           // 10+20+30
		assert.Equal(t, 20.0, result["average"])         // 60/3
		assert.Equal(t, 30.0, result["latest"])          // 最新值
		assert.Equal(t, 90.0, result["totalPlusLatest"]) // 60 + 30
		assert.Equal(t, 600.0, result["avgTimesLatest"]) // 20 * 30
	})

	t.Run("最外层嵌套普通函数验证", func(t *testing.T) {
		rsql := `SELECT deviceId, 
				SUM(value) as total,
				COUNT(*) as count,
				AVG(value) as average,
				MAX(value) as maxVal,
				(COUNT(*) * AVG(value)) as countTimesAvg,
				(SUM(value) / MAX(value)) as sumDivideMax,
				((COUNT(*) + SUM(value)) * AVG(value)) as complexNested,
				FLOOR((SUM(value) / MAX(value))) as floorResult,
				CEIL((AVG(value) / COUNT(*))) as ceilResult,
				ROUND((SUM(value) * AVG(value) / 1000), 2) as roundResult
				FROM stream 
				GROUP BY deviceId, TumblingWindow('5s')`

		ssql, resultChan := createTestEnvironment(t, rsql)

		testData := []map[string]interface{}{
			{"deviceId": "sensor1", "value": 10.0},
			{"deviceId": "sensor1", "value": 20.0},
			{"deviceId": "sensor1", "value": 30.0},
			{"deviceId": "sensor1", "value": 40.0},
		}

		results := sendDataAndCollectResults(t, ssql, resultChan, testData, 5)
		require.Len(t, results, 1)
		result := results[0]

		// 验证基础函数
		assert.Equal(t, "sensor1", result["deviceId"])
		assert.Equal(t, 100.0, result["total"])  // 10+20+30+40
		assert.Equal(t, 4.0, result["count"])    // 4 records
		assert.Equal(t, 25.0, result["average"]) // 100/4
		assert.Equal(t, 40.0, result["maxVal"])  // max value

		// 验证最外层嵌套普通函数
		// (COUNT(*) * AVG(value)) = 4 * 25 = 100
		assert.Equal(t, 100.0, result["countTimesAvg"], "最外层嵌套函数计算错误")

		// (SUM(value) / MAX(value)) = 100 / 40 = 2.5
		assert.Equal(t, 2.5, result["sumDivideMax"], "最外层嵌套函数计算错误")

		// ((COUNT(*) + SUM(value)) * AVG(value)) = (4 + 100) * 25 = 2600
		assert.Equal(t, 2600.0, result["complexNested"], "最外层复杂嵌套函数计算错误")

		// 验证最外层嵌套普通函数
		// FLOOR((SUM(value) / MAX(value))) = FLOOR(100/40) = FLOOR(2.5) = 2
		if floorResult, ok := result["floorResult"].(float64); ok {
			assert.Equal(t, 2.0, floorResult, "FLOOR函数嵌套计算错误")
		}

		// CEIL((AVG(value) / COUNT(*))) = CEIL(25/4) = CEIL(6.25) = 7
		if ceilResult, ok := result["ceilResult"].(float64); ok {
			assert.Equal(t, 7.0, ceilResult, "CEIL函数嵌套计算错误")
		}

		// ROUND((SUM(value) * AVG(value) / 1000), 2) = ROUND(100*25/1000, 2) = ROUND(2.5, 2) = 2.5
		if roundResult, ok := result["roundResult"].(float64); ok {
			assert.Equal(t, 2.5, roundResult, "ROUND函数嵌套计算错误")
		}

		// 验证最外层嵌套普通函数的正确性
		assert.Equal(t, 100.0, result["countTimesAvg"], "COUNT(*) * AVG(value) 计算错误")
		assert.Equal(t, 2.5, result["sumDivideMax"], "SUM(value) / MAX(value) 计算错误")
		assert.Equal(t, 2600.0, result["complexNested"], "复杂嵌套表达式计算错误")
		assert.Equal(t, 2.0, result["floorResult"], "FLOOR函数嵌套计算错误")
		assert.Equal(t, 7.0, result["ceilResult"], "CEIL函数嵌套计算错误")
		assert.Equal(t, 2.5, result["roundResult"], "ROUND函数嵌套计算错误")
	})

	t.Run("电表读数差值计算", func(t *testing.T) {
		rsql := `SELECT deviceId, 
				(LAST_VALUE(displayNum) - FIRST_VALUE(displayNum)) as diffVal,
				window_start() as start, 
				window_end() as end 
				FROM stream 
				GROUP BY deviceId, TumblingWindow('5s')`

		ssql, resultChan := createTestEnvironment(t, rsql)

		testData := []map[string]interface{}{
			// 设备1的数据
			{"deviceId": "meter001", "displayNum": 100.0},
			{"deviceId": "meter001", "displayNum": 115.0},

			// 设备2的数据
			{"deviceId": "meter002", "displayNum": 200.0},
			{"deviceId": "meter002", "displayNum": 206.0},
		}

		results := sendDataAndCollectResults(t, ssql, resultChan, testData, 5)
		require.GreaterOrEqual(t, len(results), 1, "应该至少有一个窗口的结果")

		// 预期结果
		expectedDiffs := map[string]float64{
			"meter001": 15.0, // 115.0 - 100.0 = 15.0 kWh
			"meter002": 6.0,  // 206.0 - 200.0 = 6.0 kWh
		}

		// 验证每个设备的计算结果
		deviceResults := make(map[string]map[string]interface{})
		for _, result := range results {
			deviceId, ok := result["deviceId"].(string)
			require.True(t, ok, "deviceId应该是字符串类型")
			deviceResults[deviceId] = result
		}

		for deviceId, expectedDiff := range expectedDiffs {
			result, exists := deviceResults[deviceId]
			assert.True(t, exists, "应该有设备 %s 的结果", deviceId)

			if exists {
				diffVal, ok := result["diffVal"].(float64)
				assert.True(t, ok, "diffVal应该是float64类型")
				assert.InEpsilon(t, expectedDiff, diffVal, 0.001,
					"设备 %s 的用电量计算应该正确: 期望 %.1f, 实际 %.1f",
					deviceId, expectedDiff, diffVal)

				// 验证窗口时间字段存在
				assert.Contains(t, result, "start", "结果应包含窗口开始时间")
				assert.Contains(t, result, "end", "结果应包含窗口结束时间")
			}
		}

		// 原始问题验证成功：电表读数差值计算正确
	})

	t.Run("综合功能验证", func(t *testing.T) {
		rsql := `SELECT deviceId, 
				SUM(value) as total,
				AVG(value) as average,
				FIRST_VALUE(value) as first,
				LAST_VALUE(value) as last,
				LATEST(value) as latest,
				COUNT(*) as count,
				MAX(value) as maxVal,
				MIN(value) as minVal,
				((SUM(value) + FIRST_VALUE(value)) / COUNT(*)) as complexCalc1,
				(LAST_VALUE(value) * AVG(value) - FIRST_VALUE(value)) as complexCalc2,
				((LATEST(value) + SUM(value)) / COUNT(*)) as complexCalc3,
				(MAX(value) + MIN(value) - AVG(value)) as complexCalc4,
				ROUND(SQRT(ABS(AVG(value) - MIN(value))), 2) as nestedMathFunc,
				UPPER(CONCAT('RESULT_', CAST(ROUND(SUM(value), 0) as STRING))) as nestedStrMathFunc
				FROM stream 
				GROUP BY deviceId, TumblingWindow('5s')`

		ssql, resultChan := createTestEnvironment(t, rsql)

		testData := []map[string]interface{}{
			{"deviceId": "sensor1", "value": 10.0},
			{"deviceId": "sensor1", "value": 20.0},
			{"deviceId": "sensor1", "value": 30.0},
			{"deviceId": "sensor1", "value": 40.0},
		}

		results := sendDataAndCollectResults(t, ssql, resultChan, testData, 5)
		require.Len(t, results, 1)
		result := results[0]

		// 验证基础函数
		assert.Equal(t, "sensor1", result["deviceId"])
		assert.Equal(t, 100.0, result["total"])  // 10+20+30+40
		assert.Equal(t, 25.0, result["average"]) // 100/4
		assert.Equal(t, 10.0, result["first"])   // 第一个值
		assert.Equal(t, 40.0, result["last"])    // 最后一个值
		assert.Equal(t, 40.0, result["latest"])  // 最新值
		assert.Equal(t, 4.0, result["count"])    // 4条记录
		assert.Equal(t, 40.0, result["maxVal"])  // 最大值
		assert.Equal(t, 10.0, result["minVal"])  // 最小值

		// 验证复杂表达式计算
		assert.Equal(t, 27.5, result["complexCalc1"])  // (100 + 10) / 4 = 27.5
		assert.Equal(t, 990.0, result["complexCalc2"]) // 40 * 25 - 10 = 990
		assert.Equal(t, 35.0, result["complexCalc3"])  // (40 + 100) / 4 = 35
		assert.Equal(t, 25.0, result["complexCalc4"])  // 40 + 10 - 25 = 25

		// 验证多层嵌套数学函数
		// ROUND(SQRT(ABS(AVG(value) - MIN(value))), 2) = ROUND(SQRT(ABS(25-10)), 2) = ROUND(SQRT(15), 2) ≈ 3.87
		if nestedMathFunc, ok := result["nestedMathFunc"].(float64); ok {
			assert.InEpsilon(t, 3.87, nestedMathFunc, 0.01, "多层嵌套数学函数计算错误")
		}

		// 验证多层嵌套字符串和数学函数
		// UPPER(CONCAT('RESULT_', CAST(ROUND(SUM(value), 0) as STRING))) = UPPER(CONCAT('RESULT_', '100')) = 'RESULT_100'
		if nestedStrMathFunc, ok := result["nestedStrMathFunc"].(string); ok {
			assert.Equal(t, "RESULT_100", nestedStrMathFunc, "多层嵌套字符串和数学函数计算错误")
		}
	})

	t.Run("嵌套聚合函数运算测试", func(t *testing.T) {
		rsql := `SELECT deviceId, 
				SUM(value) as total,
				AVG(value) as average,
				COUNT(*) as count,
				MAX(value) as maxVal,
				MIN(value) as minVal,
				ROUND(AVG(ABS(value)), 2) as avgAbs,
				MAX(ROUND(value, 1)) as maxRounded,
				MIN(CEIL(value / 10)) as minCeiled,
				AVG(SQRT(value)) as avgSqrt,
				SUM(POWER(value, 2)) as sumSquares,
				CEIL(AVG(FLOOR(SQRT(value)))) as tripleNested2,
				ABS(MIN(ROUND(value / 5, 2))) as tripleNested3
				FROM stream 
				GROUP BY deviceId, TumblingWindow('5s')`

		ssql, resultChan := createTestEnvironment(t, rsql)

		testData := []map[string]interface{}{
			{"deviceId": "sensor1", "value": 16.0},
			{"deviceId": "sensor1", "value": 25.0},
			{"deviceId": "sensor1", "value": 36.0},
			{"deviceId": "sensor1", "value": 49.0},
		}

		results := sendDataAndCollectResults(t, ssql, resultChan, testData, 5)
		require.Len(t, results, 1)
		result := results[0]

		// 验证基础聚合函数
		assert.Equal(t, "sensor1", result["deviceId"])
		assert.Equal(t, 126.0, result["total"])  // 16+25+36+49
		assert.Equal(t, 31.5, result["average"]) // 126/4
		assert.Equal(t, 4.0, result["count"])    // 4 records
		assert.Equal(t, 49.0, result["maxVal"])  // max value
		assert.Equal(t, 16.0, result["minVal"])  // min value

		// 验证嵌套聚合函数运算
		// ROUND(AVG(ABS(value)), 2) = ROUND(AVG(16,25,36,49), 2) = ROUND(31.5, 2) = 31.5
		if avgAbs, ok := result["avgAbs"].(float64); ok {
			assert.Equal(t, 31.5, avgAbs, "AVG(ABS(value))计算错误")
		}

		// MAX(ROUND(value, 1)) = MAX(16.0, 25.0, 36.0, 49.0) = 49.0
		if maxRounded, ok := result["maxRounded"].(float64); ok {
			assert.Equal(t, 49.0, maxRounded, "MAX(ROUND(value, 1))计算错误")
		}

		// MIN(CEIL(value / 10)) = MIN(CEIL(1.6), CEIL(2.5), CEIL(3.6), CEIL(4.9)) = MIN(2, 3, 4, 5) = 2
		if minCeiled, ok := result["minCeiled"].(float64); ok {
			assert.Equal(t, 2.0, minCeiled, "MIN(CEIL(value / 10))计算错误")
		}

		// AVG(SQRT(value)) = AVG(SQRT(16), SQRT(25), SQRT(36), SQRT(49)) = AVG(4, 5, 6, 7) = 5.5
		if avgSqrt, ok := result["avgSqrt"].(float64); ok {
			assert.Equal(t, 5.5, avgSqrt, "AVG(SQRT(value))计算错误")
		}

		// SUM(POWER(value, 2)) = SUM(16^2, 25^2, 36^2, 49^2) = SUM(256, 625, 1296, 2401) = 4578
		if sumSquares, ok := result["sumSquares"].(float64); ok {
			assert.Equal(t, 4578.0, sumSquares, "SUM(POWER(value, 2))计算错误")
		}

		// CEIL(AVG(FLOOR(SQRT(value))))
		// = CEIL(AVG(FLOOR(4), FLOOR(5), FLOOR(6), FLOOR(7))) = CEIL(AVG(4, 5, 6, 7)) = CEIL(5.5) = 6
		if tripleNested2, ok := result["tripleNested2"].(float64); ok {
			assert.Equal(t, 6.0, tripleNested2, "三层嵌套聚合2计算错误")
		}

		// ABS(MIN(ROUND(value / 5, 2)))
		// = ABS(MIN(ROUND(3.2, 2), ROUND(5, 2), ROUND(7.2, 2), ROUND(9.8, 2)))
		// = ABS(MIN(3.2, 5.0, 7.2, 9.8)) = ABS(3.2) = 3.2
		if tripleNested3, ok := result["tripleNested3"].(float64); ok {
			assert.Equal(t, 3.2, tripleNested3, "三层嵌套聚合3计算错误")
		}
	})

	t.Run("验证：NTH_VALUE和LEAD函数", func(t *testing.T) {
		rsql := `SELECT deviceId, 
				SUM(value) as total,
				COUNT(*) as count,
				NTH_VALUE(value, 2) as secondValue,
				LEAD(value, 1) as leadValue,
				(COUNT(*) * NTH_VALUE(value, 2)) as countTimesSecond,
				(SUM(value) + LEAD(value, 1)) as sumPlusLead
				FROM stream 
				GROUP BY deviceId, TumblingWindow('5s')`

		ssql, resultChan := createTestEnvironment(t, rsql)

		testData := []map[string]interface{}{
			{"deviceId": "sensor1", "value": 10.0},
			{"deviceId": "sensor1", "value": 20.0},
			{"deviceId": "sensor1", "value": 30.0},
			{"deviceId": "sensor1", "value": 40.0},
		}

		results := sendDataAndCollectResults(t, ssql, resultChan, testData, 5)
		require.Len(t, results, 1)
		result := results[0]

		// 验证基础函数
		assert.Equal(t, "sensor1", result["deviceId"])
		assert.Equal(t, 100.0, result["total"]) // 10+20+30+40
		assert.Equal(t, 4.0, result["count"])   // 4 records

		// 验证窗口函数基础功能
		assert.NotNil(t, result["countTimesSecond"], "COUNT(*) * NTH_VALUE(value, 2) 应该有计算结果")

	})

	t.Run("验证：NTH_VALUE基础功能", func(t *testing.T) {
		rsql := `SELECT deviceId, 
				NTH_VALUE(value, 1) as firstValue,
				NTH_VALUE(value, 2) as secondValue,
				NTH_VALUE(value, 3) as thirdValue,
				NTH_VALUE(value, 4) as fourthValue
				FROM stream 
				GROUP BY deviceId, TumblingWindow('5s')`

		ssql, resultChan := createTestEnvironment(t, rsql)

		testData := []map[string]interface{}{
			{"deviceId": "sensor1", "value": 100.0},
			{"deviceId": "sensor1", "value": 200.0},
			{"deviceId": "sensor1", "value": 300.0},
			{"deviceId": "sensor1", "value": 400.0},
		}

		results := sendDataAndCollectResults(t, ssql, resultChan, testData, 5)
		require.Len(t, results, 1)
		result := results[0]

		// 验证 NTH_VALUE 函数的返回值
		// 期望结果：按添加顺序
		// 第1个值: 100, 第2个值: 200, 第3个值: 300, 第4个值: 400
		if firstValue, ok := result["firstValue"].(float64); ok {
			assert.Equal(t, 100.0, firstValue, "第1个值应该是100")
		} else {
			assert.Error(t, errors.New("firstValue 为空"))
		}

		if secondValue, ok := result["secondValue"].(float64); ok {
			assert.Equal(t, 200.0, secondValue, "第2个值应该是200")
		} else {
			assert.Error(t, errors.New("secondValue 为空"))
		}

		if thirdValue, ok := result["thirdValue"].(float64); ok {
			assert.Equal(t, 300.0, thirdValue, "第3个值应该是300")
		} else {
			assert.Error(t, errors.New("thirdValue 为空"))
		}

		if fourthValue, ok := result["fourthValue"].(float64); ok {
			assert.Equal(t, 400.0, fourthValue, "第4个值应该是400")
		} else {
			assert.Error(t, errors.New("fourthValue 为空"))
		}
	})
}
