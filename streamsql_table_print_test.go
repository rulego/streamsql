package streamsql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestPrintTable 测试PrintTable方法的基本功能
func TestPrintTable(t *testing.T) {
	// 创建StreamSQL实例并测试PrintTable
	ssql := New()
	err := ssql.Execute("SELECT device, AVG(temperature) as avg_temp FROM stream GROUP BY device, TumblingWindow('2s')")
	assert.NoError(t, err)

	// 使用PrintTable方法（不验证输出内容，只确保不会panic）
	assert.NotPanics(t, func() {
		ssql.PrintTable()
	}, "PrintTable方法不应该panic")

	// 发送测试数据
	testData := []map[string]interface{}{
		{"device": "sensor1", "temperature": 25.0},
		{"device": "sensor2", "temperature": 30.0},
	}

	for _, data := range testData {
		ssql.Emit(data)
	}

	// 等待窗口触发
	time.Sleep(3 * time.Second)
}

// TestPrintTableFormat 测试printTableFormat方法处理不同数据类型
func TestPrintTableFormat(t *testing.T) {
	ssql := New()

	// 测试不同类型的数据，确保不会panic
	assert.NotPanics(t, func() {
		// 测试空切片
		ssql.printTableFormat([]map[string]interface{}{})
	}, "空切片不应该panic")

	assert.NotPanics(t, func() {
		// 测试单个map
		ssql.printTableFormat(map[string]interface{}{"key": "value"})
	}, "单个map不应该panic")

	assert.NotPanics(t, func() {
		// 测试其他类型
		ssql.printTableFormat("string data")
	}, "字符串数据不应该panic")
}