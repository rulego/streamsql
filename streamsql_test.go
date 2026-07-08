package streamsql

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStreamData(t *testing.T) {
	// 步骤1: 创建 StreamSQL 实例
	// StreamSQL 是流式 SQL 处理引擎的核心组件，负责管理整个流处理生命周期
	ssql := New()
	// 确保测试结束时停止流处理，释放资源
	defer ssql.Stop()

	// 步骤2: 定义流式 SQL 查询语句
	// 这个 SQL 语句展示了 StreamSQL 的核心功能：
	// - SELECT: 选择要输出的字段和聚合函数
	// - FROM stream: 指定数据源为流数据
	// - WHERE: 过滤条件，排除 device3 的数据
	// - GROUP BY: 按设备ID分组，配合滚动窗口进行聚合
	// - TumblingWindow('5s'): 5秒滚动窗口，每5秒触发一次计算
	// - avg(), min(): 聚合函数，计算平均值和最小值
	// - window_start(), window_end(): 窗口函数，获取窗口的开始和结束时间
	rsql := "SELECT deviceId,avg(temperature) as avg_temp,min(humidity) as min_humidity ," +
		"window_start() as start,window_end() as end FROM  stream  where deviceId!='device3' group by deviceId,TumblingWindow('5s')"

	// 步骤3: 执行 SQL 语句，启动流式分析任务
	// Execute 方法会解析 SQL、构建执行计划、初始化窗口管理器和聚合器
	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	// 步骤4: 设置测试环境和并发控制
	var wg sync.WaitGroup
	wg.Add(1)
	// 设置30秒测试超时时间，防止测试无限运行
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 步骤5: 启动数据生产者协程
	// 模拟实时数据流，持续向 StreamSQL 输入数据
	go func() {
		defer wg.Done()
		// 创建定时器，每秒触发一次数据生成
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// 每秒生成10条随机测试数据，模拟高频数据流
				// 这种数据密度可以测试 StreamSQL 的实时处理能力
				for i := 0; i < 10; i++ {
					// 构造设备数据，包含设备ID、温度和湿度
					randomData := map[string]interface{}{
						"deviceId":    fmt.Sprintf("device%d", rand.Intn(3)+1), // 随机选择 device1, device2, device3
						"temperature": 20.0 + rand.Float64()*10,                // 温度范围: 20-30度
						"humidity":    50.0 + rand.Float64()*20,                // 湿度范围: 50-70%
					}
					// 将数据添加到流中，触发 StreamSQL 的实时处理
					// Emit 会将数据分发到相应的窗口和聚合器中
					ssql.Emit(randomData)
				}

			case <-ctx.Done():
				// 超时或取消信号，停止数据生成
				return
			}
		}
	}()

	// 步骤6: 设置结果处理管道
	resultChan := make(chan interface{}, 10)
	// 添加计算结果回调函数（Sink）
	// 当窗口触发计算时，结果会通过这个回调函数输出
	ssql.stream.AddSink(func(result []map[string]interface{}) {
		// 非阻塞发送，避免阻塞 sink worker
		select {
		case resultChan <- result:
		default:
			// Channel 已满，忽略（非阻塞发送）
		}
	})

	// 步骤7: 启动结果消费者协程
	// 记录收到的结果数量，用于验证测试效果
	var resultCount int64
	var countMutex sync.Mutex
	var consumerWg sync.WaitGroup
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		for {
			select {
			case <-resultChan:
				// 每当收到一个窗口的计算结果时，计数器加1
				// 注释掉的代码可以用于调试，打印每个结果的详细信息
				//fmt.Printf("打印结果: [%s] %v\n", time.Now().Format("15:04:05.000"), result)
				countMutex.Lock()
				resultCount++
				countMutex.Unlock()
			case <-ctx.Done():
				// 测试超时，退出消费者 goroutine
				// 不关闭 channel，让主程序自动退出时清理
				return
			}
		}
	}()

	// 步骤8: 等待测试完成
	// 等待数据生产者协程结束（30秒超时或手动取消）
	wg.Wait()

	// 停止流处理，确保所有 goroutine 正确退出
	ssql.Stop()

	// 等待一小段时间，确保所有 sink worker 完成当前任务
	// 这样可以确保所有结果都被发送到 channel
	time.Sleep(100 * time.Millisecond)

	// 取消 context，通知消费者 goroutine 退出
	cancel()

	// 等待消费者 goroutine 完成（处理完 channel 中剩余的数据或收到取消信号）
	consumerWg.Wait()

	// 步骤9: 验证测试结果
	// 预期在30秒内应该收到5个窗口的计算结果（每5秒一个窗口）
	// 这验证了 StreamSQL 的窗口触发机制是否正常工作
	countMutex.Lock()
	finalCount := resultCount
	countMutex.Unlock()
	assert.Equal(t, finalCount, int64(5))
}
