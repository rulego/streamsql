/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rulego/streamsql/stream"
)

// main 主函数，演示StreamSQL持久化功能的完整示例
// 包含有序持久化机制、数据恢复和零数据丢失配置
func main() {
	fmt.Println("=== StreamSQL 持久化功能完整示例 ===")
	fmt.Println("演示有序持久化机制、数据恢复和零数据丢失配置")
	fmt.Println()

	// 清理之前的测试数据
	cleanupTestData()

	// 示例1: 有序持久化机制演示
	fmt.Println("📌 示例1: 有序持久化机制演示")
	orderedPersistenceExample()

	// 示例2: 数据溢出持久化测试
	fmt.Println("\n📌 示例2: 数据溢出持久化测试")
	testDataOverflowPersistence()

	// 示例3: 程序重启数据恢复测试
	fmt.Println("\n📌 示例3: 程序重启数据恢复测试")
	testDataRecovery()

	// 示例4: 零数据丢失配置
	fmt.Println("\n📌 示例4: 零数据丢失配置")
	createZeroDataLossExample()

	// 示例5: 持久化文件分析
	fmt.Println("\n📌 示例5: 持久化文件分析")
	analyzePersistenceFiles()

	fmt.Println("\n✅ 持久化功能完整示例演示完成！")
}

// orderedPersistenceExample 演示有序持久化机制的使用
// 展示如何配置和使用有序持久化来保证数据时序性
func orderedPersistenceExample() {
	fmt.Println("演示如何使用有序持久化机制保证数据时序性")
	fmt.Println()

	// 创建临时目录
	tempDir := "./persistence_data"
	os.RemoveAll(tempDir) // 清理之前的数据

	// 1. 创建持久化管理器
	pm := stream.NewPersistenceManager(tempDir)
	if pm == nil {
		fmt.Println("创建持久化管理器失败")
		return
	}

	// 2. 启动管理器
	err := pm.Start()
	if err != nil {
		fmt.Printf("启动持久化管理器失败: %v\n", err)
		return
	}
	defer pm.Stop()

	fmt.Println("持久化管理器已启动，开始持久化测试数据...")

	// 3. 持久化测试数据
	testData := []map[string]interface{}{
		{"message": "test_data_1", "id": 1, "timestamp": time.Now().UnixNano()},
		{"message": "test_data_2", "id": 2, "timestamp": time.Now().UnixNano()},
		{"message": "test_data_3", "id": 3, "timestamp": time.Now().UnixNano()},
		{"message": "test_data_4", "id": 4, "timestamp": time.Now().UnixNano()},
		{"message": "test_data_5", "id": 5, "timestamp": time.Now().UnixNano()},
	}

	for i, data := range testData {
		err := pm.PersistData(data)
		if err != nil {
			fmt.Printf("持久化数据失败: %v\n", err)
			return
		}
		fmt.Printf("已持久化数据 %d: %v\n", i+1, data["message"])
	}

	// 4. 等待数据刷新到磁盘
	fmt.Println("等待数据刷新到磁盘...")
	time.Sleep(3 * time.Second)

	// 5. 显示统计信息
	stats := pm.GetStats()
	fmt.Println("\n=== 持久化统计信息 ===")
	if totalPersisted, ok := stats["total_persisted"].(int64); ok {
		fmt.Printf("总持久化数据: %d 条\n", totalPersisted)
	}
	if filesCreated, ok := stats["files_created"].(int64); ok {
		fmt.Printf("创建文件数: %d 个\n", filesCreated)
	}
	if sequenceCounter, ok := stats["sequence_counter"].(int64); ok {
		fmt.Printf("序列号计数器: %d\n", sequenceCounter)
	}
	if running, ok := stats["running"].(bool); ok {
		fmt.Printf("运行状态: %v\n", running)
	}

	fmt.Println("\n=== 有序持久化机制说明 ===")
	fmt.Println("1. 有序持久化机制通过全局序列号保证数据时序性")
	fmt.Println("2. 当内存通道满时，数据按序持久化到磁盘")
	fmt.Println("3. 系统恢复时，数据按原始顺序从磁盘加载并处理")
	fmt.Println("4. 避免了传统持久化中可能出现的数据乱序问题")
	fmt.Println("5. 实现了真正的先进先出(FIFO)数据处理")
}

// testDataOverflowPersistence 测试数据溢出时的持久化功能
// 通过创建小缓冲区并快速发送大量数据来触发溢出和持久化
func testDataOverflowPersistence() {
	// 创建临时目录
	tempDir := "./streamsql_overflow_data"
	os.RemoveAll(tempDir) // 清理之前的数据

	// 使用较小的文件大小以触发轮转
	pm := stream.NewPersistenceManagerWithConfig(tempDir, 100, 50*time.Millisecond)
	if pm == nil {
		fmt.Println("创建持久化管理器失败")
		return
	}

	err := pm.Start()
	if err != nil {
		fmt.Printf("启动持久化管理器失败: %v\n", err)
		return
	}
	defer pm.Stop()

	// 快速发送大量数据，触发文件轮转
	inputCount := 20
	fmt.Printf("快速发送 %d 条数据以触发文件轮转...\n", inputCount)

	start := time.Now()
	for i := 0; i < inputCount; i++ {
		longData := map[string]interface{}{
			"message": fmt.Sprintf("this_is_a_long_data_string_to_trigger_file_rotation_%d", i),
			"id":      i,
			"extra":   "some extra data to make it longer",
		}
		err := pm.PersistData(longData)
		if err != nil {
			fmt.Printf("持久化数据失败: %v\n", err)
			return
		}
		if i%5 == 0 {
			fmt.Printf("已发送 %d 条数据\n", i+1)
		}
	}
	duration := time.Since(start)

	// 等待数据刷新
	fmt.Println("等待数据刷新...")
	time.Sleep(200 * time.Millisecond)

	// 获取统计信息
	stats := pm.GetStats()
	fmt.Printf("\n=== 溢出持久化统计 ===\n")
	fmt.Printf("⏱️  发送耗时: %v\n", duration)
	if totalPersisted, ok := stats["total_persisted"].(int64); ok {
		fmt.Printf("📊 总持久化数据: %d 条\n", totalPersisted)
	}
	if filesCreated, ok := stats["files_created"].(int64); ok {
		fmt.Printf("📊 创建文件数: %d 个\n", filesCreated)
		if filesCreated > 1 {
			fmt.Println("✅ 文件轮转成功")
		}
	}
	if sequenceCounter, ok := stats["sequence_counter"].(int64); ok {
		fmt.Printf("📊 序列号计数器: %d\n", sequenceCounter)
	}
}

// testDataRecovery 测试程序重启后的数据恢复功能
// 模拟程序重启，加载之前持久化的数据并重新处理
func testDataRecovery() {
	// 使用与第一个示例相同的目录
	tempDir := "./persistence_data"

	// 第一阶段：持久化数据
	fmt.Println("第一阶段：持久化数据")
	pm1 := stream.NewPersistenceManager(tempDir)
	if pm1 == nil {
		fmt.Println("创建持久化管理器失败")
		return
	}

	err := pm1.Start()
	if err != nil {
		fmt.Printf("启动持久化管理器失败: %v\n", err)
		return
	}

	// 持久化测试数据
	testData := []map[string]interface{}{
		{"message": "recovery_data_1", "id": 1},
		{"message": "recovery_data_2", "id": 2},
		{"message": "recovery_data_3", "id": 3},
		{"message": "recovery_data_4", "id": 4},
		{"message": "recovery_data_5", "id": 5},
	}

	for i, data := range testData {
		err := pm1.PersistData(data)
		if err != nil {
			fmt.Printf("持久化数据失败: %v\n", err)
			return
		}
		fmt.Printf("已持久化恢复数据 %d: %v\n", i+1, data["message"])
	}

	// 等待数据刷新到磁盘
	fmt.Println("等待数据刷新到磁盘...")
	time.Sleep(3 * time.Second)

	pm1.Stop()
	fmt.Println("第一阶段完成，模拟程序重启...")

	// 第二阶段：恢复数据
	fmt.Println("\n第二阶段：恢复数据")
	pm2 := stream.NewPersistenceManager(tempDir)
	if pm2 == nil {
		fmt.Println("创建恢复管理器失败")
		return
	}

	err = pm2.Start()
	if err != nil {
		fmt.Printf("启动恢复管理器失败: %v\n", err)
		return
	}
	defer pm2.Stop()

	// 加载并恢复数据
	err = pm2.LoadAndRecoverData()
	if err != nil {
		fmt.Printf("加载恢复数据失败: %v\n", err)
		return
	}

	// 等待恢复数据填充到队列中
	time.Sleep(200 * time.Millisecond)

	// 按序获取恢复数据
	fmt.Println("\n=== 恢复的数据 ===")
	recoveredData := make([]map[string]interface{}, 0)
	for i := 0; i < len(testData); i++ {
		data, hasMore := pm2.GetRecoveryData()
		if hasMore && data != nil {
			recoveredData = append(recoveredData, data)
			fmt.Printf("恢复数据 %d: %v\n", i+1, data["message"])
		} else {
			break
		}
	}

	// 验证数据完整性
	fmt.Printf("\n原始数据数量: %d\n", len(testData))
	fmt.Printf("恢复数据数量: %d\n", len(recoveredData))
	if len(testData) == len(recoveredData) {
		fmt.Println("✅ 数据恢复完整")
	} else {
		fmt.Println("❌ 数据恢复不完整")
	}
}

// createZeroDataLossExample 创建零数据丢失配置的示例
// 使用持久化管理器演示零数据丢失配置
func createZeroDataLossExample() {
	fmt.Println("演示零数据丢失配置")

	// 创建专用目录
	tempDir := "./zero_loss_data"
	os.RemoveAll(tempDir) // 清理之前的数据

	// 使用更频繁的刷新间隔以确保数据安全
	pm := stream.NewPersistenceManagerWithConfig(tempDir, 5*1024*1024, 1*time.Second)
	if pm == nil {
		fmt.Println("创建持久化管理器失败")
		return
	}

	err := pm.Start()
	if err != nil {
		fmt.Printf("启动持久化管理器失败: %v\n", err)
		return
	}
	defer pm.Stop()

	// 发送关键数据
	criticalData := []map[string]interface{}{
		{"id": 1, "transaction": "critical_transaction_1", "amount": 1000.50},
		{"id": 2, "transaction": "critical_transaction_2", "amount": 2500.75},
		{"id": 3, "transaction": "critical_transaction_3", "amount": 750.25},
		{"id": 4, "transaction": "critical_transaction_4", "amount": 3200.00},
		{"id": 5, "transaction": "critical_transaction_5", "amount": 1800.90},
	}

	fmt.Println("发送关键数据（零数据丢失模式）...")
	for i, data := range criticalData {
		err := pm.PersistData(data)
		if err != nil {
			fmt.Printf("持久化关键数据失败: %v\n", err)
			return
		}
		fmt.Printf("已持久化关键数据 %d: %v (金额: %.2f)\n", i+1, data["transaction"], data["amount"])
		time.Sleep(100 * time.Millisecond) // 模拟实际处理间隔
	}

	// 等待所有数据刷新到磁盘
	fmt.Println("等待所有关键数据刷新到磁盘...")
	time.Sleep(3 * time.Second)

	// 获取统计信息
	stats := pm.GetStats()

	fmt.Printf("\n=== 零数据丢失统计 ===\n")
	if totalPersisted, ok := stats["total_persisted"].(int64); ok {
		fmt.Printf("📊 总持久化数据: %d 条\n", totalPersisted)
		if totalPersisted == int64(len(criticalData)) {
			fmt.Println("✅ 零数据丢失验证成功")
		} else {
			fmt.Println("❌ 检测到数据丢失")
		}
	}
	if filesCreated, ok := stats["files_created"].(int64); ok {
		fmt.Printf("📊 创建文件数: %d 个\n", filesCreated)
	}
	if sequenceCounter, ok := stats["sequence_counter"].(int64); ok {
		fmt.Printf("📊 序列号计数器: %d\n", sequenceCounter)
	}
	fmt.Printf("📊 刷新间隔: 1秒（高频刷新确保数据安全）\n")
	fmt.Printf("📊 最大文件大小: 5MB\n")
}

// analyzePersistenceFiles 分析持久化文件的内容和统计信息
// 检查持久化目录中的文件，显示文件大小和内容预览
func analyzePersistenceFiles() {
	dataDirs := []string{"./streamsql_overflow_data", "./persistence_data", "./zero_loss_data"}

	for _, dataDir := range dataDirs {
		fmt.Printf("\n检查目录: %s\n", dataDir)
		
		// 检查持久化目录
		if _, err := os.Stat(dataDir); os.IsNotExist(err) {
			fmt.Println("目录不存在")
			continue
		}

		// 列出所有持久化文件
		files, err := filepath.Glob(filepath.Join(dataDir, "streamsql_*.log"))
		if err != nil {
			fmt.Printf("读取持久化文件失败: %v\n", err)
			continue
		}

		if len(files) == 0 {
			fmt.Println("没有找到持久化文件（可能已被恢复过程删除）")
			continue
		}

		fmt.Printf("发现 %d 个持久化文件:\n", len(files))
		for i, file := range files {
			info, err := os.Stat(file)
			if err != nil {
				fmt.Printf("  %d. %s (无法读取文件信息)\n", i+1, filepath.Base(file))
				continue
			}
			fmt.Printf("  %d. %s (大小: %d bytes, 修改时间: %s)\n",
				i+1, filepath.Base(file), info.Size(), info.ModTime().Format("15:04:05"))
		}

		// 读取第一个文件的前几行内容
		if len(files) > 0 {
			fmt.Printf("\n第一个文件的前3行内容:\n")
			showFileContent(files[0], 3)
		}
	}
}

// showFileContent 显示指定文件的前几行内容
// filename: 要读取的文件路径
// maxLines: 最大显示行数
func showFileContent(filename string, maxLines int) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("无法打开文件: %v\n", err)
		return
	}
	defer file.Close()

	buffer := make([]byte, 1024)
	n, err := file.Read(buffer)
	if err != nil {
		fmt.Printf("无法读取文件: %v\n", err)
		return
	}

	content := string(buffer[:n])
	lines := []rune(content)
	lineCount := 0
	currentLine := ""

	for _, char := range lines {
		if char == '\n' {
			lineCount++
			fmt.Printf("  %d: %s\n", lineCount, currentLine)
			currentLine = ""
			if lineCount >= maxLines {
				break
			}
		} else {
			currentLine += string(char)
		}
	}

	if currentLine != "" && lineCount < maxLines {
		fmt.Printf("  %d: %s\n", lineCount+1, currentLine)
	}
}

// cleanupTestData 清理测试产生的持久化数据
// 删除测试目录及其所有内容，为新的测试做准备
func cleanupTestData() {
	dataDirs := []string{"./streamsql_overflow_data", "./persistence_data", "./zero_loss_data"}
	for _, dataDir := range dataDirs {
		if err := os.RemoveAll(dataDir); err != nil {
			fmt.Printf("清理测试数据失败 (%s): %v\n", dataDir, err)
		}
	}
	fmt.Println("测试数据清理完成")
}
