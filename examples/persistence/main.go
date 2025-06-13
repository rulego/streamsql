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
	"github.com/rulego/streamsql/types"
)

func main() {
	fmt.Println("=== StreamSQL 持久化功能测试 ===\n")

	// 清理之前的测试数据
	cleanupTestData()

	// 测试1: 创建持久化流并模拟数据溢出
	fmt.Println("📌 测试1: 数据溢出持久化")
	testDataOverflowPersistence()

	// 测试2: 模拟程序重启和数据恢复
	fmt.Println("\n📌 测试2: 程序重启数据恢复")
	testDataRecovery()

	// 测试3: 查看持久化文件内容
	fmt.Println("\n📌 测试3: 持久化文件分析")
	analyzePersistenceFiles()

	fmt.Println("\n✅ 真正持久化功能测试完成！")
}

func testDataOverflowPersistence() {
	config := types.Config{
		SimpleFields: []string{"id", "value"},
	}

	// 创建小缓冲区的持久化流处理器
	stream, err := stream.NewStreamWithLossPolicy(
		config,
		100,       // 很小的缓冲区，容易溢出
		100,       // 小结果缓冲区
		50,        // 小sink池
		"persist", // 持久化策略
		5*time.Second,
	)
	if err != nil {
		fmt.Printf("创建流失败: %v\n", err)
		return
	}

	stream.Start()

	// 快速发送大量数据，触发溢出
	inputCount := 1000
	fmt.Printf("快速发送 %d 条数据到小缓冲区 (容量100)...\n", inputCount)

	start := time.Now()
	for i := 0; i < inputCount; i++ {
		data := map[string]interface{}{
			"id":    i,
			"value": fmt.Sprintf("data_%d", i),
		}
		stream.AddData(data)
	}
	duration := time.Since(start)

	// 等待持久化完成
	fmt.Println("等待持久化操作完成...")
	time.Sleep(8 * time.Second)

	// 获取统计信息
	stats := stream.GetDetailedStats()
	persistStats := stream.GetPersistenceStats()

	fmt.Printf("⏱️  发送耗时: %v\n", duration)
	fmt.Printf("📊 输入数据: %d\n", stats["basic_stats"].(map[string]int64)["input_count"])
	fmt.Printf("📊 处理数据: %d\n", stats["basic_stats"].(map[string]int64)["output_count"])
	fmt.Printf("📊 通道容量: %d\n", stats["basic_stats"].(map[string]int64)["data_chan_cap"])
	fmt.Printf("📊 持久化启用: %v\n", persistStats["enabled"])
	fmt.Printf("📊 待写入数据: %v\n", persistStats["pending_count"])
	fmt.Printf("📊 当前文件大小: %v bytes\n", persistStats["current_file_size"])
	fmt.Printf("📊 文件索引: %v\n", persistStats["file_index"])

	stream.Stop()
}

func testDataRecovery() {
	config := types.Config{
		SimpleFields: []string{"id", "value"},
	}

	// 创建新的持久化流处理器（模拟程序重启）
	stream, err := stream.NewStreamWithLossPolicy(
		config,
		200, // 更大的缓冲区用于恢复
		200,
		100,
		"persist", // 持久化策略
		5*time.Second,
	)
	if err != nil {
		fmt.Printf("创建流失败: %v\n", err)
		return
	}

	stream.Start()

	// 添加sink来接收恢复的数据
	recoveredCount := 0
	stream.AddSink(func(data interface{}) {
		recoveredCount++
		if recoveredCount <= 5 {
			fmt.Printf("恢复数据 %d: %+v\n", recoveredCount, data)
		}
	})

	// 尝试加载并重新处理持久化数据
	fmt.Println("尝试加载持久化数据...")
	if err := stream.LoadAndReprocessPersistedData(); err != nil {
		fmt.Printf("数据恢复失败: %v\n", err)
	}

	// 等待处理完成
	time.Sleep(3 * time.Second)

	stats := stream.GetDetailedStats()
	fmt.Printf("📊 恢复后处理数据: %d\n", stats["basic_stats"].(map[string]int64)["output_count"])
	fmt.Printf("📊 接收到的恢复数据: %d\n", recoveredCount)

	stream.Stop()
}

func analyzePersistenceFiles() {
	dataDir := "./streamsql_overflow_data"

	// 检查持久化目录
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		fmt.Println("持久化目录不存在")
		return
	}

	// 列出所有持久化文件
	files, err := filepath.Glob(filepath.Join(dataDir, "streamsql_overflow_*.log"))
	if err != nil {
		fmt.Printf("读取持久化文件失败: %v\n", err)
		return
	}

	if len(files) == 0 {
		fmt.Println("没有找到持久化文件（可能已被恢复过程删除）")
		return
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

func cleanupTestData() {
	dataDir := "./streamsql_overflow_data"
	if err := os.RemoveAll(dataDir); err != nil {
		fmt.Printf("清理测试数据失败: %v\n", err)
	}
}
