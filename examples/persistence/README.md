# StreamSQL 持久化功能示例

本示例演示了 StreamSQL 的持久化功能，包括有序持久化机制、数据恢复和零数据丢失配置。

## 功能特性

### 1. 有序持久化机制
- 通过全局序列号保证数据时序性
- 当内存通道满时，数据按序持久化到磁盘
- 避免传统持久化中可能出现的数据乱序问题
- 实现真正的先进先出(FIFO)数据处理

### 2. 数据溢出持久化
- 演示小文件大小配置下的文件轮转
- 快速发送大量数据触发文件轮转机制
- 展示持久化统计信息

### 3. 程序重启数据恢复
- 模拟程序重启场景
- 从磁盘加载之前持久化的数据
- 按原始顺序恢复数据处理
- 验证数据完整性

### 4. 零数据丢失配置
- 高频刷新确保数据安全
- 关键数据持久化演示
- 数据完整性验证

### 5. 持久化文件分析
- 自动扫描持久化目录
- 显示文件信息（大小、修改时间）
- 读取并展示文件内容

## 运行示例

```bash
cd examples/persistence
go run main.go
```

## 输出说明

示例运行后会在当前目录下创建以下目录：
- `./persistence_data` - 基本持久化数据
- `./streamsql_overflow_data` - 溢出持久化数据
- `./zero_loss_data` - 零数据丢失配置数据

每个目录包含以 `streamsql_ordered_` 开头的日志文件，文件内容为 JSON 格式的持久化数据。

## 核心 API

### PersistenceManager

```go
// 创建持久化管理器
pm := stream.NewPersistenceManager(dataDir)

// 创建带配置的持久化管理器
pm := stream.NewPersistenceManagerWithConfig(dataDir, maxFileSize, flushInterval)

// 启动管理器
err := pm.Start()

// 持久化数据
err := pm.PersistData(data)

// 加载并恢复数据
err := pm.LoadAndRecoverData()

// 获取恢复数据
data, hasMore := pm.GetRecoveryData()

// 获取统计信息
stats := pm.GetStats()

// 停止管理器
pm.Stop()
```

## 注意事项

1. 确保有足够的磁盘空间用于持久化数据
2. 持久化目录会自动创建，无需手动创建
3. 程序退出时会自动清理资源
4. 建议在生产环境中根据实际需求调整文件大小和刷新间隔