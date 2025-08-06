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

package stream

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rulego/streamsql/logger"
)

// OrderedDataItem ordered data item with sequence number and timestamp
type OrderedDataItem struct {
	SequenceID int64                  `json:"sequence_id"` // Global incremental sequence number
	Timestamp  int64                  `json:"timestamp"`   // Data reception timestamp
	Data       map[string]interface{} `json:"data"`        // Actual data
	RetryCount int                    `json:"retry_count"` // Retry count
	LastRetry  int64                  `json:"last_retry"`  // Last retry timestamp
}

// DeadLetterItem dead letter queue item
type DeadLetterItem struct {
	OriginalData OrderedDataItem `json:"original_data"` // Original data
	FailureTime  int64           `json:"failure_time"`  // Failure time
	Reason       string          `json:"reason"`        // Failure reason
}

// PersistenceManager persistence manager
// Solves data timing issues, ensures first-in-first-out (FIFO) processing
// Optimized version: adds retry limits, dead letter queue and backoff strategy
type PersistenceManager struct {
	// Basic configuration
	dataDir       string        // Persistence data directory
	maxFileSize   int64         // Maximum size per file (bytes)
	flushInterval time.Duration // Flush interval

	// Sequence number management
	sequenceCounter int64 // Global sequence counter, using atomic operations

	// File management
	currentFile *os.File // Current write file
	currentSize int64    // Current file size
	fileIndex   int      // File index

	// Concurrency control
	writeMutex   sync.Mutex   // Write mutex
	pendingMutex sync.Mutex   // Pending data mutex
	runningMutex sync.RWMutex // Read-write lock protecting isRunning field

	// Data buffering
	pendingData []OrderedDataItem // Pending data to write, sorted by sequence number

	// State management
	isRunning  bool          // Whether running
	stopChan   chan struct{} // Stop channel
	flushTimer *time.Timer   // Flush timer

	// Recovery management
	recoveryQueue chan OrderedDataItem // Recovery data queue
	recoveryMode  bool                 // Whether in recovery mode
	recoveryMutex sync.RWMutex         // Recovery mode protection lock

	// Retry and dead letter queue management
	maxRetryCount   int                        // Maximum retry count
	deadLetterQueue []DeadLetterItem           // Dead letter queue
	deadLetterMutex sync.Mutex                 // Dead letter queue protection lock
	retryDataMap    map[int64]*OrderedDataItem // Retry data mapping (indexed by sequence number)
	retryMapMutex   sync.RWMutex               // Retry mapping protection lock

	// Statistics
	totalPersisted int64 // Total persisted data count
	totalLoaded    int64 // Total loaded data count
	filesCreated   int64 // Number of files created
	totalRecovered int64 // Total recovered data count
	totalDropped   int64 // Total dropped data count (entered dead letter queue)
	totalRetried   int64 // Total retried data count
}

// NewPersistenceManager creates a persistence manager
// Parameters:
//   - dataDir: data storage directory
//
// Returns:
//   - *PersistenceManager: persistence manager instance
func NewPersistenceManager(dataDir string) *PersistenceManager {
	pm := &PersistenceManager{
		dataDir:         dataDir,
		maxFileSize:     10 * 1024 * 1024, // 10MB per file
		flushInterval:   2 * time.Second,  // Flush every 2 seconds, more frequent to ensure timing
		fileIndex:       0,
		pendingData:     make([]OrderedDataItem, 0, 1000), // Pre-allocate capacity
		stopChan:        make(chan struct{}),
		recoveryQueue:   make(chan OrderedDataItem, 10000), // Recovery queue
		sequenceCounter: 0,
		// Retry and dead letter queue configuration
		maxRetryCount:   3,                                // Default maximum 3 retries
		deadLetterQueue: make([]DeadLetterItem, 0, 1000),  // Dead letter queue
		retryDataMap:    make(map[int64]*OrderedDataItem), // Retry data mapping
	}

	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		logger.Error("Failed to create persistence directory: %v", err)
	}

	return pm
}

// NewPersistenceManagerWithConfig creates a persistence manager with custom configuration
// Parameters:
//   - dataDir: data storage directory
//   - maxFileSize: maximum size per file
//   - flushInterval: flush interval
//
// Returns:
//   - *PersistenceManager: persistence manager instance
func NewPersistenceManagerWithConfig(dataDir string, maxFileSize int64, flushInterval time.Duration) *PersistenceManager {
	pm := &PersistenceManager{
		dataDir:         dataDir,
		maxFileSize:     maxFileSize,
		flushInterval:   flushInterval,
		fileIndex:       0,
		pendingData:     make([]OrderedDataItem, 0, 1000),
		stopChan:        make(chan struct{}),
		recoveryQueue:   make(chan OrderedDataItem, 10000),
		sequenceCounter: 0,
		// Retry and dead letter queue configuration
		maxRetryCount:   3,                                // Default maximum 3 retries
		deadLetterQueue: make([]DeadLetterItem, 0, 1000),  // Dead letter queue
		retryDataMap:    make(map[int64]*OrderedDataItem), // Retry data mapping
	}

	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		logger.Error("Failed to create persistence directory: %v", err)
	}

	return pm
}

// Start starts the persistence manager
// Returns:
//   - error: error during startup process
func (pm *PersistenceManager) Start() error {
	// Check if already running
	pm.runningMutex.RLock()
	running := pm.isRunning
	pm.runningMutex.RUnlock()

	if running {
		return fmt.Errorf("ordered persistence manager already running")
	}

	// Reinitialize channels if they were closed
	pm.stopChan = make(chan struct{})
	pm.recoveryQueue = make(chan OrderedDataItem, 10000)

	// Create initial file
	pm.writeMutex.Lock()
	if err := pm.createNewFile(); err != nil {
		pm.writeMutex.Unlock()
		return fmt.Errorf("failed to create initial file: %w", err)
	}
	pm.writeMutex.Unlock()

	// Set running state
	pm.runningMutex.Lock()
	pm.isRunning = true
	pm.runningMutex.Unlock()

	// Start timed flush
	pm.startFlushTimer()

	// Start background processing goroutines
	go pm.backgroundProcessor()
	go pm.recoveryProcessor()

	// Load and recover existing data
	if err := pm.LoadAndRecoverData(); err != nil {
		logger.Error("Failed to load and recover data: %v", err)
		// Don't return error, continue running
	}

	logger.Info("Ordered persistence manager started successfully, data directory: %s", pm.dataDir)
	return nil
}

// Stop stops the persistence manager
// Returns:
//   - error: error during stop process
func (pm *PersistenceManager) Stop() error {
	// Check if running
	pm.runningMutex.RLock()
	running := pm.isRunning
	pm.runningMutex.RUnlock()

	if !running {
		return nil
	}

	// Set stop state
	pm.runningMutex.Lock()
	pm.isRunning = false
	pm.runningMutex.Unlock()

	// Close stop channel safely
	select {
	case <-pm.stopChan:
		// Channel already closed
	default:
		close(pm.stopChan)
	}

	// Stop timer
	pm.writeMutex.Lock()
	if pm.flushTimer != nil {
		pm.flushTimer.Stop()
	}
	pm.writeMutex.Unlock()

	// Flush remaining data
	pm.flushPendingData()

	// Close current file with proper synchronization
	pm.writeMutex.Lock()
	if pm.currentFile != nil {
		pm.currentFile.Close()
		pm.currentFile = nil
	}
	pm.writeMutex.Unlock()

	// Close recovery queue safely
	go func() {
		// Drain the recovery queue in a separate goroutine
		for {
			select {
			case <-pm.recoveryQueue:
				// Continue draining
			default:
				// Queue is empty, safe to close
				close(pm.recoveryQueue)
				return
			}
		}
	}()

	// Give some time for the goroutine to drain the queue
	time.Sleep(100 * time.Millisecond)

	logger.Info("Ordered persistence manager stopped")
	return nil
}

// PersistData persists data ensuring timing order (compatibility method)
// Parameters:
//   - data: data to persist, must be map[string]interface{} type
//
// Returns:
//   - error: error during persistence process
func (pm *PersistenceManager) PersistData(data map[string]interface{}) error {
	return pm.PersistDataWithRetryLimit(data, 0)
}

// PersistDataWithRetryLimit persists data with retry limit support
// Parameters:
//   - data: data to persist, must be map[string]interface{} type
//   - retryCount: current retry count
//
// Returns:
//   - error: error during persistence process
func (pm *PersistenceManager) PersistDataWithRetryLimit(data map[string]interface{}, retryCount int) error {
	// Check if running
	pm.runningMutex.RLock()
	running := pm.isRunning
	pm.runningMutex.RUnlock()

	if !running {
		return fmt.Errorf("ordered persistence manager not running")
	}

	// Assign globally unique sequence number to ensure timing order
	sequenceID := atomic.AddInt64(&pm.sequenceCounter, 1)

	// Create ordered data item
	item := OrderedDataItem{
		SequenceID: sequenceID,
		Timestamp:  time.Now().UnixNano(), // Use nanosecond timestamp
		Data:       data,
		RetryCount: retryCount,
		LastRetry:  time.Now().UnixNano(),
	}

	// If retry data, update retry mapping
	if retryCount > 0 {
		pm.retryMapMutex.Lock()
		pm.retryDataMap[sequenceID] = &item
		pm.retryMapMutex.Unlock()
		atomic.AddInt64(&pm.totalRetried, 1)
	}

	// Add to pending write queue
	pm.pendingMutex.Lock()
	pm.pendingData = append(pm.pendingData, item)
	pm.pendingMutex.Unlock()

	return nil
}

// LoadAndRecoverData loads persisted data and starts ordered recovery
// Returns:
//   - error: error during loading process
func (pm *PersistenceManager) LoadAndRecoverData() error {
	// 只加载未处理的文件（排除.processed文件）
	allFiles, err := filepath.Glob(filepath.Join(pm.dataDir, "streamsql_ordered_*.log"))
	if err != nil {
		return fmt.Errorf("failed to glob files: %w", err)
	}

	// 过滤掉已处理的文件（.processed后缀的文件）
	var files []string
	for _, file := range allFiles {
		if !strings.HasSuffix(file, ".processed") {
			files = append(files, file)
		}
	}

	if len(files) == 0 {
		logger.Info("No persistence files found for recovery")
		return nil
	}

	// Collect all data items
	var allItems []OrderedDataItem

	for _, filename := range files {
		items, err := pm.loadItemsFromFile(filename)
		if err != nil {
			logger.Error("Failed to load file %s: %v", filename, err)
			continue
		}
		allItems = append(allItems, items...)

		// 加载后直接删除文件
		if deleteErr := os.Remove(filename); deleteErr != nil {
			logger.Error("Failed to delete file %s: %v", filename, deleteErr)
		} else {
			logger.Info("File %s processed and deleted", filename)
		}
	}

	// 按序列号排序，确保时序性
	sort.Slice(allItems, func(i, j int) bool {
		return allItems[i].SequenceID < allItems[j].SequenceID
	})

	// 更新序列号计数器，确保新数据的序列号不会冲突
	if len(allItems) > 0 {
		lastSequenceID := allItems[len(allItems)-1].SequenceID
		atomic.StoreInt64(&pm.sequenceCounter, lastSequenceID)
	}

	// 启动恢复模式
	pm.recoveryMutex.Lock()
	pm.recoveryMode = true
	pm.recoveryMutex.Unlock()

	// 如果没有数据需要恢复，立即退出恢复模式
	if len(allItems) == 0 {
		pm.recoveryMutex.Lock()
		pm.recoveryMode = false
		pm.recoveryMutex.Unlock()
		logger.Info("No data to recover, exiting recovery mode")
		return nil
	}

	// 将数据放入恢复队列
	for _, item := range allItems {
		select {
		case pm.recoveryQueue <- item:
			// 数据已放入恢复队列
		case <-pm.stopChan:
			return nil
		}
	}

	logger.Info("Data recovery completed, %d items recovered in order", len(allItems))

	// 启动一个goroutine来监控恢复队列，当队列为空时退出恢复模式
	go func() {
		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if len(pm.recoveryQueue) == 0 {
					pm.recoveryMutex.Lock()
					pm.recoveryMode = false
					pm.recoveryMutex.Unlock()
					return
				}
			case <-pm.stopChan:
				return
			}
		}
	}()

	atomic.AddInt64(&pm.totalLoaded, int64(len(allItems)))
	logger.Info("Started ordered recovery of %d data items", len(allItems))
	return nil
}

// IsInRecoveryMode 检查是否处于恢复模式
// 返回值:
//   - bool: 是否处于恢复模式
func (pm *PersistenceManager) IsInRecoveryMode() bool {
	pm.recoveryMutex.RLock()
	defer pm.recoveryMutex.RUnlock()
	return pm.recoveryMode
}

// GetRecoveryData 获取一条恢复数据（非阻塞）
// 返回值:
//   - map[string]interface{}: 恢复的数据，如果没有数据则返回nil
//   - bool: 是否成功获取到数据
func (pm *PersistenceManager) GetRecoveryData() (map[string]interface{}, bool) {
	select {
	case item := <-pm.recoveryQueue:
		atomic.AddInt64(&pm.totalRecovered, 1)

		// 检查队列是否为空，如果为空则退出恢复模式
		if len(pm.recoveryQueue) == 0 {
			pm.recoveryMutex.Lock()
			pm.recoveryMode = false
			pm.recoveryMutex.Unlock()
		}

		return item.Data, true
	default:
		// 队列为空，退出恢复模式
		pm.recoveryMutex.Lock()
		pm.recoveryMode = false
		pm.recoveryMutex.Unlock()
		return nil, false
	}
}

// GetStats 获取持久化统计信息
// 返回值:
//   - map[string]interface{}: 统计信息映射
func (pm *PersistenceManager) GetStats() map[string]interface{} {
	pm.pendingMutex.Lock()
	pendingCount := len(pm.pendingData)
	pm.pendingMutex.Unlock()

	pm.writeMutex.Lock()
	currentFileSize := pm.currentSize
	fileIndex := pm.fileIndex
	totalPersisted := pm.totalPersisted
	totalLoaded := pm.totalLoaded
	filesCreated := pm.filesCreated
	pm.writeMutex.Unlock()

	pm.runningMutex.RLock()
	running := pm.isRunning
	pm.runningMutex.RUnlock()

	pm.recoveryMutex.RLock()
	recoveryMode := pm.recoveryMode
	pm.recoveryMutex.RUnlock()

	sequenceCounter := atomic.LoadInt64(&pm.sequenceCounter)
	totalRecovered := atomic.LoadInt64(&pm.totalRecovered)
	recoveryQueueLen := len(pm.recoveryQueue)

	// 获取死信队列和重试统计
	pm.deadLetterMutex.Lock()
	deadLetterCount := len(pm.deadLetterQueue)
	pm.deadLetterMutex.Unlock()

	pm.retryMapMutex.RLock()
	retryMapCount := len(pm.retryDataMap)
	pm.retryMapMutex.RUnlock()

	totalDropped := atomic.LoadInt64(&pm.totalDropped)
	totalRetried := atomic.LoadInt64(&pm.totalRetried)

	return map[string]interface{}{
		"running":            running,
		"recovery_mode":      recoveryMode,
		"data_dir":           pm.dataDir,
		"pending_count":      pendingCount,
		"current_file_size":  currentFileSize,
		"file_index":         fileIndex,
		"max_file_size":      pm.maxFileSize,
		"flush_interval":     pm.flushInterval.String(),
		"total_persisted":    totalPersisted,
		"total_loaded":       totalLoaded,
		"total_recovered":    totalRecovered,
		"files_created":      filesCreated,
		"sequence_counter":   sequenceCounter,
		"recovery_queue_len": recoveryQueueLen,
		"max_retry_count":    pm.maxRetryCount,
		"dead_letter_count":  deadLetterCount,
		"retry_map_count":    retryMapCount,
		"total_dropped":      totalDropped,
		"total_retried":      totalRetried,
	}
}

// createNewFile 创建新的持久化文件
// 返回值:
//   - error: 创建过程中的错误
func (pm *PersistenceManager) createNewFile() error {
	// 关闭当前文件
	if pm.currentFile != nil {
		pm.currentFile.Close()
	}

	// 生成新文件名，使用ordered前缀区分
	filename := fmt.Sprintf("streamsql_ordered_%d_%d.log",
		time.Now().Unix(), pm.fileIndex)
	filepath := filepath.Join(pm.dataDir, filename)

	// 创建新文件
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filepath, err)
	}

	pm.currentFile = file
	pm.currentSize = 0
	pm.fileIndex++
	pm.filesCreated++

	return nil
}

// writeItemToFile 将有序数据项写入文件
// 注意：此方法应该在writeMutex锁保护下调用
// 参数:
//   - item: 要写入的有序数据项
//
// 返回值:
//   - error: 写入过程中的错误
func (pm *PersistenceManager) writeItemToFile(item OrderedDataItem) error {
	if pm.currentFile == nil {
		return fmt.Errorf("no current file")
	}

	// 序列化数据项
	jsonData, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("failed to marshal item: %w", err)
	}

	// 添加换行符
	jsonData = append(jsonData, '\n')

	// 检查文件大小
	if pm.currentSize+int64(len(jsonData)) > pm.maxFileSize {
		if err := pm.createNewFile(); err != nil {
			return fmt.Errorf("failed to create new file: %w", err)
		}
	}

	// 写入数据
	n, err := pm.currentFile.Write(jsonData)
	if err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	pm.currentSize += int64(n)
	pm.totalPersisted++
	return nil
}

// flushPendingData 刷新待写入数据，按序列号排序后写入
func (pm *PersistenceManager) flushPendingData() {
	pm.pendingMutex.Lock()
	if len(pm.pendingData) == 0 {
		pm.pendingMutex.Unlock()
		return
	}

	// 复制数据并按序列号排序
	dataToWrite := make([]OrderedDataItem, len(pm.pendingData))
	copy(dataToWrite, pm.pendingData)
	pm.pendingData = pm.pendingData[:0] // 清空切片
	pm.pendingMutex.Unlock()

	// 按序列号排序，确保写入顺序正确
	sort.Slice(dataToWrite, func(i, j int) bool {
		return dataToWrite[i].SequenceID < dataToWrite[j].SequenceID
	})

	pm.writeMutex.Lock()
	defer pm.writeMutex.Unlock()

	// 按序写入数据
	for _, item := range dataToWrite {
		if err := pm.writeItemToFile(item); err != nil {
			logger.Error("Failed to write persistence item: %v", err)
		}
	}

	// 同步到磁盘
	if pm.currentFile != nil {
		_ = pm.currentFile.Sync()
	}
}

// startFlushTimer 启动刷新定时器
func (pm *PersistenceManager) startFlushTimer() {
	pm.writeMutex.Lock()
	pm.flushTimer = time.AfterFunc(pm.flushInterval, func() {
		// 安全地检查运行状态
		pm.runningMutex.RLock()
		running := pm.isRunning
		pm.runningMutex.RUnlock()

		if running {
			pm.flushPendingData()
			pm.startFlushTimer() // 重新启动定时器
		}
	})
	pm.writeMutex.Unlock()
}

// backgroundProcessor 后台处理协程
func (pm *PersistenceManager) backgroundProcessor() {
	ticker := time.NewTicker(500 * time.Millisecond) // 更频繁的检查
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 定期检查并处理
			pm.pendingMutex.Lock()
			pendingCount := len(pm.pendingData)
			pm.pendingMutex.Unlock()

			// 如果有待写入数据，立即刷新以保证时序性
			if pendingCount > 50 { // 降低阈值，更快响应
				pm.flushPendingData()
			}

		case <-pm.stopChan:
			return
		}
	}
}

// recoveryProcessor 恢复处理协程
func (pm *PersistenceManager) recoveryProcessor() {
	// 这个协程主要用于监控恢复状态，实际恢复数据由GetRecoveryData方法提供
	ticker := time.NewTicker(100 * time.Millisecond) // 更频繁地检查
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pm.recoveryMutex.RLock()
			recoveryMode := pm.recoveryMode
			pm.recoveryMutex.RUnlock()

			if recoveryMode {
				queueLen := len(pm.recoveryQueue)
				if queueLen == 0 {
					// 队列为空，退出恢复模式
					pm.recoveryMutex.Lock()
					pm.recoveryMode = false
					pm.recoveryMutex.Unlock()
					logger.Debug("Recovery queue empty, exiting recovery mode")
					return
				} else {
					logger.Debug("Recovery in progress, %d items remaining in queue", queueLen)
				}
			} else {
				// 不在恢复模式，退出协程
				return
			}

		case <-pm.stopChan:
			return
		}
	}
}

// loadItemsFromFile 从文件加载有序数据项
// 参数:
//   - filename: 要加载的文件名
//
// 返回值:
//   - []OrderedDataItem: 加载的有序数据项列表
//   - error: 加载过程中的错误
func (pm *PersistenceManager) loadItemsFromFile(filename string) ([]OrderedDataItem, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filename, err)
	}
	defer file.Close()

	var items []OrderedDataItem
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		var item OrderedDataItem

		if err := json.Unmarshal([]byte(line), &item); err != nil {
			logger.Error("Failed to parse data line: %v", err)
			continue
		}

		items = append(items, item)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan file: %w", err)
	}

	return items, nil
}

// RetryFailedData 重试失败的数据
// 参数:
//   - sequenceID: 要重试的数据序列号
//   - reason: 失败原因
//
// 返回值:
//   - error: 重试过程中的错误
func (pm *PersistenceManager) RetryFailedData(sequenceID int64, reason string) error {
	pm.retryMapMutex.RLock()
	item, exists := pm.retryDataMap[sequenceID]
	pm.retryMapMutex.RUnlock()

	if !exists {
		return fmt.Errorf("data with sequence ID %d not found in retry map", sequenceID)
	}

	// 检查重试次数
	if item.RetryCount >= pm.maxRetryCount {
		// 移动到死信队列
		return pm.moveToDeadLetterQueue(*item, reason)
	}

	// 增加重试次数并重新持久化
	return pm.PersistDataWithRetryLimit(item.Data, item.RetryCount+1)
}

// moveToDeadLetterQueue 将数据移动到死信队列
// 参数:
//   - item: 要移动的数据项
//   - reason: 失败原因
//
// 返回值:
//   - error: 移动过程中的错误
func (pm *PersistenceManager) moveToDeadLetterQueue(item OrderedDataItem, reason string) error {
	deadLetterItem := DeadLetterItem{
		OriginalData: item,
		FailureTime:  time.Now().UnixNano(),
		Reason:       reason,
	}

	pm.deadLetterMutex.Lock()
	pm.deadLetterQueue = append(pm.deadLetterQueue, deadLetterItem)
	pm.deadLetterMutex.Unlock()

	// 从重试映射中移除
	pm.retryMapMutex.Lock()
	delete(pm.retryDataMap, item.SequenceID)
	pm.retryMapMutex.Unlock()

	atomic.AddInt64(&pm.totalDropped, 1)
	logger.Warn("Data moved to dead letter queue, sequence ID: %d, reason: %s", item.SequenceID, reason)
	return nil
}

// GetDeadLetterQueue 获取死信队列数据
// 返回值:
//   - []DeadLetterItem: 死信队列中的所有数据
func (pm *PersistenceManager) GetDeadLetterQueue() []DeadLetterItem {
	pm.deadLetterMutex.Lock()
	defer pm.deadLetterMutex.Unlock()

	// 返回副本以避免并发问题
	result := make([]DeadLetterItem, len(pm.deadLetterQueue))
	copy(result, pm.deadLetterQueue)
	return result
}

// ClearDeadLetterQueue 清空死信队列
// 返回值:
//   - int: 清空的数据项数量
func (pm *PersistenceManager) ClearDeadLetterQueue() int {
	pm.deadLetterMutex.Lock()
	defer pm.deadLetterMutex.Unlock()

	count := len(pm.deadLetterQueue)
	pm.deadLetterQueue = pm.deadLetterQueue[:0]
	return count
}

// SetMaxRetryCount 设置最大重试次数
// 参数:
//   - maxRetryCount: 最大重试次数
func (pm *PersistenceManager) SetMaxRetryCount(maxRetryCount int) {
	pm.maxRetryCount = maxRetryCount
}

// ShouldRetryRecoveredData 检查恢复数据是否应该重试
// 参数:
//   - data: 恢复的数据
//
// 返回值:
//   - bool: 是否应该重试
func (pm *PersistenceManager) ShouldRetryRecoveredData(data map[string]interface{}) bool {
	// 检查数据中的重试次数（支持retry和_retry_count字段）
	if retryCountFloat, exists := data["retry"]; exists {
		if retryCount, ok := retryCountFloat.(float64); ok {
			if int(retryCount) >= pm.maxRetryCount {
				return false
			}
		}
		if retryCount, ok := retryCountFloat.(int); ok {
			if retryCount >= pm.maxRetryCount {
				return false
			}
		}
	}

	// 兼容性检查：也检查_retry_count字段
	if retryCountFloat, exists := data["_retry_count"]; exists {
		if retryCount, ok := retryCountFloat.(float64); ok {
			if int(retryCount) >= pm.maxRetryCount {
				return false
			}
		}
		if retryCount, ok := retryCountFloat.(int); ok {
			if retryCount >= pm.maxRetryCount {
				return false
			}
		}
	}

	// 尝试从数据中获取序列号和重试次数
	if sequenceIDFloat, exists := data["_sequence_id"]; exists {
		if sequenceID, ok := sequenceIDFloat.(float64); ok {
			pm.retryMapMutex.RLock()
			item, exists := pm.retryDataMap[int64(sequenceID)]
			pm.retryMapMutex.RUnlock()

			if exists && item.RetryCount >= pm.maxRetryCount {
				return false
			}
		}
	}

	// 如果没有找到重试信息，允许重试（可能是第一次失败）
	return true
}

// MoveToDeadLetterQueue 将数据移动到死信队列（公共方法）
// 参数:
//   - data: 要移动的数据
func (pm *PersistenceManager) MoveToDeadLetterQueue(data map[string]interface{}) {
	// 创建一个临时的OrderedDataItem
	item := OrderedDataItem{
		SequenceID: atomic.AddInt64(&pm.sequenceCounter, 1),
		Timestamp:  time.Now().UnixNano(),
		Data:       data,
		RetryCount: pm.maxRetryCount + 1, // 标记为超过重试限制
		LastRetry:  time.Now().UnixNano(),
	}

	pm.moveToDeadLetterQueue(item, "exceeded retry limit during recovery")
}

// RePersistRecoveredData 重新持久化恢复数据（增加重试计数）
// 参数:
//   - data: 要重新持久化的数据
//
// 返回值:
//   - error: 重新持久化过程中的错误
func (pm *PersistenceManager) RePersistRecoveredData(data map[string]interface{}) error {
	// 尝试从数据中获取序列号和重试次数
	retryCount := 1 // 默认重试次数

	if sequenceIDFloat, exists := data["_sequence_id"]; exists {
		if sequenceID, ok := sequenceIDFloat.(float64); ok {
			pm.retryMapMutex.RLock()
			item, exists := pm.retryDataMap[int64(sequenceID)]
			pm.retryMapMutex.RUnlock()

			if exists {
				retryCount = item.RetryCount + 1
			}
		}
	}

	// 在数据中添加序列号信息以便后续跟踪
	data["_sequence_id"] = atomic.LoadInt64(&pm.sequenceCounter)
	data["_retry_count"] = retryCount
	data["_last_retry"] = time.Now().UnixNano()

	return pm.PersistDataWithRetryLimit(data, retryCount)
}
