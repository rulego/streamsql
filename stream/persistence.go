package stream

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rulego/streamsql/logger"
)

// PersistenceManager 数据持久化管理器
type PersistenceManager struct {
	dataDir       string        // 持久化数据目录
	maxFileSize   int64         // 单个文件最大大小（字节）
	flushInterval time.Duration // 刷新间隔
	currentFile   *os.File      // 当前写入文件
	currentSize   int64         // 当前文件大小
	fileIndex     int           // 文件索引
	writeMutex    sync.Mutex    // 写入互斥锁
	flushTimer    *time.Timer   // 刷新定时器
	pendingData   []interface{} // 待写入数据
	pendingMutex  sync.Mutex    // 待写入数据互斥锁
	isRunning     bool          // 是否运行中
	stopChan      chan struct{} // 停止通道

	// 统计信息 (新增)
	totalPersisted int64
	totalLoaded    int64
	filesCreated   int64
}

// NewPersistenceManager 创建默认配置的持久化管理器
func NewPersistenceManager(dataDir string) *PersistenceManager {
	pm := &PersistenceManager{
		dataDir:       dataDir,
		maxFileSize:   10 * 1024 * 1024, // 10MB per file
		flushInterval: 5 * time.Second,  // 5秒刷新一次
		fileIndex:     0,
		pendingData:   make([]interface{}, 0),
		stopChan:      make(chan struct{}),
	}

	// 确保数据目录存在
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		logger.Error("Failed to create persistence directory: %v", err)
	}

	return pm
}

// NewPersistenceManagerWithConfig 创建自定义配置的持久化管理器
func NewPersistenceManagerWithConfig(dataDir string, maxFileSize int64, flushInterval time.Duration) *PersistenceManager {
	pm := &PersistenceManager{
		dataDir:       dataDir,
		maxFileSize:   maxFileSize,
		flushInterval: flushInterval,
		fileIndex:     0,
		pendingData:   make([]interface{}, 0),
		stopChan:      make(chan struct{}),
	}

	// 确保数据目录存在
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		logger.Error("Failed to create persistence directory: %v", err)
	}

	return pm
}

// Start 启动持久化管理器
func (pm *PersistenceManager) Start() error {
	pm.writeMutex.Lock()
	defer pm.writeMutex.Unlock()

	if pm.isRunning {
		return fmt.Errorf("persistence manager already running")
	}

	// 创建初始文件
	if err := pm.createNewFile(); err != nil {
		return fmt.Errorf("failed to create initial file: %w", err)
	}

	pm.isRunning = true

	// 启动定时刷新
	pm.startFlushTimer()

	// 启动后台处理协程
	go pm.backgroundProcessor()

	logger.Info("Persistence manager started successfully, data directory: %s", pm.dataDir)
	return nil
}

// Stop 停止持久化管理器
func (pm *PersistenceManager) Stop() error {
	pm.writeMutex.Lock()
	defer pm.writeMutex.Unlock()

	if !pm.isRunning {
		return nil
	}

	pm.isRunning = false
	close(pm.stopChan)

	// 停止定时器
	if pm.flushTimer != nil {
		pm.flushTimer.Stop()
	}

	// 刷新剩余数据
	pm.flushPendingData()

	// 关闭当前文件
	if pm.currentFile != nil {
		pm.currentFile.Close()
	}

	logger.Info("Persistence manager stopped")
	return nil
}

// PersistData 持久化单条数据
func (pm *PersistenceManager) PersistData(data interface{}) error {
	if !pm.isRunning {
		return fmt.Errorf("persistence manager not running")
	}

	pm.pendingMutex.Lock()
	pm.pendingData = append(pm.pendingData, data)
	pm.totalPersisted++
	pm.pendingMutex.Unlock()

	return nil
}

// LoadPersistedData 加载并删除持久化数据
func (pm *PersistenceManager) LoadPersistedData() ([]interface{}, error) {
	files, err := filepath.Glob(filepath.Join(pm.dataDir, "streamsql_overflow_*.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to glob files: %w", err)
	}

	var allData []interface{}

	for _, filename := range files {
		data, err := pm.loadDataFromFile(filename)
		if err != nil {
			logger.Error("Failed to load file %s: %v", filename, err)
			continue
		}
		allData = append(allData, data...)
		pm.totalLoaded += int64(len(data))

		// 加载后删除文件
		if err := os.Remove(filename); err != nil {
			logger.Error("Failed to delete loaded file %s: %v", filename, err)
		}
	}

	logger.Info("Loaded %d data records from persistence files", len(allData))
	return allData, nil
}

// GetStats 获取持久化统计信息
func (pm *PersistenceManager) GetStats() map[string]interface{} {
	pm.pendingMutex.Lock()
	pendingCount := len(pm.pendingData)
	pm.pendingMutex.Unlock()

	pm.writeMutex.Lock()
	currentFileSize := pm.currentSize
	fileIndex := pm.fileIndex
	pm.writeMutex.Unlock()

	return map[string]interface{}{
		"running":           pm.isRunning,
		"data_dir":          pm.dataDir,
		"pending_count":     pendingCount,
		"current_file_size": currentFileSize,
		"file_index":        fileIndex,
		"max_file_size":     pm.maxFileSize,
		"flush_interval":    pm.flushInterval.String(),
		"total_persisted":   pm.totalPersisted,
		"total_loaded":      pm.totalLoaded,
		"files_created":     pm.filesCreated,
	}
}

// createNewFile 创建新的持久化文件
func (pm *PersistenceManager) createNewFile() error {
	// 关闭当前文件
	if pm.currentFile != nil {
		pm.currentFile.Close()
	}

	// 生成新文件名
	filename := fmt.Sprintf("streamsql_overflow_%d_%d.log",
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

	logger.Info("Created new persistence file: %s", filepath)
	return nil
}

// writeDataToFile 将数据写入文件
func (pm *PersistenceManager) writeDataToFile(data interface{}) error {
	if pm.currentFile == nil {
		return fmt.Errorf("no current file")
	}

	// 序列化数据
	jsonData, err := json.Marshal(map[string]interface{}{
		"timestamp": time.Now().Unix(),
		"data":      data,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
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
	return nil
}

// flushPendingData 刷新待写入数据
func (pm *PersistenceManager) flushPendingData() {
	pm.pendingMutex.Lock()
	dataToWrite := make([]interface{}, len(pm.pendingData))
	copy(dataToWrite, pm.pendingData)
	pm.pendingData = pm.pendingData[:0] // 清空切片
	pm.pendingMutex.Unlock()

	if len(dataToWrite) == 0 {
		return
	}

	pm.writeMutex.Lock()
	defer pm.writeMutex.Unlock()

	// 批量写入数据
	for _, data := range dataToWrite {
		if err := pm.writeDataToFile(data); err != nil {
			logger.Error("Failed to write persistence data: %v", err)
		}
	}

	// 同步到磁盘
	if pm.currentFile != nil {
		_ = pm.currentFile.Sync()
	}

	logger.Info("Flushed %d pending data records to disk", len(dataToWrite))
}

// startFlushTimer 启动刷新定时器
func (pm *PersistenceManager) startFlushTimer() {
	pm.flushTimer = time.AfterFunc(pm.flushInterval, func() {
		if pm.isRunning {
			pm.flushPendingData()
			pm.startFlushTimer() // 重新启动定时器
		}
	})
}

// backgroundProcessor 后台处理协程
func (pm *PersistenceManager) backgroundProcessor() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 定期检查并处理
			pm.pendingMutex.Lock()
			pendingCount := len(pm.pendingData)
			pm.pendingMutex.Unlock()

			// 如果有大量待写入数据，立即刷新
			if pendingCount > 100 {
				pm.flushPendingData()
			}

		case <-pm.stopChan:
			return
		}
	}
}

// loadDataFromFile 从文件加载数据
func (pm *PersistenceManager) loadDataFromFile(filename string) ([]interface{}, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filename, err)
	}
	defer file.Close()

	var data []interface{}
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		var record map[string]interface{}

		if err := json.Unmarshal([]byte(line), &record); err != nil {
			logger.Error("Failed to parse data line: %v", err)
			continue
		}

		// 提取实际数据
		if actualData, ok := record["data"]; ok {
			data = append(data, actualData)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan file: %w", err)
	}

	return data, nil
}
