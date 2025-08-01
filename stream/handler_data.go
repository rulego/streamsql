package stream

import (
	"sync/atomic"
	"time"

	"github.com/rulego/streamsql/logger"
)

// DataHandler 数据处理器，负责不同策略的数据添加
type DataHandler struct {
	stream *Stream
}

// NewDataHandler 创建数据处理器
func NewDataHandler(stream *Stream) *DataHandler {
	return &DataHandler{stream: stream}
}

// addDataBlocking 阻塞模式添加数据，保证零数据丢失
func (s *Stream) addDataBlocking(data interface{}) {
	if s.blockingTimeout <= 0 {
		// 无超时限制，永久阻塞直到成功
		dataChan := s.safeGetDataChan()
		dataChan <- data
		return
	}

	// 带超时的阻塞
	timer := time.NewTimer(s.blockingTimeout)
	defer timer.Stop()

	dataChan := s.safeGetDataChan()
	select {
	case dataChan <- data:
		// 成功添加数据
		return
	case <-timer.C:
		// 超时但不丢弃数据，记录错误但继续阻塞
		logger.Error("Data addition timeout, but continue waiting to avoid data loss")
		// 继续无限期阻塞，重新获取当前通道引用
		finalDataChan := s.safeGetDataChan()
		finalDataChan <- data
	}
}

// addDataWithExpansion 动态扩容模式
func (s *Stream) addDataWithExpansion(data interface{}) {
	// 首次尝试添加数据
	if s.safeSendToDataChan(data) {
		return
	}

	// 通道满了，动态扩容
	s.expandDataChannel()

	// 扩容后重试，重新获取通道引用
	if s.safeSendToDataChan(data) {
		logger.Debug("Successfully added data after data channel expansion")
		return
	}

	// 如果扩容后仍然满，则阻塞等待
	dataChan := s.safeGetDataChan()
	dataChan <- data
}

// addDataWithPersistence 持久化模式
func (s *Stream) addDataWithPersistence(data interface{}) {
	// 首次尝试添加数据
	if s.safeSendToDataChan(data) {
		return
	}

	// 通道满了，持久化到磁盘
	if s.persistenceManager != nil {
		if err := s.persistenceManager.PersistData(data); err != nil {
			logger.Error("Failed to persist data: %v", err)
			atomic.AddInt64(&s.droppedCount, 1)
		} else {
			logger.Debug("Data has been persisted to disk")
		}
	} else {
		logger.Error("Persistence manager not initialized, data will be lost")
		atomic.AddInt64(&s.droppedCount, 1)
	}

	// 启动异步重试
	go s.persistAndRetryData(data)
}

// addDataWithDrop 丢弃模式
func (s *Stream) addDataWithDrop(data interface{}) {
	// 智能非阻塞添加，分层背压控制
	if s.safeSendToDataChan(data) {
		return
	}

	// 数据通道已满，使用分层背压策略，获取通道状态
	s.dataChanMux.RLock()
	chanLen := len(s.dataChan)
	chanCap := cap(s.dataChan)
	currentDataChan := s.dataChan
	s.dataChanMux.RUnlock()

	usage := float64(chanLen) / float64(chanCap)

	// 根据通道使用率和缓冲区大小调整策略
	var waitTime time.Duration
	var maxRetries int

	switch {
	case chanCap >= 100000: // 超大缓冲区（基准测试模式）
		switch {
		case usage > 0.99:
			waitTime = 1 * time.Millisecond // 更长等待
			maxRetries = 3
		case usage > 0.95:
			waitTime = 500 * time.Microsecond
			maxRetries = 2
		case usage > 0.90:
			waitTime = 100 * time.Microsecond
			maxRetries = 1
		default:
			// 立即丢弃
			logger.Warn("Data channel is full, dropping input data")
			atomic.AddInt64(&s.droppedCount, 1)
			return
		}

	case chanCap >= 50000: // 高性能模式
		switch {
		case usage > 0.99:
			waitTime = 500 * time.Microsecond
			maxRetries = 2
		case usage > 0.95:
			waitTime = 200 * time.Microsecond
			maxRetries = 1
		case usage > 0.90:
			waitTime = 50 * time.Microsecond
			maxRetries = 1
		default:
			logger.Warn("Data channel is full, dropping input data")
			atomic.AddInt64(&s.droppedCount, 1)
			return
		}

	default: // 默认模式
		switch {
		case usage > 0.99:
			waitTime = 100 * time.Microsecond
			maxRetries = 1
		case usage > 0.95:
			waitTime = 50 * time.Microsecond
			maxRetries = 1
		default:
			logger.Warn("Data channel is full, dropping input data")
			atomic.AddInt64(&s.droppedCount, 1)
			return
		}
	}

	// 多次重试添加数据，使用线程安全的方式
	for retry := 0; retry < maxRetries; retry++ {
		timer := time.NewTimer(waitTime)
		select {
		case currentDataChan <- data:
			// 重试成功
			timer.Stop()
			return
		case <-timer.C:
			// 超时，继续下一次重试或者丢弃
			if retry == maxRetries-1 {
				// 最后一次重试失败，记录丢弃
				logger.Warn("Data channel is full, dropping input data")
				atomic.AddInt64(&s.droppedCount, 1)
			}
		}
	}
}

// safeGetDataChan 线程安全地获取dataChan引用
func (s *Stream) safeGetDataChan() chan interface{} {
	s.dataChanMux.RLock()
	defer s.dataChanMux.RUnlock()
	return s.dataChan
}

// safeSendToDataChan 线程安全地向dataChan发送数据
func (s *Stream) safeSendToDataChan(data interface{}) bool {
	dataChan := s.safeGetDataChan()
	select {
	case dataChan <- data:
		return true
	default:
		return false
	}
}

// expandDataChannel 动态扩容数据通道
func (s *Stream) expandDataChannel() {
	// 使用原子操作检查是否正在扩容，防止并发扩容
	if !atomic.CompareAndSwapInt32(&s.expanding, 0, 1) {
		logger.Debug("Channel expansion already in progress, skipping")
		return
	}
	defer atomic.StoreInt32(&s.expanding, 0)

	// 获取扩容锁，确保只有一个协程进行扩容
	s.expansionMux.Lock()
	defer s.expansionMux.Unlock()

	// 再次检查是否需要扩容（双重检查锁定模式）
	s.dataChanMux.RLock()
	oldCap := cap(s.dataChan)
	currentLen := len(s.dataChan)
	s.dataChanMux.RUnlock()

	// 如果当前通道使用率低于80%，则不需要扩容
	if float64(currentLen)/float64(oldCap) < 0.8 {
		logger.Debug("Channel usage below threshold, expansion not needed")
		return
	}

	newCap := int(float64(oldCap) * 1.5) // 扩容50%
	if newCap < oldCap+1000 {
		newCap = oldCap + 1000 // 至少增加1000
	}

	logger.Debug("Dynamic expansion of data channel: %d -> %d", oldCap, newCap)

	// 创建新的更大的通道
	newChan := make(chan interface{}, newCap)

	// 使用写锁安全地迁移数据
	s.dataChanMux.Lock()
	oldChan := s.dataChan

	// 将旧通道中的数据快速迁移到新通道
	migrationTimeout := time.NewTimer(5 * time.Second) // 5秒迁移超时
	defer migrationTimeout.Stop()

	migratedCount := 0
	for {
		select {
		case data := <-oldChan:
			select {
			case newChan <- data:
				migratedCount++
			case <-migrationTimeout.C:
				logger.Warn("Data migration timeout, some data may be lost during expansion")
				goto migration_done
			}
		case <-migrationTimeout.C:
			logger.Warn("Data migration timeout during channel drain")
			goto migration_done
		default:
			// 旧通道为空，迁移完成
			goto migration_done
		}
	}

migration_done:
	// 原子性地更新通道引用
	s.dataChan = newChan
	s.dataChanMux.Unlock()

	logger.Debug("Channel expansion completed: migrated %d items", migratedCount)
}

// persistAndRetryData 持久化数据并重试 (改进版本，具备指数退避和资源控制)
func (s *Stream) persistAndRetryData(data interface{}) {
	// 检查活跃重试协程数量，防止资源泄漏
	currentRetries := atomic.LoadInt32(&s.activeRetries)
	if currentRetries >= s.maxRetryRoutines {
		logger.Warn("Maximum retry routines reached (%d), dropping data", currentRetries)
		atomic.AddInt64(&s.droppedCount, 1)
		return
	}

	// 增加活跃重试计数
	atomic.AddInt32(&s.activeRetries, 1)
	defer atomic.AddInt32(&s.activeRetries, -1)

	// 使用指数退避策略
	baseInterval := 50 * time.Millisecond
	maxInterval := 2 * time.Second
	maxRetries := 10                 // 减少最大重试次数，防止长时间阻塞
	totalTimeout := 30 * time.Second // 总超时时间

	retryTimer := time.NewTimer(totalTimeout)
	defer retryTimer.Stop()

	for attempt := 0; attempt < maxRetries; attempt++ {
		// 计算当前重试间隔（指数退避）
		currentInterval := time.Duration(float64(baseInterval) * (1.5 * float64(attempt)))
		if currentInterval > maxInterval {
			currentInterval = maxInterval
		}

		// 等待重试间隔
		waitTimer := time.NewTimer(currentInterval)
		select {
		case <-waitTimer.C:
			// 继续重试
		case <-retryTimer.C:
			waitTimer.Stop()
			logger.Warn("Persistence retry timeout reached, dropping data")
			atomic.AddInt64(&s.droppedCount, 1)
			return
		case <-s.done:
			waitTimer.Stop()
			logger.Debug("Stream stopped during retry, dropping data")
			atomic.AddInt64(&s.droppedCount, 1)
			return
		}
		waitTimer.Stop()

		// 使用线程安全方式尝试发送数据
		s.dataChanMux.RLock()
		currentDataChan := s.dataChan
		s.dataChanMux.RUnlock()

		select {
		case currentDataChan <- data:
			logger.Debug("Persistence data retry successful: attempt %d", attempt+1)
			return
		case <-retryTimer.C:
			logger.Warn("Persistence retry timeout during send, dropping data")
			atomic.AddInt64(&s.droppedCount, 1)
			return
		case <-s.done:
			logger.Debug("Stream stopped during retry send, dropping data")
			atomic.AddInt64(&s.droppedCount, 1)
			return
		default:
			// 通道仍然满，继续下一次重试
			if attempt == maxRetries-1 {
				logger.Error("Persistence data retry failed after %d attempts, dropping data", maxRetries)
				atomic.AddInt64(&s.droppedCount, 1)
			} else {
				logger.Debug("Persistence retry attempt %d/%d failed, will retry with interval %v",
					attempt+1, maxRetries, currentInterval)
			}
		}
	}
}
