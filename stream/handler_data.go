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

// safeGetDataChan 线程安全地获取dataChan引用
func (s *Stream) safeGetDataChan() chan map[string]interface{} {
	s.dataChanMux.RLock()
	defer s.dataChanMux.RUnlock()
	return s.dataChan
}

// safeSendToDataChan 线程安全地向dataChan发送数据
func (s *Stream) safeSendToDataChan(data map[string]interface{}) bool {
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
	newChan := make(chan map[string]interface{}, newCap)

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

// checkAndProcessRecoveryDataOptimized 优化版恢复数据处理
// 解决溢出侧漏问题，实现指数退避和重试限制
func (s *Stream) checkAndProcessRecoveryDataOptimized() {
	// 防止重复启动恢复协程
	if atomic.LoadInt32(&s.activeRetries) >= s.maxRetryRoutines {
		return
	}

	atomic.AddInt32(&s.activeRetries, 1)
	defer atomic.AddInt32(&s.activeRetries, -1)

	// 检查是否有持久化管理器
	if s.persistenceManager == nil {
		return
	}

	// 退避策略参数
	baseBackoff := 100 * time.Millisecond
	maxBackoff := 5 * time.Second
	currentBackoff := baseBackoff
	consecutiveFailures := 0
	maxConsecutiveFailures := 10

	// 持续检查恢复数据，直到没有更多数据或Stream停止
	ticker := time.NewTicker(currentBackoff)
	defer ticker.Stop()

	maxProcessTime := 30 * time.Second // 最大处理时间
	timeout := time.NewTimer(maxProcessTime)
	defer timeout.Stop()

	processedCount := 0
	droppedCount := 0

	for {
		select {
		case <-ticker.C:
			// 尝试获取恢复数据
			if recoveredData, hasData := s.persistenceManager.GetRecoveryData(); hasData {
				// 尝试发送恢复数据到处理通道
				if s.safeSendToDataChan(recoveredData) {
					processedCount++
					consecutiveFailures = 0
					// 重置退避时间
					currentBackoff = baseBackoff
					ticker.Reset(currentBackoff)
					logger.Debug("Successfully processed recovered data item %d", processedCount)
				} else {
					consecutiveFailures++

					// 检查是否应该重试这条数据
					if !s.persistenceManager.ShouldRetryRecoveredData(recoveredData) {
						// 超过重试限制，移入死信队列
						logger.Warn("Recovered data exceeded retry limit, moving to dead letter queue")
						s.persistenceManager.MoveToDeadLetterQueue(recoveredData)
						droppedCount++
					} else {
						// 重新持久化这条数据（增加重试计数）
						if err := s.persistenceManager.RePersistRecoveredData(recoveredData); err != nil {
							logger.Error("Failed to re-persist recovered data: %v", err)
							atomic.AddInt64(&s.droppedCount, 1)
							droppedCount++
						}
					}

					// 实现指数退避
					if consecutiveFailures >= maxConsecutiveFailures {
						logger.Warn("Too many consecutive failures (%d), stopping recovery processing", consecutiveFailures)
						return
					}

					// 增加退避时间
					currentBackoff = time.Duration(float64(currentBackoff) * 1.5)
					if currentBackoff > maxBackoff {
						currentBackoff = maxBackoff
					}
					ticker.Reset(currentBackoff)
					logger.Debug("Channel full, backing off for %v (failure #%d)", currentBackoff, consecutiveFailures)
				}
			} else {
				// 没有更多恢复数据，检查是否还在恢复模式
				if !s.persistenceManager.IsInRecoveryMode() {
					logger.Info("Recovery completed: processed %d items, dropped %d items", processedCount, droppedCount)
					return
				}
				// 没有数据时也重置退避时间
				currentBackoff = baseBackoff
				ticker.Reset(currentBackoff)
			}

		case <-timeout.C:
			logger.Warn("Recovery processing timeout reached: processed %d items, dropped %d items", processedCount, droppedCount)
			return

		case <-s.done:
			logger.Info("Stream stopped during recovery processing: processed %d items, dropped %d items", processedCount, droppedCount)
			return
		}
	}
}

// checkAndProcessRecoveryData 保留原有方法以保持兼容性
// 这个方法会持续检查是否有恢复数据需要处理，确保数据按序处理
// 已弃用：请使用 checkAndProcessRecoveryDataOptimized
func (s *Stream) checkAndProcessRecoveryData() {
	s.checkAndProcessRecoveryDataOptimized()
}
