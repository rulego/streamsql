package stream

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/types"
)

// Overflow strategy constants
const (
	StrategyDrop    = "drop"    // Drop strategy
	StrategyBlock   = "block"   // Blocking strategy
	StrategyExpand  = "expand"  // Dynamic strategy
	StrategyPersist = "persist" // Persistence strategy (todo: incomplete)
)

// DataProcessingStrategy data processing strategy interface
// Defines unified interface for different overflow strategies, providing better extensibility and maintainability
type DataProcessingStrategy interface {
	// ProcessData core method for processing data
	// Parameters:
	//   - data: data to process, must be map[string]interface{} type
	ProcessData(data map[string]interface{})

	// GetStrategyName gets strategy name
	GetStrategyName() string

	// Init initializes strategy
	// Parameters:
	//   - stream: Stream instance reference
	//   - config: performance configuration
	Init(stream *Stream, config types.PerformanceConfig) error

	// Stop stops and cleans up resources
	Stop() error
}

// BlockingStrategy blocking strategy implementation
type BlockingStrategy struct {
	stream *Stream
}

// NewBlockingStrategy creates blocking strategy instance
func NewBlockingStrategy() *BlockingStrategy {
	return &BlockingStrategy{}
}

// ProcessData implements blocking mode data processing
func (bs *BlockingStrategy) ProcessData(data map[string]interface{}) {
	if bs.stream.blockingTimeout <= 0 {
		// No timeout limit, block permanently until success
		dataChan := bs.stream.safeGetDataChan()
		dataChan <- data
		return
	}

	// Blocking with timeout
	timer := time.NewTimer(bs.stream.blockingTimeout)
	defer timer.Stop()

	dataChan := bs.stream.safeGetDataChan()
	select {
	case dataChan <- data:
		// Successfully added data
		return
	case <-timer.C:
		// Timeout but don't drop data, log error but continue blocking
		logger.Error("Data addition timeout, but continue waiting to avoid data loss")
		// Continue blocking indefinitely, re-get current channel reference
		finalDataChan := bs.stream.safeGetDataChan()
		finalDataChan <- data
	}
}

// GetStrategyName gets strategy name
func (bs *BlockingStrategy) GetStrategyName() string {
	return StrategyBlock
}

// Init initializes blocking strategy
func (bs *BlockingStrategy) Init(stream *Stream, config types.PerformanceConfig) error {
	bs.stream = stream
	return nil
}

// Stop stops and cleans up blocking strategy resources
func (bs *BlockingStrategy) Stop() error {
	return nil
}

// ExpansionStrategy expansion strategy implementation
type ExpansionStrategy struct {
	stream *Stream
}

// NewExpansionStrategy creates expansion strategy instance
func NewExpansionStrategy() *ExpansionStrategy {
	return &ExpansionStrategy{}
}

// ProcessData 实现扩容模式数据处理
func (es *ExpansionStrategy) ProcessData(data map[string]interface{}) {
	// 首次尝试添加数据
	if es.stream.safeSendToDataChan(data) {
		return
	}

	// 通道满了，动态扩容
	es.stream.expandDataChannel()

	// 扩容后重试，重新获取通道引用
	if es.stream.safeSendToDataChan(data) {
		logger.Debug("Successfully added data after data channel expansion")
		return
	}

	// 如果扩容后仍然满，则阻塞等待
	dataChan := es.stream.safeGetDataChan()
	dataChan <- data
}

// GetStrategyName 获取策略名称
func (es *ExpansionStrategy) GetStrategyName() string {
	return StrategyExpand
}

// Init 初始化扩容策略
func (es *ExpansionStrategy) Init(stream *Stream, config types.PerformanceConfig) error {
	es.stream = stream
	return nil
}

// Stop 停止并清理扩容策略资源
func (es *ExpansionStrategy) Stop() error {
	return nil
}

// PersistenceStrategy 持久化策略实现
type PersistenceStrategy struct {
	stream *Stream
}

// NewPersistenceStrategy 创建持久化策略实例
func NewPersistenceStrategy() *PersistenceStrategy {
	return &PersistenceStrategy{}
}

// ProcessData 实现持久化模式数据处理
func (ps *PersistenceStrategy) ProcessData(data map[string]interface{}) {
	// 检查是否处于恢复模式，如果是则优先处理恢复数据
	if ps.stream.persistenceManager != nil && ps.stream.persistenceManager.IsInRecoveryMode() {
		// 恢复模式下，先尝试处理恢复数据
		if recoveredData, hasData := ps.stream.persistenceManager.GetRecoveryData(); hasData {
			// 优先处理恢复的数据
			if ps.stream.safeSendToDataChan(recoveredData) {
				// 恢复数据处理成功，现在处理新数据
				if ps.stream.safeSendToDataChan(data) {
					return
				}
				// 新数据无法处理，持久化（带重试限制）
				if err := ps.stream.persistenceManager.PersistDataWithRetryLimit(data, 0); err != nil {
					logger.Error("Failed to persist new data during recovery: %v", err)
					atomic.AddInt64(&ps.stream.droppedCount, 1)
				}
				return
			} else {
				// 恢复数据也无法处理，检查重试次数避免无限循环
				if !ps.stream.persistenceManager.ShouldRetryRecoveredData(recoveredData) {
					// 超过重试限制，移入死信队列
					logger.Warn("Recovered data exceeded retry limit, moving to dead letter queue")
					ps.stream.persistenceManager.MoveToDeadLetterQueue(recoveredData)
				} else {
					// 重新持久化恢复数据（增加重试计数）
					if err := ps.stream.persistenceManager.RePersistRecoveredData(recoveredData); err != nil {
						logger.Error("Failed to re-persist recovered data: %v", err)
					}
				}
				// 持久化新数据
				if err := ps.stream.persistenceManager.PersistDataWithRetryLimit(data, 0); err != nil {
					logger.Error("Failed to persist new data: %v", err)
					atomic.AddInt64(&ps.stream.droppedCount, 1)
				}
				return
			}
		}
	}

	// 正常模式或非恢复模式，首次尝试添加数据
	if ps.stream.safeSendToDataChan(data) {
		return
	}

	// 通道满了，使用持久化（带重试限制）
	if ps.stream.persistenceManager != nil {
		if err := ps.stream.persistenceManager.PersistDataWithRetryLimit(data, 0); err != nil {
			logger.Error("Failed to persist data with persistence: %v", err)
			atomic.AddInt64(&ps.stream.droppedCount, 1)
		} else {
			logger.Debug("Data has been persisted to disk with sequence ordering")
		}
	} else {
		logger.Error("Persistence manager not initialized, data will be lost")
		atomic.AddInt64(&ps.stream.droppedCount, 1)
	}

	// 启动异步恢复检查（防止重复启动）
	if atomic.LoadInt32(&ps.stream.activeRetries) < ps.stream.maxRetryRoutines {
		go ps.stream.checkAndProcessRecoveryData()
	}
}

// GetStrategyName 获取策略名称
func (ps *PersistenceStrategy) GetStrategyName() string {
	return StrategyPersist
}

// Init 初始化持久化策略
func (ps *PersistenceStrategy) Init(stream *Stream, config types.PerformanceConfig) error {
	ps.stream = stream
	return nil
}

// Stop 停止并清理持久化策略资源
func (ps *PersistenceStrategy) Stop() error {
	return nil
}

// DropStrategy 丢弃策略实现
type DropStrategy struct {
	stream *Stream
}

// NewDropStrategy 创建丢弃策略实例
func NewDropStrategy() *DropStrategy {
	return &DropStrategy{}
}

// ProcessData 实现丢弃模式数据处理
func (ds *DropStrategy) ProcessData(data map[string]interface{}) {
	// 智能非阻塞添加，分层背压控制
	if ds.stream.safeSendToDataChan(data) {
		return
	}

	// 数据通道已满，使用分层背压策略，获取通道状态
	ds.stream.dataChanMux.RLock()
	chanLen := len(ds.stream.dataChan)
	chanCap := cap(ds.stream.dataChan)
	currentDataChan := ds.stream.dataChan
	ds.stream.dataChanMux.RUnlock()

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
			atomic.AddInt64(&ds.stream.droppedCount, 1)
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
			atomic.AddInt64(&ds.stream.droppedCount, 1)
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
			atomic.AddInt64(&ds.stream.droppedCount, 1)
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
				atomic.AddInt64(&ds.stream.droppedCount, 1)
			}
		}
	}
}

// GetStrategyName 获取策略名称
func (ds *DropStrategy) GetStrategyName() string {
	return StrategyDrop
}

// Init 初始化丢弃策略
func (ds *DropStrategy) Init(stream *Stream, config types.PerformanceConfig) error {
	ds.stream = stream
	return nil
}

// Stop 停止并清理丢弃策略资源
func (ds *DropStrategy) Stop() error {
	return nil
}

// StrategyFactory 策略工厂
// 使用统一注册机制管理所有策略（内置和自定义）
type StrategyFactory struct {
	// 注册的策略映射
	strategies map[string]func() DataProcessingStrategy
	mutex      sync.RWMutex // 保护并发访问
}

// NewStrategyFactory 创建策略工厂实例
// 自动注册所有内置策略
func NewStrategyFactory() *StrategyFactory {
	factory := &StrategyFactory{
		strategies: make(map[string]func() DataProcessingStrategy),
	}

	// 注册内置策略
	factory.RegisterStrategy(StrategyBlock, func() DataProcessingStrategy { return NewBlockingStrategy() })
	factory.RegisterStrategy(StrategyExpand, func() DataProcessingStrategy { return NewExpansionStrategy() })
	factory.RegisterStrategy(StrategyPersist, func() DataProcessingStrategy { return NewPersistenceStrategy() })
	factory.RegisterStrategy(StrategyDrop, func() DataProcessingStrategy { return NewDropStrategy() })

	return factory
}

// RegisterStrategy 注册策略到工厂
// 参数:
//   - name: 策略名称
//   - constructor: 策略构造函数
func (sf *StrategyFactory) RegisterStrategy(name string, constructor func() DataProcessingStrategy) {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()
	sf.strategies[name] = constructor
}

// UnregisterStrategy 注销策略
// 参数:
//   - name: 策略名称
func (sf *StrategyFactory) UnregisterStrategy(name string) {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()
	delete(sf.strategies, name)
}

// GetRegisteredStrategies 获取所有已注册的策略名称
// 返回:
//   - []string: 策略名称列表
func (sf *StrategyFactory) GetRegisteredStrategies() []string {
	sf.mutex.RLock()
	defer sf.mutex.RUnlock()

	names := make([]string, 0, len(sf.strategies))
	for name := range sf.strategies {
		names = append(names, name)
	}
	return names
}

// CreateStrategy 根据策略名称创建对应的策略实例
// 参数:
//   - strategyName: 策略名称
//
// 返回:
//   - DataProcessingStrategy: 策略实例
//   - error: 错误信息
func (sf *StrategyFactory) CreateStrategy(strategyName string) (DataProcessingStrategy, error) {
	sf.mutex.RLock()
	constructor, exists := sf.strategies[strategyName]
	sf.mutex.RUnlock()

	if !exists {
		// 如果策略不存在，使用默认的丢弃策略
		sf.mutex.RLock()
		defaultConstructor, defaultExists := sf.strategies[StrategyDrop]
		sf.mutex.RUnlock()

		if defaultExists {
			return defaultConstructor(), nil
		}
		// 如果连默认策略都不存在，返回错误
		return nil, fmt.Errorf("strategy '%s' not found and no default strategy available", strategyName)
	}

	return constructor(), nil
}
