package stream

import (
	"fmt"
	"sync"

	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/window"
)

// StreamFactory Stream工厂，负责创建不同类型的Stream
type StreamFactory struct{}

// NewStreamFactory 创建Stream工厂
func NewStreamFactory() *StreamFactory {
	return &StreamFactory{}
}

// CreateStream 使用统一配置创建Stream
func (sf *StreamFactory) CreateStream(config types.Config) (*Stream, error) {
	// 如果没有指定性能配置，使用默认配置
	if (config.PerformanceConfig == types.PerformanceConfig{}) {
		config.PerformanceConfig = types.DefaultPerformanceConfig()
	}

	return sf.createStreamWithUnifiedConfig(config)
}

// CreateHighPerformanceStream 创建高性能Stream
func (sf *StreamFactory) CreateHighPerformanceStream(config types.Config) (*Stream, error) {
	config.PerformanceConfig = types.HighPerformanceConfig()
	return sf.createStreamWithUnifiedConfig(config)
}

// CreateLowLatencyStream 创建低延迟Stream
func (sf *StreamFactory) CreateLowLatencyStream(config types.Config) (*Stream, error) {
	config.PerformanceConfig = types.LowLatencyConfig()
	return sf.createStreamWithUnifiedConfig(config)
}

// CreateZeroDataLossStream 创建零数据丢失Stream
func (sf *StreamFactory) CreateZeroDataLossStream(config types.Config) (*Stream, error) {
	config.PerformanceConfig = types.ZeroDataLossConfig()
	return sf.createStreamWithUnifiedConfig(config)
}

// CreateCustomPerformanceStream 创建自定义性能配置的Stream
func (sf *StreamFactory) CreateCustomPerformanceStream(config types.Config, perfConfig types.PerformanceConfig) (*Stream, error) {
	config.PerformanceConfig = perfConfig
	return sf.createStreamWithUnifiedConfig(config)
}

// createStreamWithUnifiedConfig 使用统一配置创建Stream的内部实现
func (sf *StreamFactory) createStreamWithUnifiedConfig(config types.Config) (*Stream, error) {
	var win window.Window
	var err error

	// 只有在需要窗口时才创建窗口
	if config.NeedWindow {
		win, err = sf.createWindow(config)
		if err != nil {
			return nil, err
		}
	}

	// 创建Stream实例
	stream := sf.createStreamInstance(config, win)

	// 初始化持久化管理器
	if err := sf.initializePersistenceManager(stream, config.PerformanceConfig); err != nil {
		return nil, err
	}

	// 设置数据处理策略
	sf.setupDataProcessingStrategy(stream, config.PerformanceConfig)

	// 预编译字段处理信息
	stream.compileFieldProcessInfo()

	// 启动工作协程
	sf.startWorkerRoutines(stream, config.PerformanceConfig)

	return stream, nil
}

// createWindow 创建窗口
func (sf *StreamFactory) createWindow(config types.Config) (window.Window, error) {
	// 将统一的性能配置传递给窗口
	windowConfig := config.WindowConfig
	if windowConfig.Params == nil {
		windowConfig.Params = make(map[string]interface{})
	}
	// 传递完整的性能配置给窗口
	windowConfig.Params[PerformanceConfigKey] = config.PerformanceConfig

	return window.CreateWindow(windowConfig)
}

// createStreamInstance 创建Stream实例
func (sf *StreamFactory) createStreamInstance(config types.Config, win window.Window) *Stream {
	perfConfig := config.PerformanceConfig
	return &Stream{
		dataChan:         make(chan interface{}, perfConfig.BufferConfig.DataChannelSize),
		config:           config,
		Window:           win,
		resultChan:       make(chan interface{}, perfConfig.BufferConfig.ResultChannelSize),
		seenResults:      &sync.Map{},
		done:             make(chan struct{}),
		sinkWorkerPool:   make(chan func(), perfConfig.WorkerConfig.SinkPoolSize),
		allowDataDrop:    perfConfig.OverflowConfig.AllowDataLoss,
		blockingTimeout:  perfConfig.OverflowConfig.BlockTimeout,
		overflowStrategy: perfConfig.OverflowConfig.Strategy,
		maxRetryRoutines: int32(perfConfig.WorkerConfig.MaxRetryRoutines),
	}
}

// initializePersistenceManager 初始化持久化管理器
// 当溢出策略设置为持久化时，检查并初始化持久化配置
func (sf *StreamFactory) initializePersistenceManager(stream *Stream, perfConfig types.PerformanceConfig) error {
	if perfConfig.OverflowConfig.Strategy == StrategyPersist {
		if perfConfig.OverflowConfig.PersistenceConfig == nil {
			return fmt.Errorf("persistence strategy is enabled but PersistenceConfig is not provided. Please configure PersistenceConfig with DataDir, MaxFileSize, and FlushInterval. Example: perfConfig.OverflowConfig.PersistenceConfig = &types.PersistenceConfig{DataDir: \"./data\", MaxFileSize: 10*1024*1024, FlushInterval: 5*time.Second}")
		}
		persistConfig := perfConfig.OverflowConfig.PersistenceConfig
		stream.persistenceManager = NewPersistenceManagerWithConfig(
			persistConfig.DataDir,
			persistConfig.MaxFileSize,
			persistConfig.FlushInterval,
		)
		if err := stream.persistenceManager.Start(); err != nil {
			return fmt.Errorf("failed to start persistence manager: %w", err)
		}
	}
	return nil
}

// setupDataProcessingStrategy 设置数据处理策略
func (sf *StreamFactory) setupDataProcessingStrategy(stream *Stream, perfConfig types.PerformanceConfig) {
	// 根据溢出策略预设AddData函数指针，避免运行时switch判断
	switch perfConfig.OverflowConfig.Strategy {
	case StrategyBlock:
		stream.addDataFunc = stream.addDataBlocking
	case StrategyExpand:
		stream.addDataFunc = stream.addDataWithExpansion
	case StrategyPersist:
		stream.addDataFunc = stream.addDataWithPersistence
	default:
		stream.addDataFunc = stream.addDataWithDrop
	}
}

// startWorkerRoutines 启动工作协程
func (sf *StreamFactory) startWorkerRoutines(stream *Stream, perfConfig types.PerformanceConfig) {
	go stream.startSinkWorkerPool(perfConfig.WorkerConfig.SinkWorkerCount)
	go stream.startResultConsumer()
}
