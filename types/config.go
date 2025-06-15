package types

import (
	"time"

	"github.com/rulego/streamsql/aggregator"
)

// Config 流处理配置
type Config struct {
	// SQL 处理相关配置
	WindowConfig     WindowConfig                        `json:"windowConfig"`
	GroupFields      []string                            `json:"groupFields"`
	SelectFields     map[string]aggregator.AggregateType `json:"selectFields"`
	FieldAlias       map[string]string                   `json:"fieldAlias"`
	SimpleFields     []string                            `json:"simpleFields"`
	FieldExpressions map[string]FieldExpression          `json:"fieldExpressions"`
	Where            string                              `json:"where"`
	Having           string                              `json:"having"`

	// 功能开关
	NeedWindow bool `json:"needWindow"`
	Distinct   bool `json:"distinct"`

	// 结果控制
	Limit       int          `json:"limit"`
	Projections []Projection `json:"projections"`

	// 性能配置
	PerformanceConfig PerformanceConfig `json:"performanceConfig"`
}

// WindowConfig 窗口配置
type WindowConfig struct {
	Type       string                 `json:"type"`
	Params     map[string]interface{} `json:"params"`
	TsProp     string                 `json:"tsProp"`
	TimeUnit   time.Duration          `json:"timeUnit"`
	GroupByKey string                 `json:"groupByKey"` // 会话窗口分组键
}

// FieldExpression 字段表达式配置
type FieldExpression struct {
	Field      string   `json:"field"`      // 原始字段名
	Expression string   `json:"expression"` // 完整表达式
	Fields     []string `json:"fields"`     // 表达式中引用的所有字段
}

// ProjectionSourceType 投影来源类型
type ProjectionSourceType int

const (
	SourceGroupKey ProjectionSourceType = iota
	SourceAggregateResult
	SourceWindowProperty // For window_start, window_end
)

// Projection SELECT列表中的投影配置
type Projection struct {
	OutputName string               `json:"outputName"` // 输出字段名
	SourceType ProjectionSourceType `json:"sourceType"` // 数据来源类型
	InputName  string               `json:"inputName"`  // 输入字段名
}

// PerformanceConfig 性能配置
type PerformanceConfig struct {
	BufferConfig     BufferConfig     `json:"bufferConfig"`     // 缓冲区配置
	OverflowConfig   OverflowConfig   `json:"overflowConfig"`   // 溢出策略配置
	WorkerConfig     WorkerConfig     `json:"workerConfig"`     // 工作池配置
	MonitoringConfig MonitoringConfig `json:"monitoringConfig"` // 监控配置
}

// BufferConfig 缓冲区配置
type BufferConfig struct {
	DataChannelSize     int     `json:"dataChannelSize"`     // 数据输入缓冲区大小
	ResultChannelSize   int     `json:"resultChannelSize"`   // 结果输出缓冲区大小
	WindowOutputSize    int     `json:"windowOutputSize"`    // 窗口输出缓冲区大小
	EnableDynamicResize bool    `json:"enableDynamicResize"` // 是否启用动态缓冲区调整
	MaxBufferSize       int     `json:"maxBufferSize"`       // 最大缓冲区大小
	UsageThreshold      float64 `json:"usageThreshold"`      // 缓冲区使用率阈值
}

// OverflowConfig 溢出策略配置
type OverflowConfig struct {
	Strategy          string             `json:"strategy"`          // 溢出策略: "drop", "block", "expand", "persist"
	BlockTimeout      time.Duration      `json:"blockTimeout"`      // 阻塞超时时间
	AllowDataLoss     bool               `json:"allowDataLoss"`     // 是否允许数据丢失
	PersistenceConfig *PersistenceConfig `json:"persistenceConfig"` // 持久化配置
	ExpansionConfig   ExpansionConfig    `json:"expansionConfig"`   // 扩容配置
}

// PersistenceConfig 持久化配置
type PersistenceConfig struct {
	DataDir       string        `json:"dataDir"`       // 持久化数据目录
	MaxFileSize   int64         `json:"maxFileSize"`   // 最大文件大小
	FlushInterval time.Duration `json:"flushInterval"` // 刷新间隔
	MaxRetries    int           `json:"maxRetries"`    // 最大重试次数
	RetryInterval time.Duration `json:"retryInterval"` // 重试间隔
}

// ExpansionConfig 扩容配置
type ExpansionConfig struct {
	GrowthFactor     float64       `json:"growthFactor"`     // 扩容因子
	MinIncrement     int           `json:"minIncrement"`     // 最小扩容增量
	TriggerThreshold float64       `json:"triggerThreshold"` // 扩容触发阈值
	ExpansionTimeout time.Duration `json:"expansionTimeout"` // 扩容超时时间
}

// WorkerConfig 工作池配置
type WorkerConfig struct {
	SinkPoolSize     int `json:"sinkPoolSize"`     // Sink工作池大小
	SinkWorkerCount  int `json:"sinkWorkerCount"`  // Sink工作线程数
	MaxRetryRoutines int `json:"maxRetryRoutines"` // 最大重试协程数
}

// MonitoringConfig 监控配置
type MonitoringConfig struct {
	EnableMonitoring    bool              `json:"enableMonitoring"`    // 是否启用性能监控
	StatsUpdateInterval time.Duration     `json:"statsUpdateInterval"` // 统计信息更新间隔
	EnableDetailedStats bool              `json:"enableDetailedStats"` // 是否启用详细统计
	WarningThresholds   WarningThresholds `json:"warningThresholds"`   // 性能警告阈值
}

// WarningThresholds 性能警告阈值
type WarningThresholds struct {
	DropRateWarning     float64 `json:"dropRateWarning"`     // 丢弃率警告阈值
	DropRateCritical    float64 `json:"dropRateCritical"`    // 丢弃率严重阈值
	BufferUsageWarning  float64 `json:"bufferUsageWarning"`  // 缓冲区使用率警告阈值
	BufferUsageCritical float64 `json:"bufferUsageCritical"` // 缓冲区使用率严重阈值
}

// NewConfig 创建默认配置
func NewConfig() Config {
	return Config{
		PerformanceConfig: DefaultPerformanceConfig(),
	}
}

// NewConfigWithPerformance 创建带性能配置的Config
func NewConfigWithPerformance(perfConfig PerformanceConfig) Config {
	return Config{
		PerformanceConfig: perfConfig,
	}
}

// DefaultPerformanceConfig 默认性能配置
func DefaultPerformanceConfig() PerformanceConfig {
	return PerformanceConfig{
		BufferConfig: BufferConfig{
			DataChannelSize:     10000,
			ResultChannelSize:   10000,
			WindowOutputSize:    1000,
			EnableDynamicResize: true,
			MaxBufferSize:       100000,
			UsageThreshold:      0.8,
		},
		OverflowConfig: OverflowConfig{
			Strategy:      "expand",
			BlockTimeout:  30 * time.Second,
			AllowDataLoss: false,
			ExpansionConfig: ExpansionConfig{
				GrowthFactor:     1.5,
				MinIncrement:     1000,
				TriggerThreshold: 0.9,
				ExpansionTimeout: 5 * time.Second,
			},
		},
		WorkerConfig: WorkerConfig{
			SinkPoolSize:     500,
			SinkWorkerCount:  8,
			MaxRetryRoutines: 5,
		},
		MonitoringConfig: MonitoringConfig{
			EnableMonitoring:    true,
			StatsUpdateInterval: 1 * time.Second,
			EnableDetailedStats: false,
			WarningThresholds: WarningThresholds{
				DropRateWarning:     10.0,
				DropRateCritical:    25.0,
				BufferUsageWarning:  80.0,
				BufferUsageCritical: 95.0,
			},
		},
	}
}

// HighPerformanceConfig 高性能配置预设
func HighPerformanceConfig() PerformanceConfig {
	config := DefaultPerformanceConfig()
	config.BufferConfig.DataChannelSize = 50000
	config.BufferConfig.ResultChannelSize = 50000
	config.BufferConfig.WindowOutputSize = 5000
	config.BufferConfig.MaxBufferSize = 500000
	config.WorkerConfig.SinkPoolSize = 1000
	config.WorkerConfig.SinkWorkerCount = 16
	return config
}

// LowLatencyConfig 低延迟配置预设
func LowLatencyConfig() PerformanceConfig {
	config := DefaultPerformanceConfig()
	config.BufferConfig.DataChannelSize = 1000
	config.BufferConfig.ResultChannelSize = 1000
	config.BufferConfig.WindowOutputSize = 100
	config.BufferConfig.UsageThreshold = 0.7
	config.OverflowConfig.Strategy = "drop"
	config.OverflowConfig.AllowDataLoss = true
	return config
}

// ZeroDataLossConfig 零数据丢失配置预设
func ZeroDataLossConfig() PerformanceConfig {
	config := DefaultPerformanceConfig()
	config.BufferConfig.DataChannelSize = 20000
	config.BufferConfig.ResultChannelSize = 20000
	config.BufferConfig.WindowOutputSize = 2000
	config.BufferConfig.EnableDynamicResize = true
	config.OverflowConfig.Strategy = "block"
	config.OverflowConfig.AllowDataLoss = false
	config.OverflowConfig.BlockTimeout = 0 // 无超时，永久阻塞
	return config
}

// PersistencePerformanceConfig 持久化配置预设
func PersistencePerformanceConfig() PerformanceConfig {
	config := DefaultPerformanceConfig()
	config.OverflowConfig.Strategy = "persist"
	config.OverflowConfig.PersistenceConfig = &PersistenceConfig{
		DataDir:       "./streamsql_data",
		MaxFileSize:   10 * 1024 * 1024, // 10MB
		FlushInterval: 5 * time.Second,
		MaxRetries:    3,
		RetryInterval: 2 * time.Second,
	}
	return config
}
