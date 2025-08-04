package types

import (
	"time"

	"github.com/rulego/streamsql/aggregator"
)

// Config stream processing configuration
type Config struct {
	// SQL processing related configuration
	WindowConfig     WindowConfig                        `json:"windowConfig"`
	GroupFields      []string                            `json:"groupFields"`
	SelectFields     map[string]aggregator.AggregateType `json:"selectFields"`
	FieldAlias       map[string]string                   `json:"fieldAlias"`
	SimpleFields     []string                            `json:"simpleFields"`
	FieldExpressions map[string]FieldExpression          `json:"fieldExpressions"`
	FieldOrder       []string                            `json:"fieldOrder"`       // Original order of fields in SELECT statement
	Where            string                              `json:"where"`
	Having           string                              `json:"having"` 

	// Feature switches
	NeedWindow bool `json:"needWindow"`
	Distinct   bool `json:"distinct"`

	// Result control
	Limit       int          `json:"limit"`
	Projections []Projection `json:"projections"`

	// Performance configuration
	PerformanceConfig PerformanceConfig `json:"performanceConfig"`
}

// WindowConfig window configuration
type WindowConfig struct {
	Type       string                 `json:"type"`
	Params     map[string]interface{} `json:"params"`
	TsProp     string                 `json:"tsProp"`
	TimeUnit   time.Duration          `json:"timeUnit"`
	GroupByKey string                 `json:"groupByKey"` // Session window grouping key
}

// FieldExpression field expression configuration
type FieldExpression struct {
	Field      string   `json:"field"`      // original field name
	Expression string   `json:"expression"` // complete expression
	Fields     []string `json:"fields"`     // all fields referenced in expression
}

// ProjectionSourceType projection source type
type ProjectionSourceType int

const (
	SourceGroupKey ProjectionSourceType = iota
	SourceAggregateResult
	SourceWindowProperty // For window_start, window_end
)

// Projection projection configuration in SELECT list
type Projection struct {
	OutputName string               `json:"outputName"` // output field name
	SourceType ProjectionSourceType `json:"sourceType"` // data source type
	InputName  string               `json:"inputName"`  // input field name
}

// PerformanceConfig performance configuration
type PerformanceConfig struct {
	BufferConfig     BufferConfig     `json:"bufferConfig"`     // buffer configuration
	OverflowConfig   OverflowConfig   `json:"overflowConfig"`   // overflow strategy configuration
	WorkerConfig     WorkerConfig     `json:"workerConfig"`     // worker pool configuration
	MonitoringConfig MonitoringConfig `json:"monitoringConfig"` // monitoring configuration
}

// BufferConfig buffer configuration
type BufferConfig struct {
	DataChannelSize     int     `json:"dataChannelSize"`     // Data input buffer size
	ResultChannelSize   int     `json:"resultChannelSize"`   // Result output buffer size
	WindowOutputSize    int     `json:"windowOutputSize"`    // Window output buffer size
	EnableDynamicResize bool    `json:"enableDynamicResize"` // Enable dynamic buffer resizing
	MaxBufferSize       int     `json:"maxBufferSize"`       // Maximum buffer size
	UsageThreshold      float64 `json:"usageThreshold"`      // Buffer usage threshold
}

// OverflowConfig overflow strategy configuration
type OverflowConfig struct {
	Strategy          string             `json:"strategy"`          // Overflow strategy: "drop", "block", "expand", "persist"
	BlockTimeout      time.Duration      `json:"blockTimeout"`      // Block timeout duration
	AllowDataLoss     bool               `json:"allowDataLoss"`     // Allow data loss
	PersistenceConfig *PersistenceConfig `json:"persistenceConfig"` // Persistence configuration
	ExpansionConfig   ExpansionConfig    `json:"expansionConfig"`   // Expansion configuration
}

// PersistenceConfig persistence configuration
type PersistenceConfig struct {
	DataDir       string        `json:"dataDir"`       // Persistence data directory
	MaxFileSize   int64         `json:"maxFileSize"`   // Maximum file size
	FlushInterval time.Duration `json:"flushInterval"` // Flush interval
	MaxRetries    int           `json:"maxRetries"`    // Maximum retry count
	RetryInterval time.Duration `json:"retryInterval"` // Retry interval
}

// ExpansionConfig expansion configuration
type ExpansionConfig struct {
	GrowthFactor     float64       `json:"growthFactor"`     // Growth factor
	MinIncrement     int           `json:"minIncrement"`     // Minimum expansion increment
	TriggerThreshold float64       `json:"triggerThreshold"` // Expansion trigger threshold
	ExpansionTimeout time.Duration `json:"expansionTimeout"` // Expansion timeout duration
}

// WorkerConfig worker pool configuration
type WorkerConfig struct {
	SinkPoolSize     int `json:"sinkPoolSize"`     // Sink pool size
	SinkWorkerCount  int `json:"sinkWorkerCount"`  // Sink worker count
	MaxRetryRoutines int `json:"maxRetryRoutines"` // Maximum retry routines
}

// MonitoringConfig monitoring configuration
type MonitoringConfig struct {
	EnableMonitoring    bool              `json:"enableMonitoring"`    // Enable performance monitoring
	StatsUpdateInterval time.Duration     `json:"statsUpdateInterval"` // Statistics update interval
	EnableDetailedStats bool              `json:"enableDetailedStats"` // Enable detailed statistics
	WarningThresholds   WarningThresholds `json:"warningThresholds"`   // Performance warning thresholds
}

// WarningThresholds performance warning thresholds
type WarningThresholds struct {
	DropRateWarning     float64 `json:"dropRateWarning"`     // Drop rate warning threshold
	DropRateCritical    float64 `json:"dropRateCritical"`    // Drop rate critical threshold
	BufferUsageWarning  float64 `json:"bufferUsageWarning"`  // Buffer usage warning threshold
	BufferUsageCritical float64 `json:"bufferUsageCritical"` // Buffer usage critical threshold
}

// NewConfig creates default configuration
func NewConfig() Config {
	return Config{
		PerformanceConfig: DefaultPerformanceConfig(),
	}
}

// NewConfigWithPerformance creates Config with performance configuration
func NewConfigWithPerformance(perfConfig PerformanceConfig) Config {
	return Config{
		PerformanceConfig: perfConfig,
	}
}

// DefaultPerformanceConfig default performance configuration
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

// HighPerformanceConfig high performance configuration preset
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

// LowLatencyConfig low latency configuration preset
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

// ZeroDataLossConfig zero data loss configuration preset
func ZeroDataLossConfig() PerformanceConfig {
	config := DefaultPerformanceConfig()
	config.BufferConfig.DataChannelSize = 20000
	config.BufferConfig.ResultChannelSize = 20000
	config.BufferConfig.WindowOutputSize = 2000
	config.BufferConfig.EnableDynamicResize = true
	config.OverflowConfig.Strategy = "block"
	config.OverflowConfig.AllowDataLoss = false
	config.OverflowConfig.BlockTimeout = 0 // no timeout, permanent blocking
	return config
}

// PersistencePerformanceConfig persistence configuration preset
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
