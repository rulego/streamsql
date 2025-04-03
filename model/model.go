package model

import (
	"time"

	aggregator2 "github.com/rulego/streamsql/aggregator"
)

type Config struct {
	WindowConfig WindowConfig
	GroupFields  []string
	SelectFields map[string]aggregator2.AggregateType
}
type WindowConfig struct {
	Type     string
	Params   map[string]interface{}
	TsProp   string
	TimeUnit time.Duration
}
