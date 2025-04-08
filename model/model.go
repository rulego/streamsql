package model

import (
	"time"

	"github.com/rulego/streamsql/aggregator"
)

type Config struct {
	WindowConfig WindowConfig
	GroupFields  []string
	SelectFields map[string]aggregator.AggregateType
	FieldAlias   map[string]string
}
type WindowConfig struct {
	Type     string
	Params   map[string]interface{}
	TsProp   string
	TimeUnit time.Duration
}
