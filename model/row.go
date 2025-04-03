package model

import (
	"time"
)

type RowEvent interface {
	GetTimestamp() time.Time
}

type Row struct {
	Timestamp time.Time
	Data      interface{}
}

// GetTimestamp 获取时间戳
func (r *Row) GetTimestamp() time.Time {
	return r.Timestamp
}
