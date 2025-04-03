package window

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rulego/streamsql/model"
	"github.com/spf13/cast"
)

var _ Window = (*CountingWindow)(nil)

type CountingWindow struct {
	config      model.WindowConfig
	threshold   int
	count       int
	mu          sync.Mutex
	callback    func([]interface{})
	dataBuffer  []interface{}
	outputChan  chan []interface{}
	ctx         context.Context
	cancelFunc  context.CancelFunc
	ticker      *time.Ticker
	triggerChan chan struct{}
}

func NewCountingWindow(config model.WindowConfig) (*CountingWindow, error) {
	ctx, cancel := context.WithCancel(context.Background())
	threshold := cast.ToInt(config.Params["count"])
	if threshold <= 0 {
		return nil, fmt.Errorf("threshold must be a positive integer")
	}

	cw := &CountingWindow{
		threshold:   threshold,
		dataBuffer:  make([]interface{}, 0, threshold),
		outputChan:  make(chan []interface{}, 10),
		ctx:         ctx,
		cancelFunc:  cancel,
		triggerChan: make(chan struct{}, 1),
	}

	if callback, ok := config.Params["callback"].(func([]interface{})); ok {
		cw.SetCallback(callback)
	}
	return cw, nil
}

func (cw *CountingWindow) Add(data interface{}) {
	cw.mu.Lock()
	cw.dataBuffer = append(cw.dataBuffer, data)
	cw.count++
	shouldTrigger := cw.count >= cw.threshold
	cw.mu.Unlock()

	if shouldTrigger {
		cw.mu.Lock()
		v := append([]interface{}{}, cw.dataBuffer...)
		cw.mu.Unlock()

		go func() {
			if cw.callback != nil {
				cw.callback(v)
			}
			cw.outputChan <- v
			cw.Reset()
		}()
	}
}
func (cw *CountingWindow) Start() {
	go func() {
		cw.ticker = time.NewTicker(1 * time.Second)
		defer func() {
			cw.ticker.Stop()
			cw.cancelFunc()
		}()

		for {
			select {
			case <-cw.ticker.C:
				cw.Trigger()
			case <-cw.ctx.Done():
				return
			}
		}
	}()
}

func (cw *CountingWindow) Trigger() {
	cw.triggerChan <- struct{}{}

	go func() {
		cw.mu.Lock()
		defer cw.mu.Unlock()

		if cw.callback != nil && len(cw.dataBuffer) > 0 {
			cw.callback(cw.dataBuffer)
		}
		cw.Reset()
	}()
}

func (cw *CountingWindow) Reset() {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.count = 0
	cw.dataBuffer = cw.dataBuffer[:0]
}

func (cw *CountingWindow) OutputChan() <-chan []interface{} {
	return cw.outputChan
}
func (cw *CountingWindow) GetResults() []interface{} {
	return append([]interface{}{}, cw.dataBuffer...)
}
