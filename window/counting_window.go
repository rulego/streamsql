package window

import (
	"context"
	"sync"
	"time"
)

var _ Window = (*CountingWindow)(nil)

type CountingWindow struct {
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

func NewCountingWindow(threshold int, callback func([]interface{})) *CountingWindow {
	ctx, cancel := context.WithCancel(context.Background())
	return &CountingWindow{
		threshold:   threshold,
		dataBuffer:  make([]interface{}, 0, threshold),
		outputChan:  make(chan []interface{}, 10),
		ctx:         ctx,
		cancelFunc:  cancel,
		callback:    callback,
		triggerChan: make(chan struct{}, 1),
	}
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
