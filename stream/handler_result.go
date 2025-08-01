package stream

import (
	"sync/atomic"

	"github.com/rulego/streamsql/logger"
)

// ResultHandler 结果处理器，负责处理结果输出和Sink调用
type ResultHandler struct {
	stream *Stream
}

// NewResultHandler 创建结果处理器
func NewResultHandler(stream *Stream) *ResultHandler {
	return &ResultHandler{stream: stream}
}

// startSinkWorkerPool 启动Sink工作池，支持配置工作线程数
func (s *Stream) startSinkWorkerPool(workerCount int) {
	// 使用配置的工作线程数
	if workerCount <= 0 {
		workerCount = 8 // 默认值
	}

	for i := 0; i < workerCount; i++ {
		go func(workerID int) {
			for {
				select {
				case task := <-s.sinkWorkerPool:
					// 执行sink任务
					func() {
						defer func() {
							// 增强错误恢复，防止单个worker崩溃
							if r := recover(); r != nil {
								logger.Error("Sink worker %d panic recovered: %v", workerID, r)
							}
						}()
						task()
					}()
				case <-s.done:
					return
				}
			}
		}(i)
	}
}

// startResultConsumer 启动自动结果消费者，防止resultChan阻塞
func (s *Stream) startResultConsumer() {
	for {
		select {
		case <-s.resultChan:
			// 自动消费结果，防止通道阻塞
			// 这是一个保底机制，确保即使没有外部消费者，系统也不会阻塞
		case <-s.done:
			return
		}
	}
}

// sendResultNonBlocking 非阻塞方式发送结果到resultChan (智能背压控制)
func (s *Stream) sendResultNonBlocking(results []map[string]interface{}) {
	select {
	case s.resultChan <- results:
		// 成功发送到结果通道
		atomic.AddInt64(&s.outputCount, 1)
	default:
		// 结果通道已满，使用智能背压控制策略
		s.handleResultChannelBackpressure(results)
	}
}

// handleResultChannelBackpressure 处理结果通道背压
func (s *Stream) handleResultChannelBackpressure(results []map[string]interface{}) {
	chanLen := len(s.resultChan)
	chanCap := cap(s.resultChan)

	// 如果通道使用率超过90%，进入背压模式
	if float64(chanLen)/float64(chanCap) > 0.9 {
		// 尝试清理一些旧数据，为新数据腾出空间
		select {
		case <-s.resultChan:
			// 清理一个旧结果，然后尝试添加新结果
			select {
			case s.resultChan <- results:
				atomic.AddInt64(&s.outputCount, 1)
			default:
				logger.Warn("Result channel is full, dropping result data")
				atomic.AddInt64(&s.droppedCount, 1)
			}
		default:
			logger.Warn("Result channel is full, dropping result data")
			atomic.AddInt64(&s.droppedCount, 1)
		}
	} else {
		logger.Warn("Result channel is full, dropping result data")
		atomic.AddInt64(&s.droppedCount, 1)
	}
}

// callSinksAsync 异步调用所有sink函数
func (s *Stream) callSinksAsync(results []map[string]interface{}) {
	// 使用读锁安全地访问sinks切片
	s.sinksMux.RLock()
	defer s.sinksMux.RUnlock()

	if len(s.sinks) == 0 {
		return
	}

	// 直接遍历sinks切片，避免复制开销
	// 由于submitSinkTask是异步的，不会长时间持有锁
	for _, sink := range s.sinks {
		s.submitSinkTask(sink, results)
	}
}

// submitSinkTask 提交Sink任务
func (s *Stream) submitSinkTask(sink func(interface{}), results []map[string]interface{}) {
	// 捕获sink变量，避免闭包问题
	currentSink := sink

	// 提交任务到工作池
	task := func() {
		defer func() {
			// 恢复panic，防止单个sink错误影响整个系统
			if r := recover(); r != nil {
				logger.Error("Sink execution exception: %v", r)
			}
		}()
		currentSink(results)
	}

	// 非阻塞提交任务
	select {
	case s.sinkWorkerPool <- task:
		// 成功提交任务
	default:
		// 工作池已满，直接在当前goroutine执行（降级处理）
		go task()
	}
}

// AddSink 添加Sink函数
func (s *Stream) AddSink(sink func(interface{})) {
	s.sinksMux.Lock()
	defer s.sinksMux.Unlock()
	s.sinks = append(s.sinks, sink)
}

// GetResultsChan 获取结果通道
func (s *Stream) GetResultsChan() <-chan interface{} {
	return s.resultChan
}
