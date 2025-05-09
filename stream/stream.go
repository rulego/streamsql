package stream

import (
	"fmt"
	"strings"

	aggregator2 "github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/model"
	"github.com/rulego/streamsql/parser"
	"github.com/rulego/streamsql/window"
)

type Stream struct {
	dataChan   chan interface{}
	filter     parser.Condition
	Window     window.Window
	aggregator aggregator2.Aggregator
	config     model.Config
	sinks      []func(interface{})
	resultChan chan interface{} // 结果通道
}

func NewStream(config model.Config) (*Stream, error) {
	win, err := window.CreateWindow(config.WindowConfig)
	if err != nil {
		return nil, err
	}
	return &Stream{
		dataChan:   make(chan interface{}, 1000),
		config:     config,
		Window:     win,
		resultChan: make(chan interface{}, 10),
	}, nil
}

func (s *Stream) RegisterFilter(condition string) error {
	if strings.TrimSpace(condition) == "" {
		return nil
	}
	filter, err := parser.NewExprCondition(condition)
	if err != nil {
		return fmt.Errorf("compile filter error: %w", err)
	}
	s.filter = filter
	return nil
}

func (s *Stream) Start() {
	go s.process()
}

func (s *Stream) process() {
	s.aggregator = aggregator2.NewGroupAggregator(s.config.GroupFields, s.config.SelectFields, s.config.FieldAlias)

	// 启动窗口处理协程
	s.Window.Start()

	for {
		select {
		case data := <-s.dataChan:
			if s.filter == nil || s.filter.Evaluate(data) {
				s.Window.Add(data)
				// fmt.Printf("add data to win : %v \n", data)
			}
		case batch := <-s.Window.OutputChan():
			// 处理窗口批数据
			for _, item := range batch {
				s.aggregator.Put("window_start", item.Slot.WindowStart())
				s.aggregator.Put("window_end", item.Slot.WindowEnd())
				if err := s.aggregator.Add(item.Data); err != nil {
					fmt.Printf("aggregate error: %v\n", err)
				}
			}

			// 获取并发送聚合结果
			if results, err := s.aggregator.GetResults(); err == nil {
				// 发送结果到结果通道和 Sink 函数
				s.resultChan <- results
				for _, sink := range s.sinks {
					sink(results)
				}
				s.aggregator.Reset()
			}
		}
	}
}

func (s *Stream) AddData(data interface{}) {
	s.dataChan <- data
}

func (s *Stream) AddSink(sink func(interface{})) {
	s.sinks = append(s.sinks, sink)
}

func (s *Stream) GetResultsChan() <-chan interface{} {
	return s.resultChan
}

func NewStreamProcessor() (*Stream, error) {
	return NewStream(model.Config{})
}
