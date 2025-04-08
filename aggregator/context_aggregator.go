package aggregator

type ContextAggregator interface {
	GetContextKey() string
}

type WindowStartAggregator struct {
	val interface{}
}

func (w *WindowStartAggregator) New() AggregatorFunction {
	return &WindowStartAggregator{}
}

func (w *WindowStartAggregator) Add(val interface{}) {
	w.val = val
}

func (w *WindowStartAggregator) Result() interface{} {
	return w.val
}

func (w *WindowStartAggregator) GetContextKey() string {
	return "window_start"
}

type WindowEndAggregator struct {
	val interface{}
}

func (w *WindowEndAggregator) New() AggregatorFunction {
	return &WindowEndAggregator{}
}

func (w *WindowEndAggregator) Add(val interface{}) {
	w.val = val
}

func (w *WindowEndAggregator) Result() interface{} {
	return w.val
}

func (w *WindowEndAggregator) GetContextKey() string {
	return "window_end"
}
