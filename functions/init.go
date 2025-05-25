package functions

// 初始化所有内置函数
func init() {
	registerBuiltinFunctions()
	//// 注册聚合函数 - 只注册增量计算版本（实现了AggregatorFunction接口）
	//Register(NewSumFunction())
	//Register(NewAvgFunction())
	//Register(NewMinFunction())
	//Register(NewMaxFunction())
	//Register(NewCountFunction())
	//Register(NewStdDevAggregatorFunction())
	//Register(NewMedianAggregatorFunction())
	//Register(NewPercentileAggregatorFunction())
	//Register(NewCollectAggregatorFunction())
	//Register(NewLastValueAggregatorFunction())
	//Register(NewMergeAggAggregatorFunction())
	//Register(NewStdDevSAggregatorFunction())
	//Register(NewDeduplicateAggregatorFunction())
	//Register(NewVarAggregatorFunction())
	//Register(NewVarSAggregatorFunction())
	//
	//// 注册分析函数
	//Register(NewLagFunction())
	//Register(NewLatestFunction())
	//Register(NewChangedColFunction())
	//Register(NewHadChangedFunction())
	//
	//// 注册窗口函数
	//Register(NewWindowStartFunction())
	//Register(NewWindowEndFunction())
	//Register(NewExpressionFunction())
	//
	//// 注册适配器 - 使用增量计算版本
	//RegisterAggregatorAdapter(SumStr)
	//RegisterAggregatorAdapter(AvgStr)
	//RegisterAggregatorAdapter(MinStr)
	//RegisterAggregatorAdapter(MaxStr)
	//RegisterAggregatorAdapter(CountStr)
	//RegisterAggregatorAdapter(StdDevStr)
	//RegisterAggregatorAdapter(MedianStr)
	//RegisterAggregatorAdapter(PercentileStr)
	//RegisterAggregatorAdapter(CollectStr)
	//RegisterAggregatorAdapter(LastValueStr)
	//RegisterAggregatorAdapter(MergeAggStr)
	//RegisterAggregatorAdapter(StdDevSStr)
	//RegisterAggregatorAdapter(DeduplicateStr)
	//RegisterAggregatorAdapter(VarStr)
	//RegisterAggregatorAdapter(VarSStr)
	//RegisterAggregatorAdapter(WindowStartStr)
	//RegisterAggregatorAdapter(WindowEndStr)
	//RegisterAggregatorAdapter(ExpressionStr)
	//
	//RegisterAnalyticalAdapter(LagStr)
	//RegisterAnalyticalAdapter(LatestStr)
	//RegisterAnalyticalAdapter(ChangedColStr)
	//RegisterAnalyticalAdapter(HadChangedStr)
}
