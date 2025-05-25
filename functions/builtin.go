package functions

// registerBuiltinFunctions registers all built-in functions.
// The actual function implementations are now split into separate files
// (functions_math.go, functions_string.go, etc.) within this package.
func registerBuiltinFunctions() {
	// Math functions
	_ = Register(NewAbsFunction())
	_ = Register(NewSqrtFunction())
	_ = Register(NewAcosFunction())
	_ = Register(NewAsinFunction())
	_ = Register(NewAtanFunction())
	_ = Register(NewAtan2Function())
	_ = Register(NewBitAndFunction())
	_ = Register(NewBitOrFunction())
	_ = Register(NewBitXorFunction())
	_ = Register(NewBitNotFunction())
	_ = Register(NewCeilingFunction())
	_ = Register(NewCosFunction())
	_ = Register(NewCoshFunction())
	_ = Register(NewExpFunction())
	_ = Register(NewFloorFunction())
	_ = Register(NewLnFunction())
	_ = Register(NewPowerFunction())

	// String functions
	_ = Register(NewConcatFunction())
	_ = Register(NewLengthFunction())
	_ = Register(NewUpperFunction())
	_ = Register(NewLowerFunction())
	_ = Register(NewTrimFunction())
	_ = Register(NewFormatFunction())

	// Conversion functions
	_ = Register(NewCastFunction())
	_ = Register(NewHex2DecFunction())
	_ = Register(NewDec2HexFunction())
	_ = Register(NewEncodeFunction())
	_ = Register(NewDecodeFunction())

	// Time-Date functions
	_ = Register(NewNowFunction())
	_ = Register(NewCurrentTimeFunction())
	_ = Register(NewCurrentDateFunction())

	// Aggregation functions
	_ = Register(NewSumFunction())
	_ = Register(NewAvgFunction())
	_ = Register(NewMinFunction())
	_ = Register(NewMaxFunction())
	_ = Register(NewCountFunction())
	_ = Register(NewStdDevFunction())
	_ = Register(NewMedianFunction())
	_ = Register(NewPercentileFunction())
	_ = Register(NewCollectFunction())
	_ = Register(NewLastValueFunction())
	_ = Register(NewMergeAggFunction())
	_ = Register(NewStdDevSFunction())
	_ = Register(NewDeduplicateFunction())
	_ = Register(NewVarFunction())
	_ = Register(NewVarSFunction())

	// Window functions
	_ = Register(NewRowNumberFunction())

	// Analytical functions
	_ = Register(NewLagFunction())
	_ = Register(NewLatestFunction())
	_ = Register(NewChangedColFunction())
	_ = Register(NewHadChangedFunction())

	// 注册窗口函数
	_ = Register(NewWindowStartFunction())
	_ = Register(NewWindowEndFunction())

	//  表达式函数
	_ = Register(NewExpressionFunction())

	// User-defined functions (placeholder for future extension)
	// Example: _=Register(NewMyUserDefinedFunction())
}
