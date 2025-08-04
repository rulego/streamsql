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
	_ = Register(NewLogFunction())
	_ = Register(NewLog10Function())
	_ = Register(NewLog2Function())
	_ = Register(NewModFunction())
	_ = Register(NewRandFunction())
	_ = Register(NewRoundFunction())
	_ = Register(NewSignFunction())
	_ = Register(NewSinFunction())
	_ = Register(NewSinhFunction())
	_ = Register(NewTanFunction())
	_ = Register(NewTanhFunction())
	_ = Register(NewPowerFunction())

	// String functions
	_ = Register(NewConcatFunction())
	_ = Register(NewLengthFunction())
	_ = Register(NewUpperFunction())
	_ = Register(NewLowerFunction())
	_ = Register(NewTrimFunction())
	_ = Register(NewFormatFunction())
	_ = Register(NewEndswithFunction())
	_ = Register(NewStartswithFunction())
	_ = Register(NewIndexofFunction())
	_ = Register(NewSubstringFunction())
	_ = Register(NewReplaceFunction())
	_ = Register(NewSplitFunction())
	_ = Register(NewLpadFunction())
	_ = Register(NewRpadFunction())
	_ = Register(NewLtrimFunction())
	_ = Register(NewRtrimFunction())
	_ = Register(NewRegexpMatchesFunction())
	_ = Register(NewRegexpReplaceFunction())
	_ = Register(NewRegexpSubstringFunction())

	// Conversion functions
	_ = Register(NewCastFunction())
	_ = Register(NewHex2DecFunction())
	_ = Register(NewDec2HexFunction())
	_ = Register(NewEncodeFunction())
	_ = Register(NewDecodeFunction())
	_ = Register(NewConvertTzFunction())
	_ = Register(NewToSecondsFunction())
	_ = Register(NewChrFunction())
	_ = Register(NewTruncFunction())
	_ = Register(NewUrlEncodeFunction())
	_ = Register(NewUrlDecodeFunction())

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
	_ = Register(NewStdDevAggregatorFunction())
	_ = Register(NewMedianAggregatorFunction())
	_ = Register(NewPercentileFunction())
	_ = Register(NewCollectFunction())
	_ = Register(NewLastValueFunction())
	_ = Register(NewMergeAggFunction())
	_ = Register(NewStdDevSAggregatorFunction())
	_ = Register(NewDeduplicateAggregatorFunction())
	_ = Register(NewVarAggregatorFunction())
	_ = Register(NewVarSAggregatorFunction())

	// Window functions
	_ = Register(NewRowNumberFunction())
	_ = Register(NewFirstValueFunction())
	_ = Register(NewLeadFunction())
	_ = Register(NewNthValueFunction())

	// Analytical functions
	_ = Register(NewLagFunction())
	_ = Register(NewLatestFunction())
	_ = Register(NewChangedColFunction())
	_ = Register(NewHadChangedFunction())

	// Window functions
	_ = Register(NewWindowStartFunction())
	_ = Register(NewWindowEndFunction())

	// Expression functions
	_ = Register(NewExpressionFunction())
	_ = Register(NewExprFunction())
	_ = Register(NewExpressionAggregatorFunction())

	// JSON functions
	_ = Register(NewToJsonFunction())
	_ = Register(NewFromJsonFunction())
	_ = Register(NewJsonExtractFunction())
	_ = Register(NewJsonValidFunction())
	_ = Register(NewJsonTypeFunction())
	_ = Register(NewJsonLengthFunction())

	// Hash functions
	_ = Register(NewMd5Function())
	_ = Register(NewSha1Function())
	_ = Register(NewSha256Function())
	_ = Register(NewSha512Function())

	// Array functions
	_ = Register(NewArrayLengthFunction())
	_ = Register(NewArrayContainsFunction())
	_ = Register(NewArrayPositionFunction())
	_ = Register(NewArrayRemoveFunction())
	_ = Register(NewArrayDistinctFunction())
	_ = Register(NewArrayIntersectFunction())
	_ = Register(NewArrayUnionFunction())
	_ = Register(NewArrayExceptFunction())

	// Type checking functions
	_ = Register(NewIsNullFunction())
	_ = Register(NewIsNotNullFunction())
	_ = Register(NewIsNumericFunction())
	_ = Register(NewIsStringFunction())
	_ = Register(NewIsBoolFunction())
	_ = Register(NewIsArrayFunction())
	_ = Register(NewIsObjectFunction())

	// Conditional functions
	_ = Register(NewIfNullFunction())
	_ = Register(NewCoalesceFunction())
	_ = Register(NewNullIfFunction())
	_ = Register(NewGreatestFunction())
	_ = Register(NewLeastFunction())
	_ = Register(NewCaseWhenFunction())

	// Multi-row functions
	_ = Register(NewUnnestFunction())

	// User-defined functions (placeholder for future extension)
	// Example: _=Register(NewMyUserDefinedFunction())
}
