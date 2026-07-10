package stream

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/rulego/streamsql/expr"
	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/utils/fieldpath"
)

// fieldProcessInfo field processing information for caching pre-compiled field processing logic
type fieldProcessInfo struct {
	fieldName       string // Original field name
	outputName      string // Output field name
	isFunctionCall  bool   // Whether it's a function call
	hasNestedField  bool   // Whether it contains nested fields
	isSelectAll     bool   // Whether it's SELECT *
	isStringLiteral bool   // Whether it's a string literal
	stringValue     string // Pre-processed string literal value (quotes removed)
	alias           string // Field alias for quick access
}

// expressionProcessInfo expression processing information for caching pre-compiled expression processing logic
type expressionProcessInfo struct {
	originalExpr            string           // Original expression
	processedExpr           string           // Pre-processed expression
	isFunctionCall          bool             // Whether it's a function call
	hasNestedFields         bool             // Whether it contains nested fields
	compiledExpr            *expr.Expression // Pre-compiled expression object
	compiledExprFastPath    bool             // compiledExpr usable as fast path (no quotes/backticks): skip bridge per-row checks
	needsBacktickPreprocess bool             // Whether backtick preprocessing is needed
}

// compileFieldProcessInfo pre-compiles field processing information to avoid runtime re-parsing
func (s *Stream) compileFieldProcessInfo() {
	s.compiledFieldInfo = make(map[string]*fieldProcessInfo)
	s.compiledExprInfo = make(map[string]*expressionProcessInfo)

	// Compile SimpleFields information
	for _, fieldSpec := range s.config.SimpleFields {
		info := s.compileSimpleFieldInfo(fieldSpec)
		// Strip a leading stream/table alias from the OUTPUT name so joined
		// columns appear unqualified (e.g. "m.location" -> "location"). The
		// full fieldName is kept for resolution. User aliases are left as-is.
		info.outputName = s.stripJoinAlias(info.outputName)
		info.alias = info.outputName
		s.compiledFieldInfo[fieldSpec] = info
	}

	// Pre-compile expression field information
	s.compileExpressionInfo()
}

// stripJoinAlias removes a leading stream/table alias ("m." / "s.") from an
// output field name. Returns the name unchanged if it has no dot or its first
// segment is not a known alias, so user aliases and genuine nested paths survive.
func (s *Stream) stripJoinAlias(name string) string {
	if !strings.Contains(name, ".") {
		return name
	}
	parts := strings.SplitN(name, ".", 2)
	first := parts[0]
	if first == "" {
		return name
	}
	if first == s.config.SourceAlias {
		return parts[1]
	}
	for _, jc := range s.config.JoinConfigs {
		if first == jc.Alias {
			return parts[1]
		}
	}
	return name
}

// groupFieldOutputName returns the OUTPUT column name for a GROUP BY field: the
// SELECT AS alias if one was given for that expression, otherwise the
// join-alias-stripped name. Mirrors the direct path's alias > stripped rule.
func (s *Stream) groupFieldOutputName(gf string) string {
	if a, ok := s.config.SelectAlias[gf]; ok && a != "" {
		return a
	}
	return s.stripJoinAlias(gf)
}

// isInternalAggPlaceholder reports whether a SelectFields key is an internal
// placeholder (e.g. "__count_12345__") that post-aggregation cleanup removes
// before output, so it is excluded from collision detection.
func isInternalAggPlaceholder(name string) bool {
	return strings.HasPrefix(name, "__") && strings.HasSuffix(name, "__")
}

// compileOutputNames resolves GROUP BY output column names and rejects queries
// whose output columns would collide after join-alias stripping. The error is
// surfaced via CreateStream -> Execute so ambiguous SELECTs like
// "SELECT a.name, b.name" fail fast instead of silently shadowing in the map
// (a map[string]any cannot hold two same-named columns).
//
// Detection is split by execution path, since each query takes exactly one:
//   - window/aggregation path (NeedWindow): output = GROUP BY fields + agg aliases
//   - direct path: output = simple fields + expression fields
func (s *Stream) compileOutputNames() error {
	s.groupOutputNames = make([]string, len(s.config.GroupFields))
	seen := make(map[string]bool)

	if s.config.NeedWindow {
		// Window/aggregation path.
		for i, gf := range s.config.GroupFields {
			out := s.groupFieldOutputName(gf)
			s.groupOutputNames[i] = out
			if seen[out] {
				return errAmbiguousColumn(out, "multiple GROUP BY fields resolve to it")
			}
			seen[out] = true
		}
		for alias, aggType := range s.config.SelectFields {
			// Skip internal placeholders and "expression"-typed markers (those
			// are direct-path expression fields, not aggregate outputs).
			if isInternalAggPlaceholder(alias) || string(aggType) == "expression" {
				continue
			}
			if seen[alias] {
				return errAmbiguousColumn(alias, "aggregate alias collides with another output column")
			}
			seen[alias] = true
		}
	} else {
		// Direct path: simple fields (expression fields are produced via
		// FieldExpressions, so skip them here to avoid double counting).
		for _, spec := range s.config.SimpleFields {
			info := s.compiledFieldInfo[spec]
			if info == nil || info.isSelectAll {
				continue
			}
			if _, isExpr := s.config.FieldExpressions[info.outputName]; isExpr {
				continue
			}
			if seen[info.outputName] {
				return errAmbiguousColumn(info.outputName, "multiple SELECT fields resolve to it (e.g. joined tables sharing a column name)")
			}
			seen[info.outputName] = true
		}
		for name := range s.config.FieldExpressions {
			if seen[name] {
				return errAmbiguousColumn(name, "expression output collides with another output column")
			}
			seen[name] = true
		}
	}

	// Rewrite HAVING/ORDER BY references to qualified columns (m.location) into
	// their flat output names (location), since expr-lang parses "m.location" as
	// nested access and cannot resolve it against the flat result map.
	s.rewriteGroupColumnRefs()
	return nil
}

// errAmbiguousColumn formats a compile-time error for a colliding output column.
func errAmbiguousColumn(col, detail string) error {
	return fmt.Errorf("ambiguous output column %q: %s; use AS aliases to disambiguate", col, detail)
}

// projectGroupColumns renames each GROUP BY field from its qualified key to its
// output name (alias > stripped) on every result row. The qualified key is how
// the aggregator/global-window emit the value (needed to resolve it from the
// enriched row); the output name is what reaches sinks. No-op when unchanged.
func (s *Stream) projectGroupColumns(results []map[string]any) {
	for i, gf := range s.config.GroupFields {
		if i >= len(s.groupOutputNames) {
			break
		}
		out := s.groupOutputNames[i]
		if out == gf {
			continue
		}
		for _, row := range results {
			if v, ok := row[gf]; ok {
				if _, exists := row[out]; !exists {
					row[out] = v
				}
				delete(row, gf)
			}
		}
	}
}

// qualifiedRefRe matches dotted identifiers like "m.location" (a maximal run of
// identifier segments joined by "."). Used to rewrite qualified column refs in
// HAVING/ORDER BY to their flat output names without touching substrings.
var qualifiedRefRe = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+`)

// rewriteQualifiedRefs replaces whole-token qualified field references in expr
// with their output names, returning expr unchanged when there is nothing to do.
func rewriteQualifiedRefs(expr string, refs map[string]string) string {
	if expr == "" || len(refs) == 0 {
		return expr
	}
	return qualifiedRefRe.ReplaceAllStringFunc(expr, func(tok string) string {
		if out, ok := refs[tok]; ok {
			return out
		}
		return tok
	})
}

// rewriteGroupColumnRefs rewrites HAVING and ORDER BY so references to qualified
// GROUP BY / joined columns use their flat output names. expr-lang parses
// "m.location" as nested access (m -> location), which does not resolve against
// the flat result map; rewriting to "location" makes HAVING/ORDER BY work.
func (s *Stream) rewriteGroupColumnRefs() {
	refs := make(map[string]string)
	for i, gf := range s.config.GroupFields {
		if i < len(s.groupOutputNames) && s.groupOutputNames[i] != gf {
			refs[gf] = s.groupOutputNames[i]
		}
	}
	for _, info := range s.compiledFieldInfo {
		if info.outputName != "" && info.outputName != info.fieldName && strings.Contains(info.fieldName, ".") {
			refs[info.fieldName] = info.outputName
		}
	}
	if len(refs) == 0 {
		return
	}
	s.config.Having = rewriteQualifiedRefs(s.config.Having, refs)
	for i := range s.config.OrderBy {
		s.config.OrderBy[i].Expression = rewriteQualifiedRefs(s.config.OrderBy[i].Expression, refs)
	}
}

// compileSimpleFieldInfo compiles simple field information
func (s *Stream) compileSimpleFieldInfo(fieldSpec string) *fieldProcessInfo {
	info := &fieldProcessInfo{}

	if fieldSpec == "*" {
		info.isSelectAll = true
		info.fieldName = ""
		info.outputName = "*"
		info.alias = "*"
		return info
	}

	// Parse alias
	var parts []string
	// Helper to split field spec considering quotes
	splitFieldSpec := func(spec string) []string {
		inQuote := false
		var quoteChar byte
		for i := 0; i < len(spec); i++ {
			c := spec[i]
			if inQuote {
				if c == quoteChar {
					inQuote = false
				}
			} else {
				if c == '\'' || c == '"' || c == '`' {
					inQuote = true
					quoteChar = c
				} else if c == ':' {
					// Found separator
					return []string{spec[:i], spec[i+1:]}
				}
			}
		}
		// No separator found outside quotes
		return []string{spec}
	}

	parts = splitFieldSpec(fieldSpec)
	info.fieldName = parts[0]
	// Remove backticks from field name
	if len(info.fieldName) >= 2 && info.fieldName[0] == '`' && info.fieldName[len(info.fieldName)-1] == '`' {
		info.fieldName = info.fieldName[1 : len(info.fieldName)-1]
	}
	info.outputName = info.fieldName
	if len(parts) > 1 {
		info.outputName = parts[1]
		// Remove backticks from output name
		if len(info.outputName) >= 2 && info.outputName[0] == '`' && info.outputName[len(info.outputName)-1] == '`' {
			info.outputName = info.outputName[1 : len(info.outputName)-1]
		}
	}

	// Pre-determine field characteristics
	info.isFunctionCall = strings.Contains(info.fieldName, "(") && strings.Contains(info.fieldName, ")")
	info.hasNestedField = !info.isFunctionCall && fieldpath.IsNestedField(info.fieldName)

	// Check if it's a string literal and preprocess value
	info.isStringLiteral = (len(info.fieldName) >= 2 &&
		((info.fieldName[0] == '\'' && info.fieldName[len(info.fieldName)-1] == '\'') ||
			(info.fieldName[0] == '"' && info.fieldName[len(info.fieldName)-1] == '"')))

	// Preprocess string literal value, remove quotes
	if info.isStringLiteral && len(info.fieldName) >= 2 {
		info.stringValue = info.fieldName[1 : len(info.fieldName)-1]
	}

	// Set alias for quick access
	info.alias = info.outputName

	return info
}

// compileExpressionInfo pre-compiles expression processing information
func (s *Stream) compileExpressionInfo() {
	// Initialize unnest function detection flag
	s.hasUnnestFunction = false
	bridge := functions.GetExprBridge()

	for fieldName, fieldExpr := range s.config.FieldExpressions {
		exprInfo := &expressionProcessInfo{
			originalExpr: fieldExpr.Expression,
		}

		// Preprocess expression
		processedExpr := fieldExpr.Expression
		if bridge.ContainsIsNullOperator(processedExpr) {
			if processed, err := bridge.PreprocessIsNullExpression(processedExpr); err == nil {
				processedExpr = processed
			}
		}
		if bridge.ContainsLikeOperator(processedExpr) {
			if processed, err := bridge.PreprocessLikeExpression(processedExpr); err == nil {
				processedExpr = processed
			}
		}
		exprInfo.processedExpr = processedExpr

		// Pre-judge expression characteristics
		exprInfo.isFunctionCall = strings.Contains(fieldExpr.Expression, "(") && strings.Contains(fieldExpr.Expression, ")")
		exprInfo.hasNestedFields = !exprInfo.isFunctionCall && strings.Contains(fieldExpr.Expression, ".")
		exprInfo.needsBacktickPreprocess = bridge.ContainsBacktickIdentifiers(fieldExpr.Expression)

		// Check if expression contains unnest function
		if exprInfo.isFunctionCall && strings.Contains(strings.ToLower(fieldExpr.Expression), "unnest(") {
			s.hasUnnestFunction = true
		}

		// Pre-compile expression object (only for non-function call expressions)
		if !exprInfo.isFunctionCall {
			exprToCompile := fieldExpr.Expression
			if exprInfo.needsBacktickPreprocess {
				if processed, err := bridge.PreprocessBacktickIdentifiers(exprToCompile); err == nil {
					exprToCompile = processed
				}
			}
			if compiledExpr, err := expr.NewExpression(exprToCompile); err == nil {
				exprInfo.compiledExpr = compiledExpr
				// Fast path: when compiledExpr is available and the expression has no
				// quote/backtick characters (so it is not string concatenation or a
				// quoted identifier), evaluate directly via compiledExpr and skip the
				// bridge's per-row isStringConcatenation/usesExprFunction checks.
				if !strings.ContainsAny(fieldExpr.Expression, "'\"`") {
					exprInfo.compiledExprFastPath = true
				}
			}
		}

		s.compiledExprInfo[fieldName] = exprInfo
	}
}

// processExpressionField processes expression field
func (s *Stream) processExpressionField(fieldName string, dataMap map[string]any, result map[string]any) {
	exprInfo := s.compiledExprInfo[fieldName]
	if exprInfo == nil {
		// Fallback to original logic
		s.processExpressionFieldFallback(fieldName, dataMap, result)
		return
	}

	var evalResult any
	bridge := functions.GetExprBridge()

	if exprInfo.isFunctionCall {
		// For function calls, use bridge processor
		exprResult, err := bridge.EvaluateExpression(exprInfo.processedExpr, dataMap)
		if err != nil {
			s.log.Error("Function call evaluation failed for field %s: %v", fieldName, err)
			result[fieldName] = nil
			return
		}
		evalResult = exprResult
	} else if exprInfo.hasNestedFields {
		// Use pre-compiled expression object
		if exprInfo.compiledExpr != nil {
			// Use EvaluateValueWithNull to get actual value (including strings)
			exprResult, isNull, err := exprInfo.compiledExpr.EvaluateValueWithNull(dataMap)
			if err != nil {
				s.log.Error("Expression evaluation failed for field %s: %v", fieldName, err)
				result[fieldName] = nil
				return
			}
			if isNull {
				evalResult = nil
			} else {
				evalResult = exprResult
			}
		} else {
			// Fallback to dynamic compilation
			s.processExpressionFieldFallback(fieldName, dataMap, result)
			return
		}
	} else if exprInfo.compiledExprFastPath {
		// Fast path: pure arithmetic / field-reference expressions (no quotes) go
		// straight through the custom expr engine, bypassing the bridge's per-row
		// isStringConcatenation/usesExprFunction checks. Fall back to bridge on error.
		exprResult, isNull, err := exprInfo.compiledExpr.EvaluateValueWithNull(dataMap)
		if err == nil {
			if isNull {
				evalResult = nil
			} else {
				evalResult = exprResult
			}
		} else {
			exprResult, berr := bridge.EvaluateExpression(exprInfo.processedExpr, dataMap)
			if berr != nil {
				s.log.Error("Expression evaluation failed for field %s: %v", fieldName, berr)
				result[fieldName] = nil
				return
			}
			evalResult = exprResult
		}
	} else {
		// Try using bridge processor for other expressions
		exprResult, err := bridge.EvaluateExpression(exprInfo.processedExpr, dataMap)
		if err != nil {
			// If bridge fails, use pre-compiled expression object
			if exprInfo.compiledExpr != nil {
				// Use EvaluateValueWithNull to get actual value (including strings)
				exprResult, isNull, evalErr := exprInfo.compiledExpr.EvaluateValueWithNull(dataMap)
				if evalErr != nil {
					s.log.Error("Expression evaluation failed for field %s: %v", fieldName, evalErr)
					result[fieldName] = nil
					return
				}
				if isNull {
					evalResult = nil
				} else {
					evalResult = exprResult
				}
			} else {
				// Fallback to dynamic compilation
				s.processExpressionFieldFallback(fieldName, dataMap, result)
				return
			}
		} else {
			evalResult = exprResult
		}
	}

	result[fieldName] = evalResult
}

// processExpressionFieldFallback fallback logic for expression field processing
func (s *Stream) processExpressionFieldFallback(fieldName string, dataMap map[string]any, result map[string]any) {
	fieldExpr, exists := s.config.FieldExpressions[fieldName]
	if !exists {
		result[fieldName] = nil
		return
	}

	// Use bridge to calculate expression, supports IS NULL and other syntax
	bridge := functions.GetExprBridge()

	// Preprocess IS NULL and LIKE syntax in expression
	processedExpr := fieldExpr.Expression
	if bridge.ContainsIsNullOperator(processedExpr) {
		if processed, err := bridge.PreprocessIsNullExpression(processedExpr); err == nil {
			processedExpr = processed
		}
	}
	if bridge.ContainsLikeOperator(processedExpr) {
		if processed, err := bridge.PreprocessLikeExpression(processedExpr); err == nil {
			processedExpr = processed
		}
	}

	// Check if expression is a function call (contains parentheses)
	isFunctionCall := strings.Contains(fieldExpr.Expression, "(") && strings.Contains(fieldExpr.Expression, ")")

	// Check if expression contains nested fields (but exclude dots in function calls)
	hasNestedFields := false
	if !isFunctionCall && strings.Contains(fieldExpr.Expression, ".") {
		hasNestedFields = true
	}

	var evalResult any

	if isFunctionCall {
		// For function calls, prioritize bridge processor
		exprResult, err := bridge.EvaluateExpression(processedExpr, dataMap)
		if err != nil {
			s.log.Error("Function call evaluation failed for field %s: %v", fieldName, err)
			result[fieldName] = nil
			return
		}
		evalResult = exprResult
	} else if hasNestedFields {
		// Detected nested fields (non-function call), use custom expression engine
		exprToUse := fieldExpr.Expression
		if bridge.ContainsBacktickIdentifiers(exprToUse) {
			if processed, err := bridge.PreprocessBacktickIdentifiers(exprToUse); err == nil {
				exprToUse = processed
			}
		}
		expression, parseErr := expr.NewExpression(exprToUse)
		if parseErr != nil {
			s.log.Error("Expression parse failed for field %s: %v", fieldName, parseErr)
			result[fieldName] = nil
			return
		}

		// Use EvaluateValueWithNull to get actual value (including strings)
		exprResult, isNull, err := expression.EvaluateValueWithNull(dataMap)
		if err != nil {
			s.log.Error("Expression evaluation failed for field %s: %v", fieldName, err)
			result[fieldName] = nil
			return
		}
		if isNull {
			evalResult = nil
		} else {
			evalResult = exprResult
		}
	} else {
		// Try using bridge processor for other expressions
		exprResult, err := bridge.EvaluateExpression(processedExpr, dataMap)
		if err != nil {
			// If bridge fails, fallback to original expression engine
			exprToUse := fieldExpr.Expression
			if bridge.ContainsBacktickIdentifiers(exprToUse) {
				if processed, err := bridge.PreprocessBacktickIdentifiers(exprToUse); err == nil {
					exprToUse = processed
				}
			}
			expression, parseErr := expr.NewExpression(exprToUse)
			if parseErr != nil {
				s.log.Error("Expression parse failed for field %s: %v", fieldName, parseErr)
				result[fieldName] = nil
				return
			}

			// Use EvaluateValueWithNull to get actual value (including strings)
			exprResult, isNull, evalErr := expression.EvaluateValueWithNull(dataMap)
			if evalErr != nil {
				s.log.Error("Expression evaluation failed for field %s: %v", fieldName, evalErr)
				result[fieldName] = nil
				return
			}
			if isNull {
				evalResult = nil
			} else {
				evalResult = exprResult
			}
		} else {
			evalResult = exprResult
		}
	}

	result[fieldName] = evalResult
}

// processSimpleField processes simple field
func (s *Stream) processSimpleField(fieldSpec string, dataMap map[string]any, data any, result map[string]any) {
	info := s.compiledFieldInfo[fieldSpec]
	if info == nil {
		// If no pre-compiled info, fallback to original logic (safety guarantee)
		s.processSingleFieldFallback(fieldSpec, dataMap, data, result)
		return
	}

	if info.isSelectAll {
		// SELECT *: batch copy all fields, skip expression fields
		for k, v := range dataMap {
			if _, isExpression := s.config.FieldExpressions[k]; !isExpression {
				result[k] = v
			}
		}
		return
	}

	// Skip fields already processed by expression fields
	if _, isExpression := s.config.FieldExpressions[info.outputName]; isExpression {
		return
	}

	if info.isStringLiteral {
		// String literal processing: use pre-compiled string value
		result[info.alias] = info.stringValue
	} else if info.isFunctionCall {
		// Execute function call
		if funcResult, err := s.executeFunction(info.fieldName, dataMap); err == nil {
			result[info.outputName] = funcResult
		} else {
			s.log.Error("Function execution error %s: %v", info.fieldName, err)
			result[info.outputName] = nil
		}
	} else {
		// Ordinary field processing
		var value any
		var exists bool

		if info.hasNestedField {
			value, exists = fieldpath.GetNestedField(data, info.fieldName)
		} else {
			value, exists = dataMap[info.fieldName]
		}

		if exists {
			result[info.outputName] = value
		} else {
			result[info.outputName] = nil
		}
	}
}

// processSingleFieldFallback fallback processing for single field (when pre-compiled info is missing)
func (s *Stream) processSingleFieldFallback(fieldSpec string, dataMap map[string]any, data any, result map[string]any) {
	// Handle special case of SELECT *
	if fieldSpec == "*" {
		// SELECT *: return all fields, but skip fields already processed by expression fields
		for k, v := range dataMap {
			// If field already processed by expression field, skip, maintain expression calculation result
			if _, isExpression := s.config.FieldExpressions[k]; !isExpression {
				result[k] = v
			}
		}
		return
	}

	// Handle alias
	var parts []string
	// Helper to split field spec considering quotes
	splitFieldSpec := func(spec string) []string {
		inQuote := false
		var quoteChar byte
		for i := 0; i < len(spec); i++ {
			c := spec[i]
			if inQuote {
				if c == quoteChar {
					inQuote = false
				}
			} else {
				if c == '\'' || c == '"' || c == '`' {
					inQuote = true
					quoteChar = c
				} else if c == ':' {
					// Found separator
					return []string{spec[:i], spec[i+1:]}
				}
			}
		}
		// No separator found outside quotes
		return []string{spec}
	}

	parts = splitFieldSpec(fieldSpec)
	fieldName := parts[0]
	outputName := fieldName
	if len(parts) > 1 {
		outputName = parts[1]
	}

	// Skip fields already processed by expression fields
	if _, isExpression := s.config.FieldExpressions[outputName]; isExpression {
		return
	}

	// Check if it's a function call
	if strings.Contains(fieldName, "(") && strings.Contains(fieldName, ")") {
		// Execute function call
		if funcResult, err := s.executeFunction(fieldName, dataMap); err == nil {
			result[outputName] = funcResult
		} else {
			s.log.Error("Function execution error %s: %v", fieldName, err)
			result[outputName] = nil
		}
	} else {
		// Ordinary field - supports nested fields
		var value any
		var exists bool

		if fieldpath.IsNestedField(fieldName) {
			value, exists = fieldpath.GetNestedField(data, fieldName)
		} else {
			value, exists = dataMap[fieldName]
		}

		if exists {
			result[outputName] = value
		} else {
			result[outputName] = nil
		}
	}
}

// executeFunction executes function call
func (s *Stream) executeFunction(funcExpr string, data map[string]any) (any, error) {
	// Check if it's a custom function
	funcName := extractFunctionName(funcExpr)
	if funcName != "" {
		// Use function system directly
		fn, exists := functions.Get(funcName)
		if exists {
			// Parse parameters
			args, err := s.parseFunctionArgs(funcExpr, data)
			if err != nil {
				return nil, err
			}

			// Create function context
			ctx := &functions.FunctionContext{Data: data}

			// Execute function
			return fn.Execute(ctx, args)
		}
	}

	// For complex nested function calls, use ExprBridge directly
	// This avoids the float64 type limitation of Expression.Evaluate
	bridge := functions.GetExprBridge()
	result, err := bridge.EvaluateExpression(funcExpr, data)
	if err != nil {
		return nil, fmt.Errorf("evaluate function expression failed: %w", err)
	}

	return result, nil
}

// extractFunctionName extracts function name from expression
func extractFunctionName(expr string) string {
	parenIndex := strings.Index(expr, "(")
	if parenIndex == -1 {
		return ""
	}
	funcName := strings.TrimSpace(expr[:parenIndex])
	if strings.ContainsAny(funcName, " +-*/=<>!&|") {
		return ""
	}
	return funcName
}

// parseFunctionArgs parses function arguments, supports nested function calls
func (s *Stream) parseFunctionArgs(funcExpr string, data map[string]any) ([]any, error) {
	// Extract parameters within parentheses
	start := strings.Index(funcExpr, "(")
	end := strings.LastIndex(funcExpr, ")")
	if start == -1 || end == -1 || end <= start {
		return nil, fmt.Errorf("invalid function expression: %s", funcExpr)
	}

	argsStr := strings.TrimSpace(funcExpr[start+1 : end])
	if argsStr == "" {
		return []any{}, nil
	}

	// Smart split arguments, handle nested functions and quotes
	argParts, err := s.smartSplitArgs(argsStr)
	if err != nil {
		return nil, err
	}

	args := make([]any, len(argParts))

	for i, arg := range argParts {
		arg = strings.TrimSpace(arg)

		// If parameter is string constant (enclosed in quotes)
		if strings.HasPrefix(arg, "'") && strings.HasSuffix(arg, "'") {
			args[i] = strings.Trim(arg, "'")
		} else if strings.HasPrefix(arg, "\"") && strings.HasSuffix(arg, "\"") {
			args[i] = strings.Trim(arg, "\"")
		} else if strings.Contains(arg, "(") {
			// If parameter contains function call, execute recursively
			result, err := s.executeFunction(arg, data)
			if err != nil {
				return nil, fmt.Errorf("failed to execute nested function '%s': %v", arg, err)
			}
			args[i] = result
		} else if value, exists := data[arg]; exists {
			// If it's a data field
			args[i] = value
		} else if val, err := strconv.ParseFloat(arg, 64); err == nil {
			// Numeric literal
			args[i] = val
		} else if containsExpressionOperator(arg) {
			// Argument is an arithmetic/logical expression (e.g. v/3, a+b, x>5)
			// that the field/literal lookups above cannot resolve. Evaluate it
			// against the row so functions like round(v/total, 2) return a value
			// instead of silently nil-ing (the raw string failing ToFloat64).
			// Args containing '(' are already handled as nested functions above,
			// so this branch only sees operator-only expressions — no recursion.
			if result, err := functions.GetExprBridge().EvaluateExpression(arg, data); err == nil {
				args[i] = result
			} else {
				args[i] = arg
			}
		} else {
			args[i] = arg
		}
	}

	return args, nil
}

// containsExpressionOperator reports whether s contains an arithmetic or
// logical operator, i.e. looks like an expression rather than a bare
// identifier or literal. Pure numbers (incl. negatives/exponents) and known
// fields are resolved before this is consulted.
func containsExpressionOperator(s string) bool {
	return strings.ContainsAny(s, "+-*/%<>!=&|")
}

// smartSplitArgs intelligently splits arguments, considering bracket nesting and quotes
func (s *Stream) smartSplitArgs(argsStr string) ([]string, error) {
	var args []string
	var current strings.Builder
	parenDepth := 0
	inQuotes := false
	quoteChar := byte(0)

	for i := 0; i < len(argsStr); i++ {
		ch := argsStr[i]

		switch ch {
		case '\'':
			if !inQuotes {
				inQuotes = true
				quoteChar = ch
			} else if quoteChar == ch {
				inQuotes = false
				quoteChar = 0
			}
			current.WriteByte(ch)
		case '"':
			if !inQuotes {
				inQuotes = true
				quoteChar = ch
			} else if quoteChar == ch {
				inQuotes = false
				quoteChar = 0
			}
			current.WriteByte(ch)
		case '(':
			if !inQuotes {
				parenDepth++
			}
			current.WriteByte(ch)
		case ')':
			if !inQuotes {
				parenDepth--
			}
			current.WriteByte(ch)
		case ',':
			if !inQuotes && parenDepth == 0 {
				// Found parameter separator
				args = append(args, strings.TrimSpace(current.String()))
				current.Reset()
			} else {
				current.WriteByte(ch)
			}
		default:
			current.WriteByte(ch)
		}
	}

	// Add the last parameter
	if current.Len() > 0 {
		args = append(args, strings.TrimSpace(current.String()))
	}

	return args, nil
}
