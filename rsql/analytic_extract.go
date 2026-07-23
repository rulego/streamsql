package rsql

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/types"
)

// extractWhereAnalyticCalls extracts analysis function calls (including optional OVER) from the WHERE text and replaces them with placeholders.
// Returns the rewritten WHERE (placeholder reference) and a list of call information. Returns as is when there is no analysis function.
//
// Example: "current > 300 and lag(current) over (partition by deviceId) < 300"
//
//	→ "current > 300 and __analytic_0__ < 300" + [{Placeholder:__analytic_0__, FuncName:lag, Over:{PartitionBy:[deviceId]}}]
func extractWhereAnalyticCalls(condition string) (string, []types.WhereAnalyticCall, error) {
	if strings.TrimSpace(condition) == "" {
		return condition, nil, nil
	}
	var calls []types.WhereAnalyticCall
	var out strings.Builder
	i := 0
	callIdx := 0
	for i < len(condition) {
		ch := condition[i]
		if isLetter(ch) {
			start := i
			for i < len(condition) && (isLetter(condition[i]) || isDigit(condition[i])) {
				i++
			}
			ident := condition[start:i]
			lowerIdent := strings.ToLower(ident)
			// Hit analysis function + heel (
			if fn, ok := functions.Get(lowerIdent); ok && fn.GetType() == functions.TypeAnalytical {
				j := skipSpaces(condition, i)
				if j < len(condition) && condition[j] == '(' {
					argStart := j + 1
					depth := 1
					k := j + 1
					for k < len(condition) && depth > 0 {
						switch condition[k] {
						case '(':
							depth++
						case ')':
							depth--
						}
						if depth > 0 {
							k++
						}
					}
					if depth != 0 {
						return "", nil, fmt.Errorf("unbalanced parens in WHERE analytic call: %s", ident)
					}
					argsStr := condition[argStart:k]
					expr := ident + "(" + argsStr + ")"
					over, after, err := parseOverFromString(condition, k+1)
					if err != nil {
						return "", nil, err
					}
					placeholder := fmt.Sprintf("__analytic_%d__", callIdx)
					callIdx++
					calls = append(calls, types.WhereAnalyticCall{
						Placeholder: placeholder,
						FuncName:    lowerIdent,
						Expression:  expr,
						Args:        splitCallArgs(expr),
						Over:        over,
					})
					out.WriteString(placeholder)
					i = after
					continue
				}
			}
			out.WriteString(ident)
			continue
		}
		out.WriteByte(ch)
		i++
	}
	result := out.String()
	// The bare analysis function performs the entire WHERE condition (e.g., WHERE changed_col(true, temp)): the value-type analysis function returns
	// The column value itself (possibly valid variants like 0/""/false), expr AsBool will fail → full filtering of integer conditions always false.
	// Rewrite as "Placeholder!= nil": Variant/Value = Non-nil→true, Unchanged = nil→false (not affected by whether the new value is 0/empty string).
	// had_changed Returns bool, which AsBool can handle directly without rewriting (otherwise, had_changed=false will be false false and false).
	if len(calls) == 1 && calls[0].FuncName != "had_changed" && strings.TrimSpace(result) == calls[0].Placeholder {
		result = calls[0].Placeholder + " != nil"
	}
	return result, calls, nil
}

// parseOverFromString attempts to parse "over (...)" starting from pos, returning the position after OverSpec and consumption.
// Returns when there is no OVER (nil, pos, nil).
func parseOverFromString(s string, pos int) (*types.OverSpec, int, error) {
	j := skipSpaces(s, pos)
	if j+4 > len(s) || strings.ToLower(s[j:j+4]) != "over" {
		return nil, pos, nil
	}
	after := j + 4
	// After over, the character must be a non-identifier (to avoid mismatches with "overfield").
	if after < len(s) && (isLetter(s[after]) || isDigit(s[after])) {
		return nil, pos, nil
	}
	after = skipSpaces(s, after)
	if after >= len(s) || s[after] != '(' {
		return nil, pos, nil
	}
	depth := 1
	bodyStart := after + 1
	k := after + 1
	for k < len(s) && depth > 0 {
		switch s[k] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth > 0 {
			k++
		}
	}
	body := s[bodyStart:k]
	spec, err := parseOverBodyString(body)
	if err != nil {
		return nil, pos, err
	}
	return spec, k + 1, nil
}

var (
	overPartitionRe = regexp.MustCompile(`(?i)\bpartition\s+by\b\s+(.*?)(?:\bwhen\b|$)`)
	overWhenRe      = regexp.MustCompile(`(?i)\bwhen\b\s+(.*)$`)
)

// parseOverBodyString parses the OVER body (e.g., "partition by deviceId, region when status > 0").
// Only PARTITION BY and WHEN are supported.
func parseOverBodyString(body string) (*types.OverSpec, error) {
	spec := &types.OverSpec{}
	if m := overPartitionRe.FindStringSubmatch(body); m != nil {
		for _, f := range strings.Split(m[1], ",") {
			f = strings.TrimSpace(strings.Trim(strings.TrimSpace(f), "`"))
			if f != "" {
				spec.PartitionBy = append(spec.PartitionBy, f)
			}
		}
	}
	if m := overWhenRe.FindStringSubmatch(body); m != nil {
		spec.When = strings.TrimSpace(m[1])
	}
	return spec, nil
}

func skipSpaces(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	return i
}
