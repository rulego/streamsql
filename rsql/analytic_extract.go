package rsql

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/types"
)

// extractWhereAnalyticCalls 从 WHERE 文本中提取分析函数调用（含可选 OVER），替换为占位符。
// 返回改写后的 WHERE（占位符引用）和调用信息列表。无分析函数时原样返回。
//
// 例："current > 300 and lag(current) over (partition by deviceId) < 300"
//   → "current > 300 and __analytic_0__ < 300" + [{Placeholder:__analytic_0__, FuncName:lag, Over:{PartitionBy:[deviceId]}}]
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
			// 命中分析函数 + 后跟 (
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
	return out.String(), calls, nil
}

// parseOverFromString 从 pos 起尝试解析 "over (...)"，返回 OverSpec 和消耗后的位置。
// 无 OVER 时返回 (nil, pos, nil)。
func parseOverFromString(s string, pos int) (*types.OverSpec, int, error) {
	j := skipSpaces(s, pos)
	if j+4 > len(s) || strings.ToLower(s[j:j+4]) != "over" {
		return nil, pos, nil
	}
	after := j + 4
	// over 后必须是非标识符字符（避免 "overfield" 误匹配）
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

// parseOverBodyString 解析 OVER 体（如 "partition by deviceId, region when status > 0"）。
// 仅支持 PARTITION BY 和 WHEN。
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
