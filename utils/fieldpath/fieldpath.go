package fieldpath

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// FieldAccessor 字段访问器结构，用于解析复杂的字段路径
type FieldAccessor struct {
	Parts []FieldPart
}

// FieldPart 字段路径的单个部分
type FieldPart struct {
	Type    string // "field", "array_index", "map_key"
	Name    string // 字段名或键名
	Index   int    // 数组索引（当Type为"array_index"时）
	Key     string // Map键（当Type为"map_key"时）
	KeyType string // 键类型："string", "number"
}

// 正则表达式用于解析复杂字段路径
var (
	// 匹配数组索引：[0], [1], [-1] 等
	arrayIndexRegex = regexp.MustCompile(`\[(-?\d+)\]`)
	// 匹配字符串键：["key"], ['key'] 等
	stringKeyRegex = regexp.MustCompile(`\[['"]([^'"]*)['"]\]`)
	// 匹配数字键：[123] 等（与数组索引相同，但在Map上下文中）
	numberKeyRegex = regexp.MustCompile(`\[(\d+)\]`)
)

// ParseFieldPath 解析字段路径，支持点号、数组索引、Map键等复杂访问
// 支持的格式：
// - a.b.c (嵌套字段)
// - a.b[0] (数组索引)
// - a.b[0].c (数组元素的字段)
// - a.b["key"] (字符串键)
// - a.b['key'] (字符串键)
// - a.b[123] (数字键或数组索引)
// - a[0].b[1].c["key"] (混合访问)
func ParseFieldPath(fieldPath string) (*FieldAccessor, error) {
	if fieldPath == "" {
		return nil, nil
	}

	accessor := &FieldAccessor{
		Parts: make([]FieldPart, 0),
	}

	// 首先处理点号分割的基本路径
	parts := strings.Split(fieldPath, ".")

	for _, part := range parts {
		if part == "" {
			continue
		}

		// 检查当前部分是否包含数组索引或Map键访问
		if strings.Contains(part, "[") {
			// 处理包含索引/键的复杂部分
			err := parseComplexPart(part, accessor)
			if err != nil {
				return nil, err
			}
		} else {
			// 简单字段名
			accessor.Parts = append(accessor.Parts, FieldPart{
				Type: "field",
				Name: part,
			})
		}
	}

	return accessor, nil
}

// parseComplexPart 解析包含索引或键访问的复杂部分
func parseComplexPart(part string, accessor *FieldAccessor) error {
	// 找到第一个 '[' 的位置
	bracketIndex := strings.Index(part, "[")
	if bracketIndex == -1 {
		// 没有括号，当作普通字段处理
		accessor.Parts = append(accessor.Parts, FieldPart{
			Type: "field",
			Name: part,
		})
		return nil
	}

	// 如果有字段名部分，先添加字段访问
	if bracketIndex > 0 {
		fieldName := part[:bracketIndex]
		accessor.Parts = append(accessor.Parts, FieldPart{
			Type: "field",
			Name: fieldName,
		})
	}

	// 解析剩余的索引/键访问部分
	remaining := part[bracketIndex:]

	// 依次处理所有的 [xxx] 部分
	for len(remaining) > 0 && strings.HasPrefix(remaining, "[") {
		// 找到匹配的右括号
		rightBracket := strings.Index(remaining, "]")
		if rightBracket == -1 {
			return &FieldAccessError{
				Path:    part,
				Message: "unmatched bracket in field path",
			}
		}

		// 提取括号内的内容
		bracketContent := remaining[1:rightBracket]

		// 解析括号内容
		fieldPart, err := parseBracketContent(bracketContent)
		if err != nil {
			return err
		}

		accessor.Parts = append(accessor.Parts, fieldPart)

		// 移动到下一部分
		remaining = remaining[rightBracket+1:]
	}

	return nil
}

// parseBracketContent 解析括号内的内容
func parseBracketContent(content string) (FieldPart, error) {
	content = strings.TrimSpace(content)

	// 检查是否是字符串键（带引号）
	if (strings.HasPrefix(content, "'") && strings.HasSuffix(content, "'")) ||
		(strings.HasPrefix(content, "\"") && strings.HasSuffix(content, "\"")) {
		// 字符串键
		key := content[1 : len(content)-1] // 去掉引号
		return FieldPart{
			Type:    "map_key",
			Key:     key,
			KeyType: "string",
		}, nil
	}

	// 检查是否是数字
	if num, err := strconv.Atoi(content); err == nil {
		// 数字，可能是数组索引或数字键
		return FieldPart{
			Type:    "array_index", // 默认当作数组索引，实际使用时会根据数据类型调整
			Index:   num,
			Key:     content,
			KeyType: "number",
		}, nil
	}

	return FieldPart{}, &FieldAccessError{
		Path:    content,
		Message: "invalid bracket content, expected number or quoted string",
	}
}

// GetNestedField 从嵌套的map或结构体中获取字段值
// 支持点号分隔的字段路径、数组索引、Map键等复杂操作
// 支持的格式：
// - "device.info.name" (嵌套字段)
// - "data[0]" (数组索引)
// - "users[0].name" (数组元素的字段)
// - "config['key']" (字符串键)
// - "items[0][1]" (多维数组)
// - "nested.data[0].field['key']" (混合访问)
func GetNestedField(data interface{}, fieldPath string) (interface{}, bool) {
	if fieldPath == "" {
		return nil, false
	}

	// 解析字段路径
	accessor, err := ParseFieldPath(fieldPath)
	if err != nil {
		// 如果解析失败，回退到原有的简单点号访问
		return getNestedFieldSimple(data, fieldPath)
	}

	if accessor == nil || len(accessor.Parts) == 0 {
		return nil, false
	}

	// 按照解析的路径逐步访问
	current := data
	for _, part := range accessor.Parts {
		val, found := accessFieldPart(current, part)
		if !found {
			return nil, false
		}
		current = val
	}

	return current, true
}

// accessFieldPart 访问单个字段部分
func accessFieldPart(data interface{}, part FieldPart) (interface{}, bool) {
	if data == nil {
		return nil, false
	}

	v := reflect.ValueOf(data)

	// 如果是指针，解引用
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, false
		}
		v = v.Elem()
	}

	switch part.Type {
	case "field":
		return getFieldValue(data, part.Name)

	case "array_index":
		return getArrayElement(v, part.Index)

	case "map_key":
		return getMapValue(v, part.Key, part.KeyType)

	default:
		return nil, false
	}
}

// getArrayElement 获取数组或切片元素
func getArrayElement(v reflect.Value, index int) (interface{}, bool) {
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		length := v.Len()

		// 支持负数索引（从末尾开始）
		if index < 0 {
			index = length + index
		}

		if index < 0 || index >= length {
			return nil, false
		}

		elem := v.Index(index)
		return elem.Interface(), true

	case reflect.Map:
		// 如果数据是Map，将索引作为键来访问
		key := reflect.ValueOf(index)
		mapVal := v.MapIndex(key)
		if mapVal.IsValid() {
			return mapVal.Interface(), true
		}

		// 尝试字符串形式的索引
		strKey := reflect.ValueOf(strconv.Itoa(index))
		mapVal = v.MapIndex(strKey)
		if mapVal.IsValid() {
			return mapVal.Interface(), true
		}

		return nil, false

	default:
		return nil, false
	}
}

// getMapValue 获取Map值
func getMapValue(v reflect.Value, key, keyType string) (interface{}, bool) {
	if v.Kind() != reflect.Map {
		return nil, false
	}

	// 首先尝试字符串键
	if keyType == "string" || v.Type().Key().Kind() == reflect.String {
		mapVal := v.MapIndex(reflect.ValueOf(key))
		if mapVal.IsValid() {
			return mapVal.Interface(), true
		}
	}

	// 如果是数字类型的键
	if keyType == "number" {
		if num, err := strconv.Atoi(key); err == nil {
			// 尝试int键
			mapVal := v.MapIndex(reflect.ValueOf(num))
			if mapVal.IsValid() {
				return mapVal.Interface(), true
			}

			// 尝试字符串形式的数字键
			mapVal = v.MapIndex(reflect.ValueOf(key))
			if mapVal.IsValid() {
				return mapVal.Interface(), true
			}
		}
	}

	return nil, false
}

// getNestedFieldSimple 原有的简单点号访问（向后兼容）
func getNestedFieldSimple(data interface{}, fieldPath string) (interface{}, bool) {
	if fieldPath == "" {
		return nil, false
	}

	// 分割字段路径
	fields := strings.Split(fieldPath, ".")
	current := data

	for _, field := range fields {
		val, found := getFieldValue(current, field)
		if !found {
			return nil, false
		}
		current = val
	}

	return current, true
}

// getFieldValue 从单个层级获取字段值
func getFieldValue(data interface{}, fieldName string) (interface{}, bool) {
	if data == nil {
		return nil, false
	}

	v := reflect.ValueOf(data)

	// 如果是指针，解引用
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, false
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Map:
		// 处理 map[string]interface{}
		if v.Type().Key().Kind() == reflect.String {
			mapVal := v.MapIndex(reflect.ValueOf(fieldName))
			if mapVal.IsValid() {
				return mapVal.Interface(), true
			}
		}
		return nil, false

	case reflect.Struct:
		// 处理结构体
		fieldVal := v.FieldByName(fieldName)
		if fieldVal.IsValid() {
			return fieldVal.Interface(), true
		}
		return nil, false

	default:
		return nil, false
	}
}

// SetNestedField 在嵌套的map中设置字段值，支持复杂路径
// 如果路径中的某些层级不存在，会自动创建
func SetNestedField(data map[string]interface{}, fieldPath string, value interface{}) error {
	if fieldPath == "" {
		return &FieldAccessError{
			Path:    fieldPath,
			Message: "empty field path",
		}
	}

	// 解析字段路径
	accessor, err := ParseFieldPath(fieldPath)
	if err != nil {
		// 如果解析失败，回退到原有的简单设置
		setNestedFieldSimple(data, fieldPath, value)
		return nil
	}

	if accessor == nil || len(accessor.Parts) == 0 {
		return &FieldAccessError{
			Path:    fieldPath,
			Message: "invalid field path",
		}
	}

	// 逐级创建路径并设置值
	current := data

	// 处理除最后一个部分外的所有部分
	for i := 0; i < len(accessor.Parts)-1; i++ {
		part := accessor.Parts[i]

		if part.Type != "field" {
			return &FieldAccessError{
				Path:    fieldPath,
				Message: "complex path setting only supports field access in intermediate parts",
			}
		}

		// 确保中间路径存在
		if next, exists := current[part.Name]; exists {
			if nextMap, ok := next.(map[string]interface{}); ok {
				current = nextMap
			} else {
				// 如果存在但不是map，创建新的map覆盖
				newMap := make(map[string]interface{})
				current[part.Name] = newMap
				current = newMap
			}
		} else {
			// 如果不存在，创建新的map
			newMap := make(map[string]interface{})
			current[part.Name] = newMap
			current = newMap
		}
	}

	// 处理最后一个部分
	lastPart := accessor.Parts[len(accessor.Parts)-1]
	if lastPart.Type == "field" {
		current[lastPart.Name] = value
	} else {
		return &FieldAccessError{
			Path:    fieldPath,
			Message: "complex path setting only supports field access for final part",
		}
	}

	return nil
}

// setNestedFieldSimple 原有的简单设置（向后兼容）
func setNestedFieldSimple(data map[string]interface{}, fieldPath string, value interface{}) {
	if fieldPath == "" {
		return
	}

	fields := strings.Split(fieldPath, ".")
	current := data

	// 遍历到倒数第二层，确保路径存在
	for i := 0; i < len(fields)-1; i++ {
		field := fields[i]
		if next, exists := current[field]; exists {
			if nextMap, ok := next.(map[string]interface{}); ok {
				current = nextMap
			} else {
				// 如果存在但不是map，创建新的map覆盖
				newMap := make(map[string]interface{})
				current[field] = newMap
				current = newMap
			}
		} else {
			// 如果不存在，创建新的map
			newMap := make(map[string]interface{})
			current[field] = newMap
			current = newMap
		}
	}

	// 设置最终的值
	lastField := fields[len(fields)-1]
	current[lastField] = value
}

// IsNestedField 检查字段名是否包含点号或数组索引（嵌套字段）
func IsNestedField(fieldName string) bool {
	return strings.Contains(fieldName, ".") || strings.Contains(fieldName, "[")
}

// ExtractTopLevelField 从嵌套字段路径中提取顶级字段名
// 例如："device.info.name" 返回 "device"
//
//	"data[0].name" 返回 "data"
//	"a.b[0].c['key']" 返回 "a"
func ExtractTopLevelField(fieldPath string) string {
	if fieldPath == "" {
		return ""
	}

	// 找到第一个分隔符（点号或左括号）
	dotIndex := strings.Index(fieldPath, ".")
	bracketIndex := strings.Index(fieldPath, "[")

	// 取较早出现的分隔符位置
	firstSeparator := -1
	if dotIndex >= 0 && bracketIndex >= 0 {
		if dotIndex < bracketIndex {
			firstSeparator = dotIndex
		} else {
			firstSeparator = bracketIndex
		}
	} else if dotIndex >= 0 {
		firstSeparator = dotIndex
	} else if bracketIndex >= 0 {
		firstSeparator = bracketIndex
	}

	// 如果找到分隔符，返回分隔符之前的部分
	if firstSeparator > 0 {
		return fieldPath[:firstSeparator]
	}

	// 没有分隔符，整个路径就是顶级字段
	return fieldPath
}

// GetAllReferencedFields 获取嵌套字段路径中引用的所有顶级字段
// 例如：["device.info.name", "sensor.temperature", "data[0].value"] 返回 ["device", "sensor", "data"]
func GetAllReferencedFields(fieldPaths []string) []string {
	topLevelFields := make(map[string]bool)

	for _, path := range fieldPaths {
		if path != "" {
			topField := ExtractTopLevelField(path)
			if topField != "" {
				topLevelFields[topField] = true
			}
		}
	}

	result := make([]string, 0, len(topLevelFields))
	for field := range topLevelFields {
		result = append(result, field)
	}

	return result
}

// ValidateFieldPath 验证字段路径的格式是否正确
func ValidateFieldPath(fieldPath string) error {
	if fieldPath == "" {
		return &FieldAccessError{
			Path:    fieldPath,
			Message: "empty field path",
		}
	}

	_, err := ParseFieldPath(fieldPath)
	return err
}

// FieldAccessError 字段访问错误
type FieldAccessError struct {
	Path    string
	Message string
}

func (e *FieldAccessError) Error() string {
	return fmt.Sprintf("field access error for path '%s': %s", e.Path, e.Message)
}

// GetFieldPathDepth 获取字段路径的深度
func GetFieldPathDepth(fieldPath string) int {
	if fieldPath == "" {
		return 0
	}

	accessor, err := ParseFieldPath(fieldPath)
	if err != nil {
		// 回退到简单计算
		return len(strings.Split(fieldPath, "."))
	}

	if accessor == nil {
		return 0
	}

	return len(accessor.Parts)
}

// NormalizeFieldPath 规范化字段路径格式
func NormalizeFieldPath(fieldPath string) string {
	accessor, err := ParseFieldPath(fieldPath)
	if err != nil {
		return fieldPath // 如果解析失败，返回原路径
	}

	if accessor == nil || len(accessor.Parts) == 0 {
		return fieldPath
	}

	var result strings.Builder
	for i, part := range accessor.Parts {
		if i > 0 && part.Type == "field" {
			result.WriteString(".")
		}

		switch part.Type {
		case "field":
			result.WriteString(part.Name)
		case "array_index":
			result.WriteString("[")
			result.WriteString(strconv.Itoa(part.Index))
			result.WriteString("]")
		case "map_key":
			result.WriteString("[")
			if part.KeyType == "string" {
				result.WriteString("'")
				result.WriteString(part.Key)
				result.WriteString("'")
			} else {
				result.WriteString(part.Key)
			}
			result.WriteString("]")
		}
	}

	return result.String()
}
