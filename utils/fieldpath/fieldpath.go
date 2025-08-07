package fieldpath

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// FieldAccessor field accessor structure for parsing complex field paths
type FieldAccessor struct {
	Parts []FieldPart
}

// FieldPart represents a single part of field path
type FieldPart struct {
	Type    string // "field", "array_index", "map_key"
	Name    string // Field name or key name
	Index   int    // Array index (when Type is "array_index")
	Key     string // Map key (when Type is "map_key")
	KeyType string // Key type: "string", "number"
}

// Regular expressions for parsing complex field paths
var (
	// Match array index: [0], [1], [-1] etc
	arrayIndexRegex = regexp.MustCompile(`\[(-?\d+)\]`)
	// Match string keys: ["key"], ['key'] etc
	stringKeyRegex = regexp.MustCompile(`\[['"]([^'"]*)['"]\]`)
	// Match number keys: [123] etc (same as array index but in Map context)
	numberKeyRegex = regexp.MustCompile(`\[(\d+)\]`)
)

// ParseFieldPath parses field path, supports complex access like dot notation, array index, Map keys
// Supported formats:
// - a.b.c (nested fields)
// - a.b[0] (array index)
// - a.b[0].c (field of array element)
// - a.b["key"] (string key)
// - a.b['key'] (string key)
// - a.b[123] (number key or array index)
// - a[0].b[1].c["key"] (mixed access)
func ParseFieldPath(fieldPath string) (*FieldAccessor, error) {
	if fieldPath == "" {
		return nil, nil
	}

	accessor := &FieldAccessor{
		Parts: make([]FieldPart, 0),
	}

	// First handle basic path split by dots
	parts := strings.Split(fieldPath, ".")

	for _, part := range parts {
		if part == "" {
			continue
		}

		// Check if current part contains array index or Map key access
		if strings.Contains(part, "[") {
			// Handle complex part containing index/key
			err := parseComplexPart(part, accessor)
			if err != nil {
				return nil, err
			}
		} else {
			// Simple field name
			accessor.Parts = append(accessor.Parts, FieldPart{
				Type: "field",
				Name: part,
			})
		}
	}

	return accessor, nil
}

// parseComplexPart parses complex part containing index or key access
func parseComplexPart(part string, accessor *FieldAccessor) error {
	// Find position of first '['
	bracketIndex := strings.Index(part, "[")
	if bracketIndex == -1 {
		// No brackets, treat as normal field
		accessor.Parts = append(accessor.Parts, FieldPart{
			Type: "field",
			Name: part,
		})
		return nil
	}

	// If there's field name part, add field access first
	if bracketIndex > 0 {
		fieldName := part[:bracketIndex]
		accessor.Parts = append(accessor.Parts, FieldPart{
			Type: "field",
			Name: fieldName,
		})
	}

	// Parse remaining index/key access parts
	remaining := part[bracketIndex:]

	// Process all [xxx] parts sequentially
	for len(remaining) > 0 && strings.HasPrefix(remaining, "[") {
		// Find matching right bracket
		rightBracket := strings.Index(remaining, "]")
		if rightBracket == -1 {
			return &FieldAccessError{
				Path:    part,
				Message: "unmatched bracket in field path",
			}
		}

		// Extract content within brackets
		bracketContent := remaining[1:rightBracket]

		// Parse bracket content
		fieldPart, err := parseBracketContent(bracketContent)
		if err != nil {
			return err
		}

		accessor.Parts = append(accessor.Parts, fieldPart)

		// Move to next part
		remaining = remaining[rightBracket+1:]
	}

	return nil
}

// parseBracketContent parses content within brackets
func parseBracketContent(content string) (FieldPart, error) {
	content = strings.TrimSpace(content)

	// Check if it's a string key (with quotes)
	if (strings.HasPrefix(content, "'") && strings.HasSuffix(content, "'")) ||
		(strings.HasPrefix(content, "\"") && strings.HasSuffix(content, "\"")) {
		// String key
		key := content[1 : len(content)-1] // Remove quotes
		return FieldPart{
			Type:    "map_key",
			Key:     key,
			KeyType: "string",
		}, nil
	}

	// Check if it's a number
	if num, err := strconv.Atoi(content); err == nil {
		// Number, could be array index or number key
		return FieldPart{
			Type:    "array_index", // Default as array index, will adjust based on data type during actual use
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

// GetNestedField gets field value from nested map or struct
// Supports complex operations like dot-separated field paths, array indices, Map keys
// Supported formats:
// - "device.info.name" (nested fields)
// - "data[0]" (array index)
// - "users[0].name" (field of array element)
// - "config['key']" (string key)
// - "items[0][1]" (multi-dimensional array)
// - "nested.data[0].field['key']" (mixed access)
func GetNestedField(data interface{}, fieldPath string) (interface{}, bool) {
	if fieldPath == "" {
		return nil, false
	}

	// Parse field path
	accessor, err := ParseFieldPath(fieldPath)
	if err != nil {
		// If parsing fails, fallback to original simple dot access
		return getNestedFieldSimple(data, fieldPath)
	}

	if accessor == nil || len(accessor.Parts) == 0 {
		return nil, false
	}

	// Access step by step according to parsed path
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

// accessFieldPart accesses a single field part
func accessFieldPart(data interface{}, part FieldPart) (interface{}, bool) {
	if data == nil {
		return nil, false
	}

	v := reflect.ValueOf(data)

	// If it's a pointer, dereference
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

// getArrayElement gets array or slice element
func getArrayElement(v reflect.Value, index int) (interface{}, bool) {
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		length := v.Len()

		// Support negative index (from end)
		if index < 0 {
			index = length + index
		}

		if index < 0 || index >= length {
			return nil, false
		}

		elem := v.Index(index)
		return elem.Interface(), true

	case reflect.Map:
		// If data is Map, use index as key to access
		key := reflect.ValueOf(index)
		mapVal := v.MapIndex(key)
		if mapVal.IsValid() {
			return mapVal.Interface(), true
		}

		// Try string form of index
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

// getMapValue gets Map value
func getMapValue(v reflect.Value, key, keyType string) (interface{}, bool) {
	if v.Kind() != reflect.Map {
		return nil, false
	}

	// First try string key
	if keyType == "string" || v.Type().Key().Kind() == reflect.String {
		mapVal := v.MapIndex(reflect.ValueOf(key))
		if mapVal.IsValid() {
			return mapVal.Interface(), true
		}
	}

	// If it's a numeric key
	if keyType == "number" {
		if num, err := strconv.Atoi(key); err == nil {
			// Try int key
			mapVal := v.MapIndex(reflect.ValueOf(num))
			if mapVal.IsValid() {
				return mapVal.Interface(), true
			}

			// Try string form of numeric key
			mapVal = v.MapIndex(reflect.ValueOf(key))
			if mapVal.IsValid() {
				return mapVal.Interface(), true
			}
		}
	}

	return nil, false
}

// getNestedFieldSimple original simple dot access (backward compatible)
func getNestedFieldSimple(data interface{}, fieldPath string) (interface{}, bool) {
	if fieldPath == "" {
		return nil, false
	}

	// Split field path
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

// getFieldValue gets field value from single level
func getFieldValue(data interface{}, fieldName string) (interface{}, bool) {
	if data == nil {
		return nil, false
	}

	v := reflect.ValueOf(data)

	// If it's a pointer, dereference
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, false
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Map:
		// Handle map[string]interface{}
		if v.Type().Key().Kind() == reflect.String {
			mapVal := v.MapIndex(reflect.ValueOf(fieldName))
			if mapVal.IsValid() {
				return mapVal.Interface(), true
			}
		}
		return nil, false

	case reflect.Struct:
		// Handle struct
		fieldVal := v.FieldByName(fieldName)
		if fieldVal.IsValid() {
			return fieldVal.Interface(), true
		}
		return nil, false

	default:
		return nil, false
	}
}

// SetNestedField sets field value in nested map, supports complex paths
// Automatically creates missing levels in the path
func SetNestedField(data map[string]interface{}, fieldPath string, value interface{}) error {
	if fieldPath == "" {
		return &FieldAccessError{
			Path:    fieldPath,
			Message: "empty field path",
		}
	}

	// Parse field path
	accessor, err := ParseFieldPath(fieldPath)
	if err != nil {
		// If parsing fails, fallback to original simple setting
		setNestedFieldSimple(data, fieldPath, value)
		return nil
	}

	if accessor == nil || len(accessor.Parts) == 0 {
		return &FieldAccessError{
			Path:    fieldPath,
			Message: "invalid field path",
		}
	}

	// Create path level by level and set value
	current := data

	// Handle all parts except the last one
	for i := 0; i < len(accessor.Parts)-1; i++ {
		part := accessor.Parts[i]

		if part.Type != "field" {
			return &FieldAccessError{
				Path:    fieldPath,
				Message: "complex path setting only supports field access in intermediate parts",
			}
		}

		// Ensure intermediate path exists
		if next, exists := current[part.Name]; exists {
			if nextMap, ok := next.(map[string]interface{}); ok {
				current = nextMap
			} else {
				// If exists but not a map, create new map to override
				newMap := make(map[string]interface{})
				current[part.Name] = newMap
				current = newMap
			}
		} else {
			// If not exists, create new map
			newMap := make(map[string]interface{})
			current[part.Name] = newMap
			current = newMap
		}
	}

	// Handle the last part
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

// setNestedFieldSimple original simple setting (backward compatible)
func setNestedFieldSimple(data map[string]interface{}, fieldPath string, value interface{}) {
	if fieldPath == "" {
		return
	}

	fields := strings.Split(fieldPath, ".")
	current := data

	// Traverse to second-to-last level, ensure path exists
	for i := 0; i < len(fields)-1; i++ {
		field := fields[i]
		if next, exists := current[field]; exists {
			if nextMap, ok := next.(map[string]interface{}); ok {
				current = nextMap
			} else {
				// If exists but not a map, create new map to override
				newMap := make(map[string]interface{})
				current[field] = newMap
				current = newMap
			}
		} else {
			// If not exists, create new map
			newMap := make(map[string]interface{})
			current[field] = newMap
			current = newMap
		}
	}

	// Set final value
	lastField := fields[len(fields)-1]
	current[lastField] = value
}

// IsNestedField checks if field name contains dots or array indices (nested field)
func IsNestedField(fieldName string) bool {
	return strings.Contains(fieldName, ".") || strings.Contains(fieldName, "[")
}

// ExtractTopLevelField extracts top-level field name from nested field path
// Examples: "device.info.name" returns "device"
//	"data[0].name" returns "data"
//	"a.b[0].c['key']" returns "a"
func ExtractTopLevelField(fieldPath string) string {
	if fieldPath == "" {
		return ""
	}

	// Find first separator (dot or left bracket)
	dotIndex := strings.Index(fieldPath, ".")
	bracketIndex := strings.Index(fieldPath, "[")

	// Take the earlier separator position
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

	// If separator found, return part before separator
	if firstSeparator > 0 {
		return fieldPath[:firstSeparator]
	}

	// No separator, entire path is top-level field
	return fieldPath
}

// GetAllReferencedFields gets all top-level fields referenced in nested field paths
// Example: ["device.info.name", "sensor.temperature", "data[0].value"] returns ["device", "sensor", "data"]
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

// ValidateFieldPath validates if field path format is correct
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

// FieldAccessError field access error
type FieldAccessError struct {
	Path    string
	Message string
}

func (e *FieldAccessError) Error() string {
	return fmt.Sprintf("field access error for path '%s': %s", e.Path, e.Message)
}

// GetFieldPathDepth gets the depth of field path
func GetFieldPathDepth(fieldPath string) int {
	if fieldPath == "" {
		return 0
	}

	accessor, err := ParseFieldPath(fieldPath)
	if err != nil {
		// Fallback to simple calculation
		return len(strings.Split(fieldPath, "."))
	}

	if accessor == nil {
		return 0
	}

	return len(accessor.Parts)
}

// NormalizeFieldPath normalizes field path format
func NormalizeFieldPath(fieldPath string) string {
	accessor, err := ParseFieldPath(fieldPath)
	if err != nil {
		return fieldPath // If parsing fails, return original path
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
