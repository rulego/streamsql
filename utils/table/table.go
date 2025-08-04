/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package table

import (
	"fmt"
)

// PrintTableFromSlice prints table from slice data
// Supports custom field order, uses alphabetical order if fieldOrder is empty
func PrintTableFromSlice(data []map[string]interface{}, fieldOrder []string) {
	if len(data) == 0 {
		return
	}

	// Collect all column names
	columnSet := make(map[string]bool)
	for _, row := range data {
		for col := range row {
			columnSet[col] = true
		}
	}

	// Arrange column names according to field order
	var columns []string
	if len(fieldOrder) > 0 {
		// Use specified field order
		for _, field := range fieldOrder {
			if columnSet[field] {
				columns = append(columns, field)
				delete(columnSet, field) // Mark as processed
			}
		}
		// Add remaining columns (if any)
		for col := range columnSet {
			columns = append(columns, col)
		}
	} else {
		// If no field order specified, use simple sorting
		columns = make([]string, 0, len(columnSet))
		for col := range columnSet {
			columns = append(columns, col)
		}
		// Simple sorting to ensure consistent output
		for i := 0; i < len(columns)-1; i++ {
			for j := i + 1; j < len(columns); j++ {
				if columns[i] > columns[j] {
					columns[i], columns[j] = columns[j], columns[i]
				}
			}
		}
	}

	// Calculate maximum width for each column
	colWidths := make([]int, len(columns))
	for i, col := range columns {
		colWidths[i] = len(col) // Column name length
		for _, row := range data {
			if val, exists := row[col]; exists {
				valStr := fmt.Sprintf("%v", val)
				if len(valStr) > colWidths[i] {
					colWidths[i] = len(valStr)
				}
			}
		}
		// Minimum width is 4
		if colWidths[i] < 4 {
			colWidths[i] = 4
		}
	}

	// Print top border
	PrintTableBorder(colWidths)

	// Print column names
	fmt.Print("|")
	for i, col := range columns {
		fmt.Printf(" %-*s |", colWidths[i], col)
	}
	fmt.Println()

	// Print separator line
	PrintTableBorder(colWidths)

	// Print data rows
	for _, row := range data {
		fmt.Print("|")
		for i, col := range columns {
			val := ""
			if v, exists := row[col]; exists {
				val = fmt.Sprintf("%v", v)
			}
			fmt.Printf(" %-*s |", colWidths[i], val)
		}
		fmt.Println()
	}

	// Print bottom border
	PrintTableBorder(colWidths)

	// Print row count statistics
	fmt.Printf("(%d rows)\n", len(data))
}

// PrintTableBorder prints table border
func PrintTableBorder(columnWidths []int) {
	fmt.Print("+")
	for _, width := range columnWidths {
		for i := 0; i < width+2; i++ {
			fmt.Print("-")
		}
		fmt.Print("+")
	}
	fmt.Println()
}

// FormatTableData formats table data, supports multiple data types
func FormatTableData(result interface{}, fieldOrder []string) {
	switch v := result.(type) {
	case []map[string]interface{}:
		if len(v) == 0 {
			fmt.Println("(0 rows)")
			return
		}
		PrintTableFromSlice(v, fieldOrder)
	case map[string]interface{}:
		if len(v) == 0 {
			fmt.Println("(0 rows)")
			return
		}
		PrintTableFromSlice([]map[string]interface{}{v}, fieldOrder)
	default:
		// For non-table data, print directly
		fmt.Printf("Result: %v\n", result)
	}
}