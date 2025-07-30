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

// PrintTableFromSlice 从切片数据打印表格
// 支持自定义字段顺序，如果fieldOrder为空则使用字母排序
func PrintTableFromSlice(data []map[string]interface{}, fieldOrder []string) {
	if len(data) == 0 {
		return
	}

	// 收集所有列名
	columnSet := make(map[string]bool)
	for _, row := range data {
		for col := range row {
			columnSet[col] = true
		}
	}

	// 根据字段顺序排列列名
	var columns []string
	if len(fieldOrder) > 0 {
		// 使用指定的字段顺序
		for _, field := range fieldOrder {
			if columnSet[field] {
				columns = append(columns, field)
				delete(columnSet, field) // 标记已处理
			}
		}
		// 添加剩余的列（如果有的话）
		for col := range columnSet {
			columns = append(columns, col)
		}
	} else {
		// 如果没有指定字段顺序，使用简单排序
		columns = make([]string, 0, len(columnSet))
		for col := range columnSet {
			columns = append(columns, col)
		}
		// 简单排序，确保输出一致性
		for i := 0; i < len(columns)-1; i++ {
			for j := i + 1; j < len(columns); j++ {
				if columns[i] > columns[j] {
					columns[i], columns[j] = columns[j], columns[i]
				}
			}
		}
	}

	// 计算每列的最大宽度
	colWidths := make([]int, len(columns))
	for i, col := range columns {
		colWidths[i] = len(col) // 列名长度
		for _, row := range data {
			if val, exists := row[col]; exists {
				valStr := fmt.Sprintf("%v", val)
				if len(valStr) > colWidths[i] {
					colWidths[i] = len(valStr)
				}
			}
		}
		// 最小宽度为4
		if colWidths[i] < 4 {
			colWidths[i] = 4
		}
	}

	// 打印顶部边框
	PrintTableBorder(colWidths)

	// 打印列名
	fmt.Print("|")
	for i, col := range columns {
		fmt.Printf(" %-*s |", colWidths[i], col)
	}
	fmt.Println()

	// 打印分隔线
	PrintTableBorder(colWidths)

	// 打印数据行
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

	// 打印底部边框
	PrintTableBorder(colWidths)

	// 打印行数统计
	fmt.Printf("(%d rows)\n", len(data))
}

// PrintTableBorder 打印表格边框
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

// FormatTableData 格式化表格数据，支持多种数据类型
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
		// 对于非表格数据，直接打印
		fmt.Printf("Result: %v\n", result)
	}
}