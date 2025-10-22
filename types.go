/*
 * @Author: GlennLiu <glennliu0607@gmail.com>
 * @Date: 2025-10-22 15:52:10
 * @LastEditors: Glenn glennliu0607@gmail.com
 * @LastEditTime: 2025-10-22 15:57:17
 * @FilePath: \test.go
 * @Description:
 *
 * Copyright (c) 2025 by 天津晟源士兴科技有限公司, All Rights Reserved.
 */
package tdorm

import (
	"fmt"
	"time"
)

// ColumnDef 定义字段
// Type 示例："INT", "FLOAT", "NCHAR(255)", "BINARY(64)" 等
// 注意：TDengine 会强制存在 ts TIMESTAMP 字段，ORM 会自动添加
type ColumnDef struct {
	Name string
	Type string
}

// sanitizeIdent 检查并约束标识符，只允许字母、数字、下划线
func sanitizeIdent(id string) (string, error) {
	for _, r := range id {
		if !(r == '_' || (r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z')) {
			return "", fmt.Errorf("非法标识符: %s", id)
		}
	}
	if id == "" {
		return "", fmt.Errorf("标识符不能为空")
	}
	return id, nil
}

// formatValue 将 Go 值格式化为 TDengine SQL 值
func formatValue(v interface{}) (string, error) {
	switch val := v.(type) {
	case nil:
		return "NULL", nil
	case string:
		// 转义单引号
		esc := ""
		for _, ch := range val {
			if ch == '\'' {
				esc += "''"
			} else {
				esc += string(ch)
			}
		}
		return fmt.Sprintf("'%s'", esc), nil
	case time.Time:
		return fmt.Sprintf("'%s'", val.Format("2006-01-02 15:04:05.000")), nil
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val), nil
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val), nil
	case float32, float64:
		return fmt.Sprintf("%v", val), nil
	case bool:
		if val {
			return "1", nil
		}
		return "0", nil
	default:
		return "", fmt.Errorf("不支持的值类型: %T", v)
	}
}
