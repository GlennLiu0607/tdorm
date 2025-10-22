/*
 * @Author: GlennLiu <glennliu0607@gmail.com>
 * @Date: 2025-10-22 15:49:56
 * @LastEditors: Glenn glennliu0607@gmail.com
 * @LastEditTime: 2025-10-22 15:50:28
 * @FilePath: \query_helpers.go
 * @Description:
 *
 * Copyright (c) 2025 by 天津晟源士兴科技有限公司, All Rights Reserved.
 */
package tdorm

import (
	"fmt"
	"strings"
	"time"
)

// buildIntervalFill 生成 TDengine 的 INTERVAL 与 FILL 子句
// 示例：INTERVAL(1m) FILL(linear)
func buildIntervalFill(interval time.Duration, fill string) string {
	parts := []string{}
	if interval > 0 {
		// 以秒为单位，拼接成如 60s / 5m
		sec := int64(interval / time.Second)
		parts = append(parts, fmt.Sprintf("INTERVAL(%ds)", sec))
	}
	fill = strings.TrimSpace(strings.ToLower(fill))
	if fill != "" {
		// 常见：none, null, prev, linear
		parts = append(parts, fmt.Sprintf("FILL(%s)", fill))
	}
	if len(parts) == 0 {
		return ""
	}
	return " " + strings.Join(parts, " ")
}

// buildGroupByTags 生成 TAGS 分组
func buildGroupByTags(tags []string) (string, error) {
	if len(tags) == 0 {
		return "", nil
	}
	parts := make([]string, 0, len(tags))
	for _, t := range tags {
		id, err := sanitizeIdent(t)
		if err != nil {
			return "", err
		}
		parts = append(parts, id)
	}
	return " GROUP BY " + strings.Join(parts, ", "), nil
}
