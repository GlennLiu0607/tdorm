/*
 * @Author: GlennLiu <glennliu0607@gmail.com>
 * @Date: 2025-10-22 15:35:26
 * @LastEditors: Glenn glennliu0607@gmail.com
 * @LastEditTime: 2025-10-22 16:12:06
 * @FilePath: \filter.go
 * @Description:
 *
 * Copyright (c) 2025 by 天津晟源士兴科技有限公司, All Rights Reserved.
 */
package tdorm

import (
	"fmt"
	"strings"
)

// Condition 单个筛选条件，如 column op value
// Op 示例：=, >, >=, <, <=, <> , LIKE, BETWEEN, IN
// 注意：REST 连接不一定支持参数绑定，需谨慎构造 SQL
// ORM 通过列名白名单和简单转义降低风险
type Condition struct {
	Column string
	Op     string
	Value  interface{}
}

// Filter 组合筛选
// Conj 为 AND 或 OR
type Filter struct {
	Conditions []Condition
	Conj       string
	OrderBy    string
	Desc       bool
	Limit      int
}

func (f Filter) buildWhere() (string, error) {
	if len(f.Conditions) == 0 {
		return "", nil
	}
	conj := strings.ToUpper(strings.TrimSpace(f.Conj))
	if conj != "AND" && conj != "OR" {
		conj = "AND"
	}
	parts := make([]string, 0, len(f.Conditions))
	for _, c := range f.Conditions {
		col, err := sanitizeIdent(c.Column)
		if err != nil {
			return "", err
		}
		op := strings.ToUpper(strings.TrimSpace(c.Op))
		if op == "IN" {
			arr, ok := c.Value.([]interface{})
			if !ok {
				return "", fmt.Errorf("IN 需要 []interface{} 值")
			}
			vals := make([]string, 0, len(arr))
			for _, v := range arr {
				fv, err := formatValue(v)
				if err != nil {
					return "", err
				}
				vals = append(vals, fv)
			}
			parts = append(parts, fmt.Sprintf("%s IN (%s)", col, strings.Join(vals, ", ")))
			continue
		}
		if op == "BETWEEN" {
			rng, ok := c.Value.([2]interface{})
			if !ok {
				return "", fmt.Errorf("BETWEEN 需要 [2]interface{}")
			}
			l, err := formatValue(rng[0])
			if err != nil {
				return "", err
			}
			r, err := formatValue(rng[1])
			if err != nil {
				return "", err
			}
			parts = append(parts, fmt.Sprintf("%s BETWEEN %s AND %s", col, l, r))
			continue
		}
		fv, err := formatValue(c.Value)
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf("%s %s %s", col, op, fv))
	}
	return "WHERE " + strings.Join(parts, " "+conj+" "), nil
}

func (f Filter) buildOrderLimit() (string, error) {
	sb := &strings.Builder{}
	if f.OrderBy != "" {
		col, err := sanitizeIdent(f.OrderBy)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(sb, " ORDER BY %s", col)
		if f.Desc {
			sb.WriteString(" DESC")
		}
	}
	if f.Limit > 0 {
		fmt.Fprintf(sb, " LIMIT %d", f.Limit)
	}
	return sb.String(), nil
}
