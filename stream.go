/*
 * @Author: GlennLiu <glennliu0607@gmail.com>
 * @Date: 2026-01-22
 * @Description: TDengine Stream Computation Support
 *
 * Copyright (c) 2026 by 天津晟源士兴科技有限公司, All Rights Reserved.
 */
package tdorm

import (
	"fmt"
	"strings"
	"time"
)

// StreamDef 定义流计算任务配置
type StreamDef struct {
	Name          string        // 流任务名称
	TargetTable   string        // 写入的目标表 (INTO table_name)
	SubQuery      string        // 来源查询语句 (AS SELECT ...)
	IfNotExists   bool          // IF NOT EXISTS
	Trigger       string        // 触发模式，例如 "AT_ONCE", "WINDOW_CLOSE", "MAX_DELAY 5s"
	Watermark     time.Duration // 水位线延迟，例如 10*time.Second
	OtherOptions  []string      // 其他选项，例如 "IGNORE DISORDER", "DELETE_RECALC"
}

// CreateStream 创建流计算任务
// 语法参考：CREATE STREAM [IF NOT EXISTS] stream_name [options] INTO table_name AS subquery
func (c *Client) CreateStream(def StreamDef) error {
	if def.Name == "" {
		return fmt.Errorf("stream name cannot be empty")
	}
	if def.SubQuery == "" {
		return fmt.Errorf("subquery cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString("CREATE STREAM ")
	if def.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	
	name, err := sanitizeIdent(def.Name)
	if err != nil {
		return err
	}
	sb.WriteString(name)
	sb.WriteString(" ")

	// 构建选项
	if def.Trigger != "" {
		sb.WriteString(fmt.Sprintf("TRIGGER %s ", def.Trigger))
	}
	if def.Watermark > 0 {
		// TDengine 时间格式通常为 10s, 500ms 等
		// 这里简单处理：如果整秒则用s，否则ms
		ms := def.Watermark.Milliseconds()
		if ms%1000 == 0 {
			sb.WriteString(fmt.Sprintf("WATERMARK %ds ", ms/1000))
		} else {
			sb.WriteString(fmt.Sprintf("WATERMARK %dms ", ms))
		}
	}
	
	for _, opt := range def.OtherOptions {
		sb.WriteString(opt + " ")
	}

	if def.TargetTable != "" {
		target, err := sanitizeIdent(def.TargetTable)
		if err != nil {
			return err
		}
		sb.WriteString("INTO " + target + " ")
	}

	sb.WriteString("AS " + def.SubQuery)

	_, err = c.DB.Exec(sb.String())
	return err
}

// DropStream 删除流计算任务
func (c *Client) DropStream(streamName string, ifExists bool) error {
	name, err := sanitizeIdent(streamName)
	if err != nil {
		return err
	}
	sql := "DROP STREAM "
	if ifExists {
		sql += "IF EXISTS "
	}
	sql += name
	_, err = c.DB.Exec(sql)
	return err
}

// CreateStreamMsg 创建流计算并返回提示
func (c *Client) CreateStreamMsg(def StreamDef) (string, error) {
	if err := c.CreateStream(def); err != nil {
		return "", fmt.Errorf("CreateStream %s failed: %w", def.Name, err)
	}
	return fmt.Sprintf("流计算任务已创建: %s", def.Name), nil
}

// DropStreamMsg 删除流计算并返回提示
func (c *Client) DropStreamMsg(streamName string) (string, error) {
	if err := c.DropStream(streamName, true); err != nil {
		return "", fmt.Errorf("DropStream %s failed: %w", streamName, err)
	}
	return fmt.Sprintf("流计算任务已删除: %s", streamName), nil
}