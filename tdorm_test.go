/*
 * @Author: GlennLiu <glennliu0607@gmail.com>
 * @Date: 2025-10-22 15:22:18
 * @LastEditors: Glenn glennliu0607@gmail.com
 * @LastEditTime: 2025-10-22 15:57:17
 * @FilePath: \test.go
 * @Description:
 *
 * Copyright (c) 2025 by 天津晟源士兴科技有限公司, All Rights Reserved.
 */
package tdorm

import (
	"os"
	"testing"
	"time"
)

func TestSanitizeIdent(t *testing.T) {
	if v, err := sanitizeIdent("abc_123"); err != nil || v != "abc_123" {
		t.Fatalf("expected valid ident, got %v err=%v", v, err)
	}
	if _, err := sanitizeIdent(""); err == nil {
		t.Fatalf("expected error for empty ident")
	}
	if _, err := sanitizeIdent("abc-123"); err == nil {
		t.Fatalf("expected error for hyphen ident")
	}
}

func TestFormatValue(t *testing.T) {
	if v, _ := formatValue("a'b"); v != "'a''b'" {
		t.Fatalf("string escape failed: %s", v)
	}
	if v, _ := formatValue(42); v != "42" {
		t.Fatalf("int format failed: %s", v)
	}
	if v, _ := formatValue(3.14); v != "3.14" {
		t.Fatalf("float format failed: %s", v)
	}
	if v, _ := formatValue(true); v != "1" {
		t.Fatalf("bool(true) format failed: %s", v)
	}
	if v, _ := formatValue(false); v != "0" {
		t.Fatalf("bool(false) format failed: %s", v)
	}
	if v, _ := formatValue(time.Date(2024, 10, 1, 12, 30, 0, 0, time.UTC)); v != "'2024-10-01 12:30:00.000'" {
		t.Fatalf("time format failed: %s", v)
	}
	if _, err := formatValue(struct{}{}); err == nil {
		t.Fatalf("expected error for unsupported type")
	}
}

func TestFilterBuildWhere_InBetweenOrderLimit(t *testing.T) {
	// IN
	f := Filter{Conditions: []Condition{{Column: "voltage", Op: "IN", Value: []interface{}{218, 219}}}, Conj: "AND"}
	where, err := f.buildWhere()
	if err != nil {
		t.Fatalf("buildWhere error: %v", err)
	}
	if where != "WHERE voltage IN (218, 219)" {
		t.Fatalf("unexpected where: %s", where)
	}

	// BETWEEN with time
	t1 := time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 10, 2, 0, 0, 0, 0, time.UTC)
	f2 := Filter{Conditions: []Condition{{Column: "ts", Op: "BETWEEN", Value: [2]interface{}{t1, t2}}}}
	where2, err := f2.buildWhere()
	if err != nil {
		t.Fatalf("buildWhere error: %v", err)
	}
	expectedLeft := "WHERE ts BETWEEN '2024-10-01 00:00:00.000' AND '2024-10-02 00:00:00.000'"
	if where2 != expectedLeft {
		t.Fatalf("unexpected between where: %s", where2)
	}

	// ORDER BY + LIMIT
	f3 := Filter{OrderBy: "ts", Desc: true, Limit: 10}
	post, err := f3.buildOrderLimit()
	if err != nil {
		t.Fatalf("buildOrderLimit error: %v", err)
	}
	if post != " ORDER BY ts DESC LIMIT 10" {
		t.Fatalf("unexpected order/limit: %s", post)
	}
}

// 可选的集成测试：设置 TAOS_DSN 环境变量为 REST DSN，才会运行
// 示例：root:taosdata@http(127.0.0.1:6041)/
func TestClientLifecycle_Optional(t *testing.T) {
	dsn := os.Getenv("TAOS_DSN")
	if dsn == "" {
		t.Skip("skip integration test: TAOS_DSN not set")
	}
	client, err := NewClient(dsn)
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}
	defer client.Close()

	db := "tdorm_test"
	if err := client.CreateDatabaseIfNotExists(db); err != nil {
		t.Fatalf("create db: %v", err)
	}
	if err := client.UseDatabase(db); err != nil {
		t.Fatalf("use db: %v", err)
	}

	cols := []ColumnDef{{Name: "current", Type: "FLOAT"}, {Name: "voltage", Type: "INT"}, {Name: "phase", Type: "FLOAT"}}
	tags := []ColumnDef{{Name: "location", Type: "BINARY(64)"}, {Name: "groupId", Type: "INT"}}
	if err := client.CreateStable("meters", cols, tags); err != nil {
		t.Fatalf("create stable: %v", err)
	}
	if err := client.EnsureSubTable("d1001", "meters", []interface{}{"California.SanFrancisco", 2}); err != nil {
		t.Fatalf("ensure sub: %v", err)
	}

	if err := client.Insert("d1001", map[string]interface{}{"current": 10.3, "voltage": 219, "phase": 0.31}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// 测试：增加列
	newCol := ColumnDef{Name: "humidity", Type: "FLOAT"}
	if msg, err := client.AddColumnToStableMsg("meters", newCol); err != nil {
		t.Fatalf("add column: %v", err)
	} else {
		t.Log(msg)
	}
	// 验证插入新列数据
	if err := client.Insert("d1001", map[string]interface{}{"current": 10.5, "humidity": 45.2}); err != nil {
		t.Fatalf("insert with new column: %v", err)
	}

	// 测试：获取超级表列名
	colsList, msg, err := client.GetStableColumnsMsg("meters")
	if err != nil {
		t.Fatalf("get stable columns: %v", err)
	}
	t.Log(msg, colsList)
	// 简单验证是否包含 ts, current, voltage, phase, location, groupId, humidity
	foundHumidity := false
	for _, c := range colsList {
		if c == "humidity" {
			foundHumidity = true
			break
		}
	}
	if !foundHumidity {
		t.Fatalf("expected 'humidity' column in stable schema, got: %v", colsList)
	}

	f := Filter{Conditions: []Condition{{Column: "voltage", Op: ">=", Value: 218}}, Conj: "AND", OrderBy: "ts", Desc: true, Limit: 5}
	rows, err := client.Query("d1001", []string{"ts", "current", "voltage", "phase", "humidity"}, f)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) == 0 {
		t.Fatalf("expected at least one row after insert")
	}

	// 测试：流计算
	streamDef := StreamDef{
		Name:         "avg_current_1m",
		TargetTable:  "meters_avg_1m",
		SubQuery:     "SELECT avg(current) FROM meters PARTITION BY location INTERVAL(1m)",
		IfNotExists:  true,
		Trigger:      "AT_ONCE",
		Watermark:    10 * time.Second,
		OtherOptions: []string{"IGNORE DISORDER"},
	}
	if msg, err := client.CreateStreamMsg(streamDef); err != nil {
		t.Fatalf("create stream: %v", err)
	} else {
		t.Log(msg)
	}

	// 简单验证流存在性 (CreateStream 是幂等的 IF NOT EXISTS)
	if err := client.CreateStream(streamDef); err != nil {
		t.Fatalf("create stream idempotency: %v", err)
	}

	// 删除流
	if msg, err := client.DropStreamMsg("avg_current_1m"); err != nil {
		t.Fatalf("drop stream: %v", err)
	} else {
		t.Log(msg)
	}
}
