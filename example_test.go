package tdorm

import (
	"fmt"
	"time"
)

// Example 展示连接、建表、子表、插入与查询的常见用法。
// 注意：该示例假设 TDengine REST 服务运行在 127.0.0.1:6041，
// 如需运行示例，请先确保服务可用，并修改 DSN 为你的环境。
func Example() {
	dsn := "root:pass@http(127.0.0.1:6041)/demo"
	cli, _ := NewClient(dsn)
	defer cli.Close()

	// 创建超级表
	cols := []ColumnDef{{Name: "current", Type: "FLOAT"}, {Name: "voltage", Type: "INT"}}
	tags := []ColumnDef{{Name: "location", Type: "NCHAR(64)"}, {Name: "device_id", Type: "NCHAR(64)"}}
	_ = cli.CreateStable("meters", cols, tags)

	// 创建子表并写入一行
	_ = cli.EnsureSubTable("meter001", "meters", []interface{}{"roomA", "dev-001"})
	_ = cli.Insert("meter001", map[string]interface{}{"current": 12.3, "voltage": 220})

	// 查询最近 10 分钟数据
	f := Filter{Conj: "AND", Conditions: []Condition{
		{Column: "ts", Op: ">=", Value: time.Now().Add(-10 * time.Minute)},
	}, OrderBy: "ts", Limit: 10}
	rows, _ := cli.Query("meter001", []string{"ts", "current", "voltage"}, f)
	_ = rows

	// 不校验输出，仅展示用法
	// Output:
}

// ExampleFilter 展示如何构造筛选条件并生成 WHERE/ORDER 片段。
func ExampleFilter() {
	f := Filter{
		Conj: "AND",
		Conditions: []Condition{
			{Column: "voltage", Op: ">=", Value: 200},
			{Column: "location", Op: "IN", Value: []interface{}{"roomA", "roomB"}},
		},
		OrderBy: "ts",
		Limit:   5,
	}
	where, _ := f.buildWhere()
	post, _ := f.buildOrderLimit()
	fmt.Println(where)
	fmt.Println(post)
	// Output:
	// WHERE voltage >= 200 AND location IN ('roomA', 'roomB')
	//  ORDER BY ts LIMIT 5
}
