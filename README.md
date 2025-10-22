# tdorm: TDengine REST ORM

[![GitHub Repo](https://img.shields.io/badge/GitHub-tdorm-black?logo=github)](https://github.com/GlennLiu0607/tdorm)

源码仓库：`https://github.com/GlennLiu0607/tdorm.git`

## 安装
```
go get github.com/GlennLiu0607/tdorm@latest
```
- 仓库地址：`https://github.com/GlennLiu0607/tdorm.git`
- Go 导入路径：`github.com/GlennLiu0607/tdorm`（与 `go.mod` 保持一致）

一个轻量 Go 封装，基于 `taosRestful` 驱动，提供：
- 连接与数据库切换
- 超级表/子表管理
- 写入数据（单行与批量）
- 通用查询与筛选构建
- 多表聚合与按 TAGS 分组
- 时间降采样与插值（INTERVAL + FILL）
- 异步查询（返回通道与取消函数）
- 连续查询（CQ）创建/删除封装
- 订阅（基于轮询的增量拉取）

## 快速开始
```go
dsn := "root:pass@http(127.0.0.1:6041)/"
cli, _ := tdorm.NewClient(dsn)
defer cli.Close()

_ = cli.CreateDatabaseIfNotExists("powerdb")
_ = cli.UseDatabase("powerdb")

// 超级表
cols := []tdorm.ColumnDef{{Name:"current", Type:"FLOAT"}, {Name:"voltage", Type:"INT"}}
tags := []tdorm.ColumnDef{{Name:"location", Type:"NCHAR(64)"}, {Name:"device_id", Type:"NCHAR(64)"}}
_ = cli.CreateStable("meters", cols, tags)

// 子表
_ = cli.EnsureSubTable("meter001", "meters", []interface{}{"roomA", "dev-001"})

// 写入数据（单行）
_ = cli.Insert("meter001", map[string]interface{}{"current": 12.3, "voltage": 220})

// 写入数据（批量）
_ = cli.BatchInsert("meter001", []map[string]interface{}{
    {"ts": time.Now().Add(-time.Minute), "current": 11.8, "voltage": 220},
    {"current": 12.1, "voltage": 221}, // 未提供 ts 将使用 NOW()
})
```

## 提示信息封装（Msg 方法）

为方便 UI 与日志展示，新增一组带提示字符串的包装方法（以 `Msg` 结尾）。这些方法在调用原始 API 成功时返回友好提示，在失败时返回包含上下文的错误。

- `CreateDatabaseIfNotExistsMsg(db)`: 创建数据库并提示
- `UseDatabaseMsg(db)`: 切库提示（REST 下建议改用 DSN 指定库）
- `CreateStableMsg(stable, columns, tagColumns)`: 创建超级表提示
- `EnsureSubTableMsg(sub, stable, tagValues)`: 创建子表提示
- `InsertMsg(table, row)`: 单行插入提示
- `BatchInsertMsg(table, rows)`: 批量插入提示
- `QueryMsg(table, cols, filter)`: 查询返回数据与提示
- `UpdateMsg(table, set, filter)`: 更新返回影响行数与提示
- `DeleteMsg(table, filter)`: 删除返回影响行数与提示
- `QueryAggregateAcrossStableMsg(stable, aggExpr, filter, groupTags, interval, fill)`: 跨超级表聚合提示
- `QueryDownsampleWithFillMsg(name, selectExpr, filter, interval, fill)`: 降采样插值提示
- `CreateContinuousQueryMsg(sql)`: 创建连续查询提示
- `DropContinuousQueryMsg(sql)`: 删除连续查询提示

原有方法保持不变，`Msg` 方法只是对其进行成功/失败信息包装。

## 使用示例（片段）

```go
c, _ := tdorm.NewClient("root:pass@http(127.0.0.1:6041)/demo")
msg, err := c.CreateStableMsg("meters", []tdorm.ColumnDef{{Name:"current", Type:"FLOAT"}}, []tdorm.ColumnDef{{Name:"location", Type:"VARCHAR(64)"}})
fmt.Println(msg, err)

rows, tip, err := c.QueryMsg("d01", []string{"ts", "current"}, tdorm.Filter{Conj:"AND", OrderBy:"ts", Limit:10})
fmt.Println(tip, err)
printRows(rows)
```

## 筛选与查询
```go
f := tdorm.Filter{
    Conj: "AND",
    Conditions: []tdorm.Condition{
        {Column:"ts", Op:">=", Value: time.Now().Add(-time.Hour)},
        {Column:"voltage", Op:">", Value: 200},
    },
    OrderBy: "ts DESC",
    Limit: 100,
}
rows, _ := cli.Query("meter001", []string{"ts","current","voltage"}, f)
```

## 多表聚合与 TAGS 分组
对超级表做聚合，可选：WHERE、`GROUP BY TAGS(tag1, tag2)`、`INTERVAL` 与 `FILL`。
```go
// 最近 1 小时内，按 location 分组统计 avg(current)，每 10s 降采样并线性插值
f := tdorm.Filter{Conj:"AND", Conditions: []tdorm.Condition{
    {Column:"ts", Op:">=", Value: time.Now().Add(-time.Hour)},
}}
rows, _ := cli.QueryAggregateAcrossStable(
    "meters",      // 超级表
    "avg(current)",
    f,
    []string{"location"},     // GROUP BY TAGS
    10*time.Second,            // INTERVAL
    "LINEAR"                   // FILL 可选 NONE | PREV | NULL | LINEAR
)
```

## 降采样与插值（单表或超级表）
```go
f := tdorm.Filter{Conj:"AND", Conditions: []tdorm.Condition{
    {Column:"ts", Op:">=", Value: time.Now().Add(-30 * time.Minute)},
}}
rows, _ := cli.QueryDownsampleWithFill(
    "meter001",         // 子表或超级表
    "avg(current)",
    f,
    5*time.Second,
    "PREV",
)
```

## 异步查询
返回结果通道、错误通道与取消函数：
```go
resCh, errCh, cancel := cli.AsyncQuery("SELECT ts, current FROM meter001 WHERE ts >= now - 1h ORDER BY ts")
go func(){
    for rows := range resCh { fmt.Println("rows:", len(rows)) }
}()
if time.Since(start) > 2*time.Second { cancel() } // 外部取消
if err := <-errCh; err != nil { fmt.Println("async error:", err) }
```

## 连续查询（CQ）
不同 TDengine 版本 CQ 语法略有差异，请以实际版本为准。
```go
// 示例：按分钟生成降采样表（语法仅示意）
sqlCQ := `CREATE TABLE meter001_min AS SELECT FIRST(ts) AS ts, AVG(current) AS current FROM meter001 INTERVAL(1m) FILL(PREV)`
_ = cli.CreateContinuousQuery(sqlCQ)
// 删除 CQ（亦为完整 SQL，根据版本语法调整）
_ = cli.DropContinuousQuery("DROP TABLE meter001_min")
```

## 订阅（轮询增量拉取）
REST 模式下没有原生订阅接口，可使用轮询增量拉取实现近实时订阅。
```go
onData := func(rows []map[string]interface{}){ fmt.Println("batch:", len(rows)) }
sub, _ := cli.NewSubscriptionPoller("meter001", []string{"ts","current"}, tdorm.Filter{}, 3*time.Second, onData)
sub.Start()
// ...运行一段时间后
sub.Stop()
```

## 注意事项
- 依赖 `taosAdapter` 开启 REST 服务，默认端口 `6041`。
- DSN 示例：`root:pass@http(127.0.0.1:6041)/powerdb`，密码包含特殊字符请使用 URL 编码或引号。
- UPDATE/DELETE 支持情况取决于 TDengine 版本，生产使用前请验证。
- CQ 与 FILL 语法请以所用版本文档为准（2.6 文档）。

## 开源协议
- 本项目遵循仓库中的 `LICENSE` 文件。
- 此项目为个人所有；如有商业/闭源使用、二次分发或定制需求，请联系：刘国田 `glennliu0607@gmail.com`。