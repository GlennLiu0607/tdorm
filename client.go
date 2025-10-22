/*
 * @Author: GlennLiu <glennliu0607@gmail.com>
 * @Date: 2025-10-22 16:14:02
 * @LastEditors: Glenn glennliu0607@gmail.com
 * @LastEditTime: 2025-10-22 16:14:02
 * @FilePath: \client.go
 * @Description:
 *
 * Copyright (c) 2025 by 天津晟源士兴科技有限公司, All Rights Reserved.
 */
package tdorm

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

// Client 封装 TDengine REST 连接
type Client struct {
	DB *sql.DB
}

// NewClient 通过 REST DSN 建立连接，例如：root:pass@http(127.0.0.1:6041)/
func NewClient(dsn string) (*Client, error) {
	db, err := sql.Open("taosRestful", dsn)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return &Client{DB: db}, nil
}

func (c *Client) Close() error {
	if c.DB != nil {
		return c.DB.Close()
	}
	return nil
}

// CreateDatabaseIfNotExists 创建数据库（幂等）
func (c *Client) CreateDatabaseIfNotExists(dbName string) error {
	name, err := sanitizeIdent(dbName)
	if err != nil {
		return err
	}
	_, err = c.DB.Exec("CREATE DATABASE IF NOT EXISTS " + name)
	return err
}

// UseDatabase 切换数据库
func (c *Client) UseDatabase(dbName string) error {
	name, err := sanitizeIdent(dbName)
	if err != nil {
		return err
	}
	_, err = c.DB.Exec("USE " + name)
	return err
}

// CreateStable 创建超级表（幂等）。自动添加 ts TIMESTAMP
func (c *Client) CreateStable(stable string, columns []ColumnDef, tagColumns []ColumnDef) error {
	st, err := sanitizeIdent(stable)
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		return errors.New("columns 不能为空")
	}
	// 字段定义
	fieldDefs := []string{"ts TIMESTAMP"}
	for _, col := range columns {
		name, err := sanitizeIdent(col.Name)
		if err != nil {
			return err
		}
		if strings.EqualFold(name, "ts") {
			continue
		}
		fieldDefs = append(fieldDefs, fmt.Sprintf("%s %s", name, col.Type))
	}
	// TAG 定义
	tagDefs := make([]string, 0, len(tagColumns))
	for _, tag := range tagColumns {
		name, err := sanitizeIdent(tag.Name)
		if err != nil {
			return err
		}
		tagDefs = append(tagDefs, fmt.Sprintf("%s %s", name, tag.Type))
	}
	sqlStr := fmt.Sprintf("CREATE STABLE IF NOT EXISTS %s (%s)", st, strings.Join(fieldDefs, ", "))
	if len(tagDefs) > 0 {
		sqlStr += " TAGS (" + strings.Join(tagDefs, ", ") + ")"
	}
	_, err = c.DB.Exec(sqlStr)
	return err
}

// EnsureSubTable 基于超级表自动创建子表（带 TAGS 值）
func (c *Client) EnsureSubTable(sub string, stable string, tagValues []interface{}) error {
	subName, err := sanitizeIdent(sub)
	if err != nil {
		return err
	}
	st, err := sanitizeIdent(stable)
	if err != nil {
		return err
	}
	vals := make([]string, 0, len(tagValues))
	for _, v := range tagValues {
		fv, err := formatValue(v)
		if err != nil {
			return err
		}
		vals = append(vals, fv)
	}
	sqlStr := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s USING %s TAGS (%s)", subName, st, strings.Join(vals, ", "))
	_, err = c.DB.Exec(sqlStr)
	return err
}

// Insert 插入一行。若未提供 ts，则使用 NOW()
func (c *Client) Insert(table string, row map[string]interface{}) error {
	tbl, err := sanitizeIdent(table)
	if err != nil {
		return err
	}
	cols := []string{"ts"}
	vals := []string{"NOW()"}
	if v, ok := row["ts"]; ok {
		fv, err := formatValue(v)
		if err != nil {
			return err
		}
		vals[0] = fv
	}
	for k, v := range row {
		if strings.EqualFold(k, "ts") {
			continue
		}
		col, err := sanitizeIdent(k)
		if err != nil {
			return err
		}
		fv, err := formatValue(v)
		if err != nil {
			return err
		}
		cols = append(cols, col)
		vals = append(vals, fv)
	}
	sqlStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tbl, strings.Join(cols, ", "), strings.Join(vals, ", "))
	_, err = c.DB.Exec(sqlStr)
	return err
}

// BatchInsert 批量插入多行（简单拼接）。
func (c *Client) BatchInsert(table string, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	tbl, err := sanitizeIdent(table)
	if err != nil {
		return err
	}
	// 收集列集合
	colSet := map[string]struct{}{"ts": {}}
	for _, r := range rows {
		for k := range r {
			colSet[k] = struct{}{}
		}
	}
	cols := make([]string, 0, len(colSet))
	for k := range colSet {
		cols = append(cols, k)
	}
	// 保证 ts 在首位
	sortCols := []string{"ts"}
	for _, c := range cols {
		if c != "ts" {
			sortCols = append(sortCols, c)
		}
	}
	// 构造 VALUES
	valGroups := make([]string, 0, len(rows))
	for _, r := range rows {
		vals := make([]string, 0, len(sortCols))
		for _, c := range sortCols {
			if c == "ts" {
				if v, ok := r["ts"]; ok {
					fv, err := formatValue(v)
					if err != nil {
						return err
					}
					vals = append(vals, fv)
				} else {
					vals = append(vals, "NOW()")
				}
				continue
			}
			v := r[c]
			fv, err := formatValue(v)
			if err != nil {
				return err
			}
			vals = append(vals, fv)
		}
		valGroups = append(valGroups, "("+strings.Join(vals, ", ")+")")
	}
	sqlStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tbl, strings.Join(sortCols, ", "), strings.Join(valGroups, " "))
	_, err = c.DB.Exec(sqlStr)
	return err
}

// Query 以筛选条件查询，返回行列表（map）
func (c *Client) Query(table string, columns []string, f Filter) ([]map[string]interface{}, error) {
	tbl, err := sanitizeIdent(table)
	if err != nil {
		return nil, err
	}
	cols := "*"
	if len(columns) > 0 {
		parts := make([]string, 0, len(columns))
		for _, col := range columns {
			v, err := sanitizeIdent(col)
			if err != nil {
				return nil, err
			}
			parts = append(parts, v)
		}
		cols = strings.Join(parts, ", ")
	}
	where, err := f.buildWhere()
	if err != nil {
		return nil, err
	}
	post, err := f.buildOrderLimit()
	if err != nil {
		return nil, err
	}
	sqlStr := fmt.Sprintf("SELECT %s FROM %s %s%s", cols, tbl, where, post)
	rows, err := c.DB.Query(sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		vals := make([]interface{}, len(cols))
		scans := make([]interface{}, len(cols))
		for i := range vals {
			scans[i] = &vals[i]
		}
		if err := rows.Scan(scans...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			row[col] = vals[i]
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// Update 执行更新（注意：不同 TDengine 版本对 UPDATE 支持不同）
func (c *Client) Update(table string, set map[string]interface{}, f Filter) (int64, error) {
	tbl, err := sanitizeIdent(table)
	if err != nil {
		return 0, err
	}
	if len(set) == 0 {
		return 0, errors.New("set 不能为空")
	}
	pairs := make([]string, 0, len(set))
	for k, v := range set {
		col, err := sanitizeIdent(k)
		if err != nil {
			return 0, err
		}
		fv, err := formatValue(v)
		if err != nil {
			return 0, err
		}
		pairs = append(pairs, fmt.Sprintf("%s=%s", col, fv))
	}
	where, err := f.buildWhere()
	if err != nil {
		return 0, err
	}
	sqlStr := fmt.Sprintf("UPDATE %s SET %s %s", tbl, strings.Join(pairs, ", "), where)
	res, err := c.DB.Exec(sqlStr)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// Delete 删除（注意：不同 TDengine 版本对 DELETE 支持不同）
func (c *Client) Delete(table string, f Filter) (int64, error) {
	tbl, err := sanitizeIdent(table)
	if err != nil {
		return 0, err
	}
	where, err := f.buildWhere()
	if err != nil {
		return 0, err
	}
	if strings.TrimSpace(where) == "" {
		return 0, errors.New("危险操作：不允许无 WHERE 的删除")
	}
	sqlStr := fmt.Sprintf("DELETE FROM %s %s", tbl, where)
	res, err := c.DB.Exec(sqlStr)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// Helper: 快速插入当前时刻一行到子表
func (c *Client) InsertNow(subTable string, kv map[string]interface{}) error {
	kvCopy := map[string]interface{}{"ts": time.Now()}
	for k, v := range kv {
		kvCopy[k] = v
	}
	return c.Insert(subTable, kvCopy)
}

// QueryAggregateAcrossStable 对超级表做聚合查询，可选 TAGS 分组、时间降采样与插值
// aggExpr 例如："avg(current)", "count(*)", "max(voltage)"
func (c *Client) QueryAggregateAcrossStable(stable string, aggExpr string, f Filter, groupTags []string, interval time.Duration, fill string) ([]map[string]interface{}, error) {
	st, err := sanitizeIdent(stable)
	if err != nil {
		return nil, err
	}
	where, err := f.buildWhere()
	if err != nil {
		return nil, err
	}
	group, err := buildGroupByTags(groupTags)
	if err != nil {
		return nil, err
	}
	post, err := f.buildOrderLimit()
	if err != nil {
		return nil, err
	}
	intFill := buildIntervalFill(interval, fill)
	sqlStr := fmt.Sprintf("SELECT %s FROM %s %s%s%s%s", aggExpr, st, where, group, intFill, post)
	rows, err := c.DB.Query(sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		vals := make([]interface{}, len(cols))
		scans := make([]interface{}, len(cols))
		for i := range vals {
			scans[i] = &vals[i]
		}
		if err := rows.Scan(scans...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			row[col] = vals[i]
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// QueryDownsampleWithFill 对单表或超级表做降采样并插值
// selectExpr 如 "avg(current)"，也可为 "*"；stableOrTable 可传子表或超级表名
func (c *Client) QueryDownsampleWithFill(stableOrTable string, selectExpr string, f Filter, interval time.Duration, fill string) ([]map[string]interface{}, error) {
	name, err := sanitizeIdent(stableOrTable)
	if err != nil {
		return nil, err
	}
	where, err := f.buildWhere()
	if err != nil {
		return nil, err
	}
	post, err := f.buildOrderLimit()
	if err != nil {
		return nil, err
	}
	intFill := buildIntervalFill(interval, fill)
	sqlStr := fmt.Sprintf("SELECT %s FROM %s %s%s%s", selectExpr, name, where, intFill, post)
	rows, err := c.DB.Query(sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		vals := make([]interface{}, len(cols))
		scans := make([]interface{}, len(cols))
		for i := range vals {
			scans[i] = &vals[i]
		}
		if err := rows.Scan(scans...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			row[col] = vals[i]
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// AsyncQuery 异步查询：返回结果通道、错误通道与取消函数
func (c *Client) AsyncQuery(sqlStr string) (<-chan []map[string]interface{}, <-chan error, func()) {
	resCh := make(chan []map[string]interface{}, 1)
	errCh := make(chan error, 1)
	cancel := make(chan struct{})
	cancelFn := func() { close(cancel) }
	go func() {
		defer close(resCh)
		defer close(errCh)
		rows, err := c.DB.Query(sqlStr)
		if err != nil {
			errCh <- err
			return
		}
		defer rows.Close()
		result := make([]map[string]interface{}, 0)
		for rows.Next() {
			select {
			case <-cancel:
				return
			default:
			}
			cols, err := rows.Columns()
			if err != nil {
				errCh <- err
				return
			}
			vals := make([]interface{}, len(cols))
			scans := make([]interface{}, len(cols))
			for i := range vals {
				scans[i] = &vals[i]
			}
			if err := rows.Scan(scans...); err != nil {
				errCh <- err
				return
			}
			row := make(map[string]interface{}, len(cols))
			for i, col := range cols {
				row[col] = vals[i]
			}
			result = append(result, row)
		}
		if rows.Err() != nil {
			errCh <- rows.Err()
			return
		}
		resCh <- result
	}()
	return resCh, errCh, cancelFn
}

// CreateContinuousQuery 封装创建连续查询（CQ）语句执行
// 传入完整 SQL，如：CREATE TABLE target AS SELECT ... INTERVAL(60s)
func (c *Client) CreateContinuousQuery(sqlStr string) error {
	_, err := c.DB.Exec(sqlStr)
	return err
}

// DropContinuousQuery 删除 CQ，传入完整 SQL（不同版本语法可能不同）
func (c *Client) DropContinuousQuery(sqlStr string) error {
	_, err := c.DB.Exec(sqlStr)
	return err
}

// SubscriptionPoller 通过轮询实现“订阅”效果（REST 不支持原生订阅时的替代方案）
type SubscriptionPoller struct {
	Client   *Client
	Table    string
	Columns  []string
	Filter   Filter
	Interval time.Duration
	lastTS   time.Time
	OnData   func(rows []map[string]interface{})
	stopCh   chan struct{}
}

func (c *Client) NewSubscriptionPoller(table string, cols []string, f Filter, interval time.Duration, onData func([]map[string]interface{})) (*SubscriptionPoller, error) {
	tbl, err := sanitizeIdent(table)
	if err != nil {
		return nil, err
	}
	return &SubscriptionPoller{Client: c, Table: tbl, Columns: cols, Filter: f, Interval: interval, OnData: onData, stopCh: make(chan struct{})}, nil
}

func (s *SubscriptionPoller) Start() {
	go func() {
		ticker := time.NewTicker(s.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				// 增量条件：ts > lastTS
				extra := Filter{Conj: "AND"}
				if !s.lastTS.IsZero() {
					extra.Conditions = append(extra.Conditions, Condition{Column: "ts", Op: ">", Value: s.lastTS})
				}
				// 合并原有 Filter 与增量条件
				merged := Filter{Conj: "AND", Conditions: append(s.Filter.Conditions, extra.Conditions...), OrderBy: "ts"}
				rows, err := s.Client.Query(s.Table, s.Columns, merged)
				if err != nil {
					continue
				}
				if len(rows) > 0 {
					// 更新 lastTS
					if v, ok := rows[len(rows)-1]["ts"]; ok {
						if tt, ok2 := v.(time.Time); ok2 {
							s.lastTS = tt
						}
					}
					if s.OnData != nil {
						s.OnData(rows)
					}
				}
			}
		}
	}()
}

func (s *SubscriptionPoller) Stop() { close(s.stopCh) }

// CreateDatabaseIfNotExistsMsg 创建数据库并返回提示信息
// 成功："数据库已存在或创建成功: <db>"；失败返回错误
func (c *Client) CreateDatabaseIfNotExistsMsg(dbName string) (string, error) {
	if err := c.CreateDatabaseIfNotExists(dbName); err != nil {
		return "", fmt.Errorf("CreateDatabase %s failed: %w", dbName, err)
	}
	return fmt.Sprintf("数据库已存在或创建成功: %s", dbName), nil
}

// UseDatabaseMsg 切换数据库并返回提示
// 注意：REST 模式下建议改用 DSN 指定库；此接口用于支持原生或已启用 USE 的环境
func (c *Client) UseDatabaseMsg(dbName string) (string, error) {
	if err := c.UseDatabase(dbName); err != nil {
		return "", fmt.Errorf("UseDatabase %s failed: %w", dbName, err)
	}
	return fmt.Sprintf("已切换到数据库: %s", dbName), nil
}

// CreateStableMsg 创建超级表并返回提示
func (c *Client) CreateStableMsg(stable string, columns []ColumnDef, tagColumns []ColumnDef) (string, error) {
	if err := c.CreateStable(stable, columns, tagColumns); err != nil {
		return "", fmt.Errorf("CreateStable %s failed: %w", stable, err)
	}
	return fmt.Sprintf("超级表已创建/存在: %s", stable), nil
}

// EnsureSubTableMsg 创建子表并返回提示
func (c *Client) EnsureSubTableMsg(sub string, stable string, tagValues []interface{}) (string, error) {
	if err := c.EnsureSubTable(sub, stable, tagValues); err != nil {
		return "", fmt.Errorf("EnsureSubTable %s using %s failed: %w", sub, stable, err)
	}
	return fmt.Sprintf("子表已创建/存在: %s (USING %s)", sub, stable), nil
}

// InsertMsg 插入单行并返回提示
func (c *Client) InsertMsg(table string, row map[string]interface{}) (string, error) {
	if err := c.Insert(table, row); err != nil {
		return "", fmt.Errorf("Insert into %s failed: %w", table, err)
	}
	return fmt.Sprintf("已写入 1 行到 %s", table), nil
}

// BatchInsertMsg 批量插入并返回提示
func (c *Client) BatchInsertMsg(table string, rows []map[string]interface{}) (string, error) {
	if err := c.BatchInsert(table, rows); err != nil {
		return "", fmt.Errorf("BatchInsert into %s failed: %w", table, err)
	}
	return fmt.Sprintf("已批量写入 %d 行到 %s", len(rows), table), nil
}

// QueryMsg 执行查询返回数据和提示
func (c *Client) QueryMsg(table string, columns []string, f Filter) ([]map[string]interface{}, string, error) {
	rows, err := c.Query(table, columns, f)
	if err != nil {
		return nil, "", fmt.Errorf("Query %s failed: %w", table, err)
	}
	return rows, fmt.Sprintf("查询成功，共 %d 行", len(rows)), nil
}

// UpdateMsg 执行更新返回影响行数与提示
func (c *Client) UpdateMsg(table string, set map[string]interface{}, f Filter) (int64, string, error) {
	affected, err := c.Update(table, set, f)
	if err != nil {
		return 0, "", fmt.Errorf("Update %s failed: %w", table, err)
	}
	return affected, fmt.Sprintf("更新成功，影响行数: %d", affected), nil
}

// DeleteMsg 执行删除返回影响行数与提示
func (c *Client) DeleteMsg(table string, f Filter) (int64, string, error) {
	affected, err := c.Delete(table, f)
	if err != nil {
		return 0, "", fmt.Errorf("Delete %s failed: %w", table, err)
	}
	return affected, fmt.Sprintf("删除成功，影响行数: %d", affected), nil
}

// QueryAggregateAcrossStableMsg 聚合查询（跨超级表）返回数据与提示
func (c *Client) QueryAggregateAcrossStableMsg(stable string, aggExpr string, f Filter, groupTags []string, interval time.Duration, fill string) ([]map[string]interface{}, string, error) {
	rows, err := c.QueryAggregateAcrossStable(stable, aggExpr, f, groupTags, interval, fill)
	if err != nil {
		return nil, "", fmt.Errorf("Aggregate on %s failed: %w", stable, err)
	}
	return rows, fmt.Sprintf("聚合查询成功，共 %d 行", len(rows)), nil
}

// QueryDownsampleWithFillMsg 降采样插值返回数据与提示
func (c *Client) QueryDownsampleWithFillMsg(stableOrTable string, selectExpr string, f Filter, interval time.Duration, fill string) ([]map[string]interface{}, string, error) {
	rows, err := c.QueryDownsampleWithFill(stableOrTable, selectExpr, f, interval, fill)
	if err != nil {
		return nil, "", fmt.Errorf("Downsample on %s failed: %w", stableOrTable, err)
	}
	return rows, fmt.Sprintf("降采样查询成功，共 %d 行", len(rows)), nil
}

// CreateContinuousQueryMsg 创建连续查询并返回提示
func (c *Client) CreateContinuousQueryMsg(sqlStr string) (string, error) {
	if err := c.CreateContinuousQuery(sqlStr); err != nil {
		return "", fmt.Errorf("Create CQ failed: %w", err)
	}
	return "连续查询创建成功", nil
}

// DropContinuousQueryMsg 删除连续查询并返回提示
func (c *Client) DropContinuousQueryMsg(sqlStr string) (string, error) {
	if err := c.DropContinuousQuery(sqlStr); err != nil {
		return "", fmt.Errorf("Drop CQ failed: %w", err)
	}
	return "连续查询删除成功", nil
}
