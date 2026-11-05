package metrics

import (
	"context"
	"database/sql"
	"log"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type mysqlMetric struct {
	variable string
	desc     *prometheus.Desc
	kind     prometheus.ValueType
}

var (
	mysqlUp             = prometheus.NewDesc("linkchat_mysql_up", "是否成功读取本轮 MySQL 状态指标。", nil, nil)
	mysqlScrapeDuration = prometheus.NewDesc("linkchat_mysql_scrape_duration_seconds", "读取 MySQL 状态指标的耗时。", nil, nil)
	mysqlStatusMetrics  = []mysqlMetric{
		{"Innodb_buffer_pool_pages_total", prometheus.NewDesc("linkchat_mysql_buffer_pool_pages_total", "Buffer pool 总页数。", nil, nil), prometheus.GaugeValue},
		{"Innodb_buffer_pool_pages_free", prometheus.NewDesc("linkchat_mysql_buffer_pool_pages_free", "Buffer pool 空闲页数。", nil, nil), prometheus.GaugeValue},
		{"Innodb_buffer_pool_pages_dirty", prometheus.NewDesc("linkchat_mysql_buffer_pool_pages_dirty", "Buffer pool 脏页数。", nil, nil), prometheus.GaugeValue},
		{"Innodb_buffer_pool_wait_free", prometheus.NewDesc("linkchat_mysql_buffer_pool_wait_free_total", "分配页面时因没有干净页而等待刷脏完成的累计次数，不是等待时长。", nil, nil), prometheus.CounterValue},
		{"Innodb_buffer_pool_reads", prometheus.NewDesc("linkchat_mysql_buffer_pool_reads_total", "Buffer pool 未命中而需要从磁盘读取页面的累计次数。", nil, nil), prometheus.CounterValue},
		{"Innodb_buffer_pool_read_requests", prometheus.NewDesc("linkchat_mysql_buffer_pool_read_requests_total", "Buffer pool 累计逻辑读请求数。", nil, nil), prometheus.CounterValue},
		{"Innodb_buffer_pool_pages_flushed", prometheus.NewDesc("linkchat_mysql_buffer_pool_pages_flushed_total", "Buffer pool 累计刷出页数。", nil, nil), prometheus.CounterValue},
		{"Innodb_data_fsyncs", prometheus.NewDesc("linkchat_mysql_data_fsyncs_total", "InnoDB 累计文件同步操作数。", nil, nil), prometheus.CounterValue},
		{"Innodb_redo_log_current_lsn", prometheus.NewDesc("linkchat_mysql_redo_log_current_lsn", "InnoDB 当前 redo LSN。", nil, nil), prometheus.GaugeValue},
		{"Innodb_redo_log_checkpoint_lsn", prometheus.NewDesc("linkchat_mysql_redo_log_checkpoint_lsn", "InnoDB 最近 checkpoint 的 LSN。", nil, nil), prometheus.GaugeValue},
		{"Innodb_redo_log_capacity_resized", prometheus.NewDesc("linkchat_mysql_redo_log_capacity_bytes", "最近完成容量调整后实际使用的 redo 日志总容量。", nil, nil), prometheus.GaugeValue},
	}
	mysqlVariableMetrics = []mysqlMetric{
		{"innodb_buffer_pool_size", prometheus.NewDesc("linkchat_mysql_buffer_pool_size_bytes", "配置的 InnoDB buffer pool 容量。", nil, nil), prometheus.GaugeValue},
	}
)

type MySQLCollector struct {
	db      *sql.DB
	timeout time.Duration
}

func NewMySQLCollector(db *sql.DB, timeout time.Duration) *MySQLCollector {
	return &MySQLCollector{db: db, timeout: timeout}
}

func (c *MySQLCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- mysqlUp
	ch <- mysqlScrapeDuration
	for _, group := range [][]mysqlMetric{mysqlStatusMetrics, mysqlVariableMetrics} {
		for _, metric := range group {
			ch <- metric.desc
		}
	}
}

func (c *MySQLCollector) Collect(ch chan<- prometheus.Metric) {
	startedAt := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	values := make(map[string]float64)
	err := c.readValues(ctx, `SHOW GLOBAL STATUS WHERE Variable_name IN (
		'Innodb_buffer_pool_pages_total', 'Innodb_buffer_pool_pages_free',
		'Innodb_buffer_pool_pages_dirty', 'Innodb_buffer_pool_wait_free',
		'Innodb_buffer_pool_reads', 'Innodb_buffer_pool_read_requests',
		'Innodb_buffer_pool_pages_flushed', 'Innodb_data_fsyncs',
		'Innodb_redo_log_current_lsn', 'Innodb_redo_log_checkpoint_lsn',
		'Innodb_redo_log_capacity_resized'
	)`, values)
	if err == nil {
		err = c.readValues(ctx, "SHOW GLOBAL VARIABLES LIKE 'innodb_buffer_pool_size'", values)
	}
	up := 1.0
	if err != nil {
		up = 0
		log.Printf("采集 MySQL 状态失败：%v", err)
	}
	ch <- prometheus.MustNewConstMetric(mysqlUp, prometheus.GaugeValue, up)
	ch <- prometheus.MustNewConstMetric(mysqlScrapeDuration, prometheus.GaugeValue, time.Since(startedAt).Seconds())
	if err != nil {
		return
	}
	for _, group := range [][]mysqlMetric{mysqlStatusMetrics, mysqlVariableMetrics} {
		for _, metric := range group {
			if value, found := values[metric.variable]; found {
				ch <- prometheus.MustNewConstMetric(metric.desc, metric.kind, value)
			}
		}
	}
}

func (c *MySQLCollector) readValues(ctx context.Context, query string, values map[string]float64) error {
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, raw string
		if err := rows.Scan(&name, &raw); err != nil {
			return err
		}
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return err
		}
		values[name] = value
	}
	return rows.Err()
}
