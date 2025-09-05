-- Migrated from DuckDB test: test/sql/join/ performance pattern tests
-- Tests join patterns common in time-series queries

CREATE TABLE metrics_perf(metric_id INTEGER, metric_name VARCHAR, "value" DOUBLE, timestamp_val BIGINT, ts TIMESTAMP TIME INDEX);

CREATE TABLE metadata_perf(metric_id INTEGER, unit VARCHAR, description VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE thresholds_perf(metric_id INTEGER, warning_level DOUBLE, critical_level DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO metrics_perf VALUES 
(1, 'cpu_usage', 65.2, 1700000000, 1000), (1, 'cpu_usage', 72.1, 1700000060, 2000),
(2, 'memory_usage', 85.5, 1700000000, 3000), (2, 'memory_usage', 78.3, 1700000060, 4000),
(3, 'disk_io', 120.7, 1700000000, 5000), (3, 'disk_io', 95.2, 1700000060, 6000);

INSERT INTO metadata_perf VALUES 
(1, 'percent', 'CPU utilization percentage', 1000),
(2, 'percent', 'Memory utilization percentage', 2000),
(3, 'MB/s', 'Disk I/O throughput', 3000);

INSERT INTO thresholds_perf VALUES 
(1, 70.0, 90.0, 1000), (2, 80.0, 95.0, 2000), (3, 100.0, 150.0, 3000);

-- Join for monitoring dashboard
SELECT 
  m.metric_name,
  md.unit,
  m."value",
  t.warning_level,
  t.critical_level,
  CASE 
    WHEN m."value" >= t.critical_level THEN 'CRITICAL'
    WHEN m."value" >= t.warning_level THEN 'WARNING'
    ELSE 'OK'
  END as status
FROM metrics_perf m
INNER JOIN metadata_perf md ON m.metric_id = md.metric_id
INNER JOIN thresholds_perf t ON m.metric_id = t.metric_id
ORDER BY m.timestamp_val, m.metric_id;

-- Time-series join with latest values
SELECT 
  latest_metrics.metric_name,
  latest_metrics.latest_value,
  md.unit,
  t.warning_level
FROM (
  SELECT 
    metric_id,
    metric_name,
    "value" as latest_value,
    ROW_NUMBER() OVER (PARTITION BY metric_id ORDER BY timestamp_val DESC) as rn
  FROM metrics_perf
) latest_metrics
INNER JOIN metadata_perf md ON latest_metrics.metric_id = md.metric_id
INNER JOIN thresholds_perf t ON latest_metrics.metric_id = t.metric_id
WHERE latest_metrics.rn = 1
ORDER BY latest_metrics.metric_id;

-- Historical analysis join
SELECT 
  md.description,
  COUNT(*) as total_readings,
  AVG(m."value") as avg_value,
  COUNT(CASE WHEN m."value" > t.warning_level THEN 1 END) as warning_count,
  COUNT(CASE WHEN m."value" > t.critical_level THEN 1 END) as critical_count
FROM metrics_perf m
INNER JOIN metadata_perf md ON m.metric_id = md.metric_id
INNER JOIN thresholds_perf t ON m.metric_id = t.metric_id
GROUP BY md.description, m.metric_id
ORDER BY critical_count DESC, warning_count DESC, avg_value DESC;

DROP TABLE metrics_perf;

DROP TABLE metadata_perf;

DROP TABLE thresholds_perf;