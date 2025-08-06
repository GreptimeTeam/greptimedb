create table metric (ts timestamp(3) time index, val double);
create table labels (ts timestamp(3) time index, host string primary key, cpu double);

insert into metric values
    (0,0),
    (10000,8),
    (20000,8),
    (30000,2),
    (40000,3);

insert into labels values
    ('1970-01-01 00:00:00', 'host1', 0.1),
    ('1970-01-01 00:00:10', 'host1', 0.8),
    ('1970-01-01 00:00:20', 'host1', 0.8),
    ('1970-01-01 00:00:30', 'host1', 0.2),
    ('1970-01-01 00:00:40', 'host1', 0.3),
    ('1970-01-01 00:00:00', 'host2', 0.2),
    ('1970-01-01 00:00:10', 'host2', 0.9),
    ('1970-01-01 00:00:20', 'host2', 0.7),
    ('1970-01-01 00:00:30', 'host2', 0.4),
    ('1970-01-01 00:00:40', 'host2', 0.5);

-- Basic TQL CTE without column aliases
WITH tql as (
    TQL EVAL (0, 40, '10s') metric
)
SELECT * FROM tql;

-- TQL CTE with column aliases
WITH tql (the_timestamp, the_value) as (
    TQL EVAL (0, 40, '10s') metric
)
SELECT * FROM tql;

-- Hybrid CTEs (TQL + SQL)
WITH 
    tql_data (ts, val) AS (TQL EVAL (0, 40, '10s') metric),
    filtered AS (SELECT * FROM tql_data WHERE val > 5)
SELECT count(*) FROM filtered;

-- TQL CTE with complex PromQL expressions
WITH 
    tql_data (ts, val) AS (TQL EVAL (0, 40, '10s') rate(metric[20s])),
    filtered (ts, val) AS (SELECT * FROM tql_data WHERE val > 0)
SELECT sum(val) FROM filtered;

-- TQL CTE with aggregation functions
WITH tql_agg(ts, summary) AS (
    TQL EVAL (0, 40, '10s') sum(labels{host=~"host.*"})
)
SELECT round(avg(summary)) as avg_sum FROM tql_agg;

-- TQL CTE with label selectors
WITH host_metrics AS (
    TQL EVAL (0, 40, '10s') labels{host="host1"}
)
SELECT count(*) as host1_points FROM host_metrics;

-- Multiple TQL CTEs referencing different tables
WITH 
    metric_data(ts, val) AS (TQL EVAL (0, 40, '10s') metric),
    label_data(ts, host, cpu) AS (TQL EVAL (0, 40, '10s') labels{host="host2"})
SELECT 
    m.val as metric_val,
    l.cpu as label_val
FROM metric_data m, label_data l 
WHERE m.ts = l.ts
ORDER BY m.ts
LIMIT 3;

-- TQL CTE with mathematical operations
WITH computed(ts, val) AS (
    TQL EVAL (0, 40, '10s') metric * 2 + 1
)
SELECT min(val) as min_computed, max(val) as max_computed FROM computed;

-- TQL CTE with window functions in SQL part
WITH tql_base(ts, val) AS (
    TQL EVAL (0, 40, '10s') metric
)
SELECT 
    ts,
    val,
    LAG(val, 1) OVER (ORDER BY ts) as prev_value
FROM tql_base
ORDER BY ts;

-- TQL CTE with HAVING clause
WITH tql_grouped(ts, host, cpu) AS (
    TQL EVAL (0, 40, '10s') labels
)
SELECT 
    DATE_TRUNC('minute', ts) as minute,
    count(*) as point_count
FROM tql_grouped 
GROUP BY minute
HAVING count(*) > 1;

-- TQL CTE with UNION
-- SQLNESS SORT_RESULT 3 1
WITH 
    host1_data(ts, host, cpu) AS (TQL EVAL (0, 40, '10s') labels{host="host1"}),
    host2_data(ts, host, cpu) AS (TQL EVAL (0, 40, '10s') labels{host="host2"})
SELECT 'host1' as source, ts, cpu FROM host1_data
UNION ALL
SELECT 'host2' as source, ts, cpu FROM host2_data;

-- Nested CTEs with TQL
WITH 
    base_tql(ts, val) AS (TQL EVAL (0, 40, '10s') metric),
    processed(ts, percent) AS (
        SELECT ts, val * 100 as percent 
        FROM base_tql 
        WHERE val > 0
    ),
    final(ts, percent) AS (
        SELECT * FROM processed WHERE percent > 200
    )
SELECT count(*) as high_values FROM final;

-- TQL CTE with time-based functions
WITH time_shifted AS (
    TQL EVAL (0, 40, '10s') metric offset 50s
)
SELECT * FROM time_shifted;

-- TQL CTE with JOIN between TQL and regular table
-- SQLNESS SORT_RESULT 3 1
WITH tql_summary(ts, host, cpu) AS (
    TQL EVAL (0, 40, '10s') avg_over_time(labels[30s])
)
SELECT 
    t.ts,
    t.cpu as avg_value,
    l.host
FROM tql_summary t
JOIN labels l ON DATE_TRUNC('second', t.ts) = DATE_TRUNC('second', l.ts)
WHERE l.host = 'host1'
ORDER BY t.ts
LIMIT 5;

-- Error case - TQL ANALYZE should fail
WITH tql_analyze AS (
    TQL ANALYZE (0, 40, '10s') metric
)
SELECT * FROM tql_analyze limit 1;

-- Error case - TQL EXPLAIN should fail  
WITH tql_explain AS (
    TQL EXPLAIN (0, 40, '10s') metric
)
SELECT * FROM tql_explain limit 1;

-- TQL CTE with lookback parameter
WITH tql_lookback AS (
    TQL EVAL (0, 40, '10s', 15s) metric
)
SELECT count(*) FROM tql_lookback;

drop table metric;
drop table labels;
