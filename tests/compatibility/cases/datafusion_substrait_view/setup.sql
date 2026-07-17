CREATE TABLE view_metrics (
    host STRING,
    region STRING,
    value INT,
    event_ts TIMESTAMP TIME INDEX,
    PRIMARY KEY (host)
);

INSERT INTO view_metrics VALUES
    ('alpha', 'east', 10, '2024-01-01 00:00:00'),
    ('beta', 'east', NULL, '2024-01-01 00:01:00'),
    ('gamma', 'west', 30, '2024-01-01 00:02:00'),
    ('delta', 'west', 5, '2024-01-01 00:03:00');

CREATE TABLE view_labels (
    host STRING,
    label STRING,
    enabled BOOLEAN,
    label_ts TIMESTAMP TIME INDEX,
    PRIMARY KEY (host)
);

INSERT INTO view_labels VALUES
    ('alpha', 'production', TRUE, '2024-01-01 00:00:00'),
    ('beta', 'staging', NULL, '2024-01-01 00:01:00'),
    ('gamma', NULL, FALSE, '2024-01-01 00:02:00'),
    ('orphan', 'unused', TRUE, '2024-01-01 00:03:00');

CREATE VIEW persisted_scan_filter AS
SELECT
    host AS host_name,
    value AS metric_value,
    CAST(event_ts AS TIMESTAMP(3)) AS event_time
FROM view_metrics
WHERE value IS NOT NULL AND value >= 10;

CREATE VIEW persisted_grouped_aggregate AS
SELECT
    region AS region_name,
    COUNT(*) AS row_count,
    SUM(value) AS value_sum,
    AVG(value) AS value_avg
FROM view_metrics
GROUP BY region;

CREATE VIEW persisted_union AS
SELECT host AS host_name, region AS region_name
FROM view_metrics
WHERE region = 'east'
UNION
SELECT host AS host_name, region AS region_name
FROM view_metrics
WHERE host = 'beta'
UNION ALL
SELECT host AS host_name, region AS region_name
FROM view_metrics
WHERE host = 'beta';

CREATE VIEW persisted_inner_join AS
SELECT
    metrics.host AS host_name,
    metrics.value AS metric_value,
    labels.label AS label_name
FROM view_metrics AS metrics
INNER JOIN view_labels AS labels ON metrics.host = labels.host
WHERE metrics.value IS NOT NULL OR labels.enabled IS NULL;

CREATE VIEW persisted_window AS
SELECT
    region AS region_name,
    host AS host_name,
    event_ts AS event_time,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY event_ts) AS row_num
FROM view_metrics;

CREATE VIEW persisted_date_format AS
SELECT
    host AS host_name,
    date_format(event_ts, '%Y-%m-%d') AS event_day
FROM view_metrics;
