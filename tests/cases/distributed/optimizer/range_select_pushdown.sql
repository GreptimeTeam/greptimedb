CREATE TABLE range_select_pushdown (
    ts TIMESTAMP TIME INDEX,
    shard STRING,
    series STRING,
    filter_only STRING,
    unused_tag STRING,
    value_a DOUBLE,
    value_b DOUBLE,
    unused_value DOUBLE,
    PRIMARY KEY (shard, series, filter_only, unused_tag),
) PARTITION ON COLUMNS (shard) (
    shard < 'm',
    shard >= 'm'
);

INSERT INTO range_select_pushdown VALUES
    (0, 'alpha', 'api', 'include', 'unused-a', 1.0, 10.0, 100.0),
    (2000, 'zulu', 'api', 'include', 'unused-b', 3.0, 30.0, 200.0),
    (7000, 'zulu', 'api', 'include', 'unused-c', 5.0, 50.0, 300.0),
    (8000, 'zulu', 'api', 'include', 'unused-d', NULL, 80.0, 400.0),
    (20000, 'alpha', 'api', 'include', 'unused-e', 2.0, 20.0, 500.0),
    (22000, 'zulu', 'api', 'include', 'unused-f', 4.0, 40.0, 600.0),
    (0, 'alpha', 'api', 'exclude', 'unused-g', 100.0, 1000.0, 700.0);

SELECT
    ts,
    series,
    min_value,
    max_value,
    sum_value,
    count_value,
    avg_value,
    avg_compound + 1.0 AS avg_compound_plus_one
FROM (
    SELECT
        ts,
        series,
        min(value_a) RANGE '10s' AS min_value,
        max(value_a) RANGE '10s' AS max_value,
        sum(value_a) RANGE '10s' AS sum_value,
        count(value_a) RANGE '10s' AS count_value,
        avg(value_a) RANGE '10s' AS avg_value,
        avg(value_a + value_b) RANGE '10s' AS avg_compound
    FROM range_select_pushdown
    WHERE filter_only = 'include'
        AND ts >= '1970-01-01 00:00:00'
        AND ts < '1970-01-01 00:00:30'
    ALIGN '5s' BY (series) FILL 0
)
WHERE series = 'api'
ORDER BY series, ts;

DROP TABLE range_select_pushdown;
