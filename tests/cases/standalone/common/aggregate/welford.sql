CREATE TABLE test_welford (
    `id` INT PRIMARY KEY,
    `value` DOUBLE,
    `ts` TIMESTAMP TIME INDEX DEFAULT now()
);

INSERT INTO test_welford (`id`, `value`) VALUES
    (1, 10.0),
    (2, 20.0),
    (3, 30.0),
    (4, 40.0),
    (5, 50.0),
    (6, 60.0),
    (7, 70.0),
    (8, 80.0),
    (9, 90.0),
    (10, 100.0),
    (11, NULL);

SELECT stddev_pop_calc(stddev_pop_state(`value`)) FROM test_welford;

SELECT stddev_pop_calc(stddev_pop_state(`value`)) FROM test_welford WHERE false;

CREATE TABLE grouped_welford (
    `id` INT PRIMARY KEY,
    `state` BINARY,
    `ts` TIMESTAMP TIME INDEX DEFAULT now()
);

INSERT INTO grouped_welford (`id`, `state`)
SELECT 1, stddev_pop_state(`value`) FROM test_welford WHERE id <= 5;

INSERT INTO grouped_welford (`id`, `state`)
SELECT 2, stddev_pop_state(`value`) FROM test_welford WHERE id > 5;

SELECT stddev_pop_calc(stddev_pop_merge(`state`)) FROM grouped_welford;

DROP TABLE grouped_welford;
DROP TABLE test_welford;

CREATE TABLE welford_window_raw (
    `id` INT PRIMARY KEY,
    `value` DOUBLE,
    `ts` TIMESTAMP TIME INDEX
);

INSERT INTO welford_window_raw VALUES
    (1, 1.0, '2024-01-01 00:01:05'),
    (2, 2.0, '2024-01-01 00:01:20'),
    (3, 3.0, '2024-01-01 00:01:50'),
    (4, 10.0, '2024-01-01 00:02:10'),
    (5, 20.0, '2024-01-01 00:02:40'),
    (6, 4.0, '2024-01-01 00:03:05'),
    (7, 8.0, '2024-01-01 00:03:15'),
    (8, 12.0, '2024-01-01 00:03:35'),
    (9, 16.0, '2024-01-01 00:03:55'),
    (10, 100.0, '2024-01-01 00:04:05'),
    (11, 200.0, '2024-01-01 00:04:25'),
    (12, 300.0, '2024-01-01 00:04:45');

CREATE TABLE welford_minute_states (
    `minute_ts` TIMESTAMP TIME INDEX,
    `state` BINARY
);

INSERT INTO welford_minute_states (`minute_ts`, `state`)
SELECT
    date_bin(INTERVAL '1 minute', `ts`) AS minute_ts,
    stddev_pop_state(`value`) AS state
FROM welford_window_raw
GROUP BY minute_ts;

-- Merging persisted minute states must reproduce aggregation over the raw samples.
WITH ranges AS (
    SELECT
        '1-3' AS range_name,
        CAST('2024-01-01 00:01:00' AS TIMESTAMP) AS start_ts,
        CAST('2024-01-01 00:04:00' AS TIMESTAMP) AS end_ts
    UNION ALL
    SELECT
        '2-4' AS range_name,
        CAST('2024-01-01 00:02:00' AS TIMESTAMP) AS start_ts,
        CAST('2024-01-01 00:05:00' AS TIMESTAMP) AS end_ts
), direct AS (
    SELECT
        ranges.range_name,
        count(*) AS sample_count,
        stddev_pop(raw.`value`) AS stddev
    FROM ranges CROSS JOIN welford_window_raw AS raw
    WHERE raw.`ts` >= ranges.start_ts
      AND raw.`ts` < ranges.end_ts
    GROUP BY ranges.range_name
), merged AS (
    SELECT
        ranges.range_name,
        count(*) AS state_count,
        stddev_pop_calc(stddev_pop_merge(states.`state`)) AS stddev
    FROM ranges CROSS JOIN welford_minute_states AS states
    WHERE states.minute_ts >= ranges.start_ts
      AND states.minute_ts < ranges.end_ts
    GROUP BY ranges.range_name
)
SELECT
    direct.range_name,
    direct.sample_count,
    merged.state_count,
    direct.stddev AS direct_stddev,
    merged.stddev AS merged_stddev,
    abs(direct.stddev - merged.stddev) AS difference
FROM direct JOIN merged ON direct.range_name = merged.range_name
ORDER BY direct.range_name;

DROP TABLE welford_minute_states;
DROP TABLE welford_window_raw;
