CREATE TABLE access_log (
    "url" STRING,
    user_id BIGINT,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY ("url", user_id)
);

CREATE TABLE access_log_10s (
    "url" STRING,
    time_window timestamp time INDEX,
    state BINARY,
    PRIMARY KEY ("url")
);

CREATE FLOW calc_access_log_10s SINK TO access_log_10s
AS
SELECT
    "url",
    date_bin('10s'::INTERVAL, ts) AS time_window,
    hll(user_id) AS state
FROM
    access_log
GROUP BY
    "url",
    time_window;

-- 插入 4 条数据
INSERT INTO access_log VALUES
        ("/dashboard", 1, "2025-03-04 00:00:00"),
        ("/dashboard", 1, "2025-03-04 00:00:01"),
        ("/dashboard", 2, "2025-03-04 00:00:05"),
        ("/not_found", 3, "2025-03-04 00:00:11"),
        ("/dashboard", 4, "2025-03-04 00:00:15");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_access_log_10s');

-- 此时查询 access_log_10s 应该有 3 条数据Í
SELECT "url", time_window FROM access_log_10s;

-- 可以通过 hll_count 查询 access_log_10s 中的近似数据
SELECT "url", time_window, hll_count(state) FROM access_log_10s;

-- 进一步的，可以把 10 秒级别的数据聚合到每分钟，通过 hll_merge 来合并 10 秒的 hyperloglog 状态
SELECT
    "url",
    date_bin('1 minute'::INTERVAL, time_window) AS time_window_1m,
    hll_count(hll_merge(state)) as uv_per_min
FROM
    access_log_10s
GROUP BY
    "url",
    time_window_1m
ORDER BY
    time_window_1m;;

DROP FLOW calc_access_log_10s;
DROP TABLE access_log_10s;
DROP TABLE access_log;

CREATE TABLE percentile_base (
    "id" INT PRIMARY KEY,
    "value" DOUBLE,
    ts timestamp(0) time index
);

CREATE TABLE percentile_5s (
    "percentile_state" BINARY,
    time_window timestamp(0) time index
);

CREATE FLOW calc_percentile_5s SINK TO percentile_5s
AS
SELECT
    uddsketch_state(128, 0.01, "value") AS "value",
    date_bin('5 seconds'::INTERVAL, ts) AS time_window
FROM
    percentile_base
WHERE
    value > 0 AND value < 100
GROUP BY
    time_window;

INSERT INTO percentile_base ("id", "value", ts) VALUES
    (1, 10.0, 1),
    (2, 20.0, 2),
    (3, 30.0, 3),
    (4, 40.0, 4),
    (5, 50.0, 5),
    (6, 60.0, 6),
    (7, 70.0, 7),
    (8, 80.0, 8),
    (9, 90.0, 9),
    (10, 100.0, 10);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_percentile_5s');

SELECT
    time_window,
    uddsketch_calc(0.99, `percentile_state`) AS p99
FROM
    percentile_5s
ORDER BY
    time_window;

DROP FLOW calc_percentile_5s;
DROP TABLE percentile_5s;
DROP TABLE percentile_base;
