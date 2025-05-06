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

-- insert 4 rows of data
INSERT INTO access_log VALUES
        ("/dashboard", 1, "2025-03-04 00:00:00"),
        ("/dashboard", 1, "2025-03-04 00:00:01"),
        ("/dashboard", 2, "2025-03-04 00:00:05"),
        ("/not_found", 3, "2025-03-04 00:00:11"),
        ("/dashboard", 4, "2025-03-04 00:00:15");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_access_log_10s');

-- query should return 3 rows
-- SQLNESS SORT_RESULT 3 1
SELECT "url", time_window FROM access_log_10s
ORDER BY
    time_window;

-- use hll_count to query the approximate data in access_log_10s
-- SQLNESS SORT_RESULT 3 1
SELECT "url", time_window, hll_count(state) FROM access_log_10s
ORDER BY
    time_window;

-- further, we can aggregate 10 seconds of data to every minute, by using hll_merge to merge 10 seconds of hyperloglog state
-- SQLNESS SORT_RESULT 3 1
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
    time_window_1m;

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
    "value" > 0 AND "value" < 70
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

CREATE TABLE percentile_base (
    "id" INT PRIMARY KEY,
    "value" DOUBLE,
    ts timestamp(0) time index
);

CREATE TABLE percentile_5s (
    "percentile_state" BINARY,
    time_window timestamp(0) time index
);

CREATE TABLE percentile_10s (
    "percentile_state" BINARY,
    time_window timestamp(0) time index
);

CREATE FLOW calc_percentile_5s SINK TO percentile_5s
AS
SELECT
    uddsketch_state(128, 0.01, CASE WHEN "value" > 0 AND "value" < 70 THEN "value" ELSE NULL END) AS "value",
    date_bin('5 seconds'::INTERVAL, ts) AS time_window
FROM
    percentile_base
GROUP BY
    time_window;

CREATE FLOW calc_percentile_10s SINK TO percentile_10s
AS
SELECT
    uddsketch_merge(128, 0.01, percentile_state),
    date_bin('10 seconds'::INTERVAL, time_window) AS time_window
FROM
    percentile_5s
GROUP BY
    date_bin('10 seconds'::INTERVAL, time_window);

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

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_percentile_10s');

SELECT
    time_window,
    uddsketch_calc(0.99, percentile_state) AS p99
FROM
    percentile_5s
ORDER BY
    time_window;

SELECT
    time_window,
    uddsketch_calc(0.99, percentile_state) AS p99
FROM
    percentile_10s
ORDER BY
    time_window;

DROP FLOW calc_percentile_5s;
DROP FLOW calc_percentile_10s;
DROP TABLE percentile_5s;
DROP TABLE percentile_10s;
DROP TABLE percentile_base;
