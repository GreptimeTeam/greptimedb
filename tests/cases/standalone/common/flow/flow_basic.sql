CREATE TABLE numbers_input_basic (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

CREATE FLOW test_numbers_basic SINK TO out_num_cnt_basic AS
SELECT
    sum(number)
FROM
    numbers_input_basic
GROUP BY
    tumble(ts, '1 second', '2021-07-01 00:00:00');

SHOW CREATE TABLE out_num_cnt_basic;

-- TODO(discord9): confirm if it's necessary to flush flow here?
-- because flush_flow result is at most 1
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_numbers_basic');

SHOW CREATE TABLE out_num_cnt_basic;

-- SQLNESS ARG restart=true
INSERT INTO
    numbers_input_basic
VALUES
    (20, "2021-07-01 00:00:00.200"),
    (22, "2021-07-01 00:00:00.600");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_numbers_basic');

SELECT
    "sum(numbers_input_basic.number)",
    window_start,
    window_end
FROM
    out_num_cnt_basic;

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_numbers_basic');

INSERT INTO
    numbers_input_basic
VALUES
    (23, "2021-07-01 00:00:01.000"),
    (24, "2021-07-01 00:00:01.500");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_numbers_basic');

-- note that this quote-unquote column is a column-name, **not** a aggregation expr, generated by datafusion
SELECT
    "sum(numbers_input_basic.number)",
    window_start,
    window_end
FROM
    out_num_cnt_basic;

select * from INFORMATION_SCHEMA.flows;

DROP FLOW test_numbers_basic;

DROP TABLE numbers_input_basic;

DROP TABLE out_num_cnt_basic;

-- test count(*) rewrite
CREATE TABLE input_basic (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

CREATE FLOW test_wildcard_basic SiNk TO out_basic AS
SELECT
    COUNT(*) as wildcard
FROM
    input_basic;

SHOW CREATE TABLE out_basic;

select * from INFORMATION_SCHEMA.flows;

DROP FLOW test_wildcard_basic;

CREATE FLOW test_wildcard_basic sink TO out_basic AS
SELECT
    COUNT(*) as wildcard
FROM
    input_basic;

SHOW CREATE TABLE out_basic;

-- SQLNESS ARG restart=true
INSERT INTO
    input_basic
VALUES
    (23, "2021-07-01 00:00:01.000"),
    (24, "2021-07-01 00:00:01.500");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_wildcard_basic');

SHOW CREATE TABLE out_basic;

SELECT wildcard FROM out_basic;

select * from INFORMATION_SCHEMA.flows;

DROP FLOW test_wildcard_basic;
DROP TABLE out_basic;
DROP TABLE input_basic;

-- test distinct
CREATE TABLE distinct_basic (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

CREATE FLOW test_distinct_basic SINK TO out_distinct_basic AS
SELECT
    DISTINCT number as dis
FROM
    distinct_basic;

SHOW CREATE TABLE out_distinct_basic;

-- TODO(discord9): confirm if it's necessary to flush flow here?
-- because flush_flow result is at most 1
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_distinct_basic');

-- SQLNESS ARG restart=true
INSERT INTO
    distinct_basic
VALUES
    (20, "2021-07-01 00:00:00.200"),
    (20, "2021-07-01 00:00:00.200"),
    (22, "2021-07-01 00:00:00.600");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_distinct_basic');

SHOW CREATE TABLE out_distinct_basic;

SELECT
    dis
FROM
    out_distinct_basic;

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_distinct_basic');

INSERT INTO
    distinct_basic
VALUES
    (23, "2021-07-01 00:00:01.000"),
    (24, "2021-07-01 00:00:01.500");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_distinct_basic');

-- note that this quote-unquote column is a column-name, **not** a aggregation expr, generated by datafusion
SELECT
    dis
FROM
    out_distinct_basic;

select * from INFORMATION_SCHEMA.flows;

DROP FLOW test_distinct_basic;

DROP TABLE distinct_basic;

DROP TABLE out_distinct_basic;

CREATE TABLE bytes_log (
    byte INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- event time
    TIME INDEX(ts)
);

-- TODO(discord9): remove this after auto infer table's time index is impl
CREATE TABLE approx_rate (
    rate DOUBLE,
    time_window TIMESTAMP,
    update_at TIMESTAMP,
    TIME INDEX(time_window)
);

CREATE FLOW find_approx_rate SINK TO approx_rate AS
SELECT
    (max(byte) - min(byte)) / 30.0 as rate,
    date_bin(INTERVAL '30 second', ts) as time_window
from
    bytes_log
GROUP BY
    time_window;

SHOW CREATE TABLE approx_rate;

INSERT INTO
    bytes_log
VALUES
    (NULL, '2023-01-01 00:00:01'),
    (300, '2023-01-01 00:00:29');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('find_approx_rate');

SELECT
    rate,
    time_window
FROM
    approx_rate;

INSERT INTO
    bytes_log
VALUES
    (NULL, '2022-01-01 00:00:01'),
    (NULL, '2022-01-01 00:00:29');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('find_approx_rate');

SELECT
    rate,
    time_window
FROM
    approx_rate;

INSERT INTO
    bytes_log
VALUES
    (101, '2025-01-01 00:00:01'),
    (300, '2025-01-01 00:00:29');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('find_approx_rate');

SELECT
    rate,
    time_window
FROM
    approx_rate;

INSERT INTO
    bytes_log
VALUES
    (450, '2025-01-01 00:00:32'),
    (500, '2025-01-01 00:00:37');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('find_approx_rate');

SELECT
    rate,
    time_window
FROM
    approx_rate;

DROP TABLE bytes_log;

select * from INFORMATION_SCHEMA.flows;

DROP FLOW find_approx_rate;

DROP TABLE approx_rate;

-- input table
CREATE TABLE ngx_access_log (
    client STRING,
    country STRING,
    access_time TIMESTAMP TIME INDEX
);

-- create flow task to calculate the distinct country
CREATE FLOW calc_ngx_country SINK TO ngx_country AS
SELECT
    DISTINCT country,
FROM
    ngx_access_log;

SHOW CREATE TABLE ngx_country;

INSERT INTO
    ngx_access_log
VALUES
    ("cli1", "b", 0);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_ngx_country');

SHOW CREATE TABLE ngx_country;

SELECT
    country
FROM
    ngx_country;

-- making sure distinct is working
INSERT INTO
    ngx_access_log
VALUES
    ("cli1", "b", 1);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_ngx_country');

SELECT
    country
FROM
    ngx_country;

INSERT INTO
    ngx_access_log
VALUES
    ("cli1", "c", 2);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_ngx_country');

SELECT
    country
FROM
    ngx_country;

select * from INFORMATION_SCHEMA.flows;

DROP FLOW calc_ngx_country;

DROP TABLE ngx_access_log;

DROP TABLE ngx_country;

CREATE TABLE ngx_access_log (
    client STRING,
    country STRING,
    access_time TIMESTAMP TIME INDEX
);

CREATE FLOW calc_ngx_country SINK TO ngx_country AS
SELECT
    DISTINCT country,
    -- this distinct is not necessary, but it's a good test to see if it works
    date_bin(INTERVAL '1 hour', access_time) as time_window,
FROM
    ngx_access_log
GROUP BY
    country,
    time_window;

SHOW CREATE TABLE ngx_country;

INSERT INTO
    ngx_access_log
VALUES
    ("cli1", "b", 0);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_ngx_country');

SHOW CREATE TABLE ngx_country;

SELECT
    country,
    time_window
FROM
    ngx_country;

-- making sure distinct is working
INSERT INTO
    ngx_access_log
VALUES
    ("cli1", "b", 1);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_ngx_country');

SELECT
    country,
    time_window
FROM
    ngx_country;

INSERT INTO
    ngx_access_log
VALUES
    ("cli1", "c", 2);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_ngx_country');

SELECT
    country,
    time_window
FROM
    ngx_country;

select * from INFORMATION_SCHEMA.flows;

DROP FLOW calc_ngx_country;

DROP TABLE ngx_access_log;

DROP TABLE ngx_country;

CREATE TABLE temp_sensor_data (
    sensor_id INT,
    loc STRING,
    temperature DOUBLE,
    ts TIMESTAMP TIME INDEX
);

CREATE TABLE temp_alerts (
    sensor_id INT,
    loc STRING,
    max_temp DOUBLE,
    ts TIMESTAMP TIME INDEX
);

CREATE FLOW temp_monitoring SINK TO temp_alerts AS
SELECT
    sensor_id,
    loc,
    max(temperature) as max_temp,
FROM
    temp_sensor_data
GROUP BY
    sensor_id,
    loc
HAVING
    max_temp > 100;

SHOW CREATE TABLE temp_alerts;

INSERT INTO
    temp_sensor_data
VALUES
    (1, "room1", 50, 0);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('temp_monitoring');

-- This table should not exist yet
SHOW TABLES LIKE 'temp_alerts';

INSERT INTO
    temp_sensor_data
VALUES
    (1, "room1", 150, 1);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('temp_monitoring');

SHOW TABLES LIKE 'temp_alerts';

SELECT
    sensor_id,
    loc,
    max_temp
FROM
    temp_alerts;

INSERT INTO
    temp_sensor_data
VALUES
    (2, "room1", 0, 2);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('temp_monitoring');

SELECT
    sensor_id,
    loc,
    max_temp
FROM
    temp_alerts;

select * from INFORMATION_SCHEMA.flows;

DROP FLOW temp_monitoring;

DROP TABLE temp_sensor_data;

DROP TABLE temp_alerts;

CREATE TABLE ngx_access_log (
    client STRING,
    stat INT,
    size INT,
    access_time TIMESTAMP TIME INDEX
);

CREATE TABLE ngx_distribution (
    stat INT,
    bucket_size INT,
    total_logs BIGINT,
    time_window TIMESTAMP TIME INDEX,
    -- auto generated column by flow engine
    update_at TIMESTAMP,
    PRIMARY KEY(stat, bucket_size)
);

CREATE FLOW calc_ngx_distribution SINK TO ngx_distribution AS
SELECT
    stat,
    trunc(size, -1) :: INT as bucket_size,
    count(client) AS total_logs,
    date_bin(INTERVAL '1 minutes', access_time) as time_window,
FROM
    ngx_access_log
GROUP BY
    stat,
    time_window,
    bucket_size;

SHOW CREATE TABLE ngx_distribution;

INSERT INTO
    ngx_access_log
VALUES
    ("cli1", 200, 100, 0);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_ngx_distribution');

SELECT
    stat,
    bucket_size,
    total_logs,
    time_window
FROM
    ngx_distribution;

INSERT INTO
    ngx_access_log
VALUES
    ("cli1", 200, 200, 1),
    ("cli1", 200, 205, 1),
    ("cli1", 200, 209, 1),
    ("cli1", 200, 210, 1),
    ("cli2", 200, 300, 1);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_ngx_distribution');

SELECT
    stat,
    bucket_size,
    total_logs,
    time_window
FROM
    ngx_distribution;

select * from INFORMATION_SCHEMA.flows;

DROP FLOW calc_ngx_distribution;

DROP TABLE ngx_access_log;

DROP TABLE ngx_distribution;

CREATE TABLE requests (
    service_name STRING,
    service_ip STRING,
    val INT,
    ts TIMESTAMP TIME INDEX
);

CREATE TABLE requests_without_ip (
    service_name STRING,
    val INT,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY(service_name)
);

CREATE FLOW requests_long_term SINK TO requests_without_ip AS
SELECT
    service_name,
    val,
    ts
FROM
    requests;

SHOW CREATE TABLE requests_without_ip;

INSERT INTO
    requests
VALUES
    (NULL, "10.0.0.1", 100, "2024-10-18 19:00:00"),
    ("svc1", "10.0.0.2", 100, "2024-10-18 19:00:00"),
    (NULL, "10.0.0.1", 200, "2024-10-18 19:00:30"),
    ("svc1", "10.0.0.2", 200, "2024-10-18 19:00:30"),
    (NULL, "10.0.0.1", 300, "2024-10-18 19:01:00"),
    (NULL, "10.0.0.2", 100, "2024-10-18 19:01:01"),
    ("svc1", "10.0.0.1", 400, "2024-10-18 19:01:30"),
    ("svc1", "10.0.0.2", 200, "2024-10-18 19:01:31");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('requests_long_term');

SELECT
    *
FROM
    requests_without_ip;

-- Test if FLOWS table works, but don't care about the result since it vary from runs
SELECT
    count(CASE WHEN state_size > 0 THEN 1 ELSE 0 END) as active_flows,
FROM
    INFORMATION_SCHEMA.FLOWS;

INSERT INTO
    requests
VALUES
    (null, "10.0.0.1", 100, "2024-10-19 19:00:00"),
    (null, "10.0.0.2", 100, "2024-10-19 19:00:00"),
    (null, "10.0.0.1", 200, "2024-10-19 19:00:30"),
    (null, "10.0.0.2", 200, "2024-10-19 19:00:30"),
    (null, "10.0.0.1", 300, "2024-10-19 19:01:00"),
    (null, "10.0.0.2", 100, "2024-10-19 19:01:01"),
    (null, "10.0.0.1", 400, "2024-10-19 19:01:30"),
    (null, "10.0.0.2", 200, "2024-10-19 19:01:31");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('requests_long_term');

SELECT
    *
FROM
    requests_without_ip;

INSERT INTO
    requests
VALUES
    ("svc2", "10.0.0.1", 100, "2024-10-18 19:00:00"),
    ("svc2", "10.0.0.2", 100, "2024-10-18 19:00:00"),
    ("svc2", "10.0.0.1", 200, "2024-10-18 19:00:30"),
    ("svc2", "10.0.0.2", 200, "2024-10-18 19:00:30"),
    ("svc2", "10.0.0.1", 300, "2024-10-18 19:01:00"),
    ("svc2", "10.0.0.2", 100, "2024-10-18 19:01:01"),
    ("svc2", "10.0.0.1", 400, "2024-10-18 19:01:30"),
    ("svc2", "10.0.0.2", 200, "2024-10-18 19:01:31");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('requests_long_term');

SELECT
    *
FROM
    requests_without_ip;

select * from INFORMATION_SCHEMA.flows;

DROP FLOW requests_long_term;

DROP TABLE requests_without_ip;

DROP TABLE requests;

CREATE TABLE android_log (
    `log` STRING,
    ts TIMESTAMP(9),
    TIME INDEX(ts)
);

CREATE TABLE android_log_abnormal (
    crash BIGINT NULL,
    fatal BIGINT NULL,
    backtrace BIGINT NULL,
    anr BIGINT NULL,
    time_window TIMESTAMP(9) TIME INDEX,
    update_at TIMESTAMP,
);

CREATE FLOW calc_android_log_abnormal
SINK TO android_log_abnormal
AS
SELECT
    sum(case when `log` LIKE '%am_crash%' then 1 else 0 end) as crash,
    sum(case when `log` LIKE '%FATAL EXCEPTION%' then 1 else 0 end) as fatal,
    sum(case when `log` LIKE '%backtrace%' then 1 else 0 end) as backtrace,
    sum(case when `log` LIKE '%am_anr%' then 1 else 0 end) as anr,
    date_bin(INTERVAL '5 minutes', ts) as time_window,
FROM android_log
GROUP BY
    time_window;

SHOW CREATE TABLE android_log_abnormal;

INSERT INTO android_log values
("am_crash", "2021-07-01 00:01:01.000"),
("asas.backtrace.ssss", "2021-07-01 00:01:01.000");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_android_log_abnormal');

SELECT crash, fatal, backtrace, anr FROM android_log_abnormal;

INSERT INTO android_log values
("FATAL EXCEPTION", "2021-07-01 00:01:01.000"),
("mamam_anraaaa", "2021-07-01 00:01:01.000");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_android_log_abnormal');

SELECT crash, fatal, backtrace, anr FROM android_log_abnormal;

select * from INFORMATION_SCHEMA.flows;

DROP FLOW calc_android_log_abnormal;

DROP TABLE android_log_abnormal;

DROP TABLE android_log;

CREATE TABLE android_log (
    `log` STRING,
    ts TIMESTAMP(9),
    TIME INDEX(ts)
);

CREATE TABLE android_log_abnormal (
    crash BIGINT NULL,
    fatal BIGINT NULL,
    backtrace BIGINT NULL,
    anr BIGINT NULL,
    time_window TIMESTAMP(9) TIME INDEX,
    update_at TIMESTAMP,
);

CREATE FLOW calc_android_log_abnormal
SINK TO android_log_abnormal
AS
SELECT
    sum(case when regexp_like(`log`, '.*am_crash.*') then 1 else 0 end) as crash,
    sum(case when regexp_like(`log`, '.*FATAL EXCEPTION.*') then 1 else 0 end) as fatal,
    sum(case when regexp_like(`log`, '.*backtrace.*') then 1 else 0 end) as backtrace,
    sum(case when regexp_like(`log`, '.*am_anr.*') then 1 else 0 end) as anr,
    date_bin(INTERVAL '5 minutes', ts) as time_window,
FROM android_log
GROUP BY
    time_window;

SHOW CREATE TABLE android_log_abnormal;

INSERT INTO android_log values
("am_crash", "2021-07-01 00:01:01.000"),
("asas.backtrace.ssss", "2021-07-01 00:01:01.000");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_android_log_abnormal');

SELECT crash, fatal, backtrace, anr FROM android_log_abnormal;

INSERT INTO android_log values
("FATAL EXCEPTION", "2021-07-01 00:01:01.000"),
("mamam_anraaaa", "2021-07-01 00:01:01.000");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_android_log_abnormal');

SELECT crash, fatal, backtrace, anr FROM android_log_abnormal;

select * from INFORMATION_SCHEMA.flows;

DROP FLOW calc_android_log_abnormal;

DROP TABLE android_log_abnormal;

DROP TABLE android_log;

CREATE TABLE numbers_input_basic (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

CREATE FLOW test_numbers_basic SINK TO out_num_cnt_basic AS
SELECT
    sum(case when number > 10 then 1 else 0 end)/count(number) as avg_after_filter_num
FROM
    numbers_input_basic;

SHOW CREATE TABLE out_num_cnt_basic;

-- TODO(discord9): confirm if it's necessary to flush flow here?
-- because flush_flow result is at most 1
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_numbers_basic');

-- SQLNESS ARG restart=true
INSERT INTO
    numbers_input_basic
VALUES
    (20, "2021-07-01 00:00:00.200"),
    (22, "2021-07-01 00:00:00.600");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_numbers_basic');

SELECT avg_after_filter_num FROM out_num_cnt_basic;

INSERT INTO
    numbers_input_basic
VALUES
    (10, "2021-07-01 00:00:00.200"),
    (23, "2021-07-01 00:00:00.600");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_numbers_basic');

select * from INFORMATION_SCHEMA.flows;

DROP FLOW test_numbers_basic;
DROP TABLE numbers_input_basic;
DROP TABLE out_num_cnt_basic;

CREATE TABLE `live_connection_log`
(
    `device_model` STRING NULL,
    `connect_protocol` INT NULL,
    `connect_mode` INT NULL,
    `connect_retry_times` DOUBLE NULL,
    `connect_result` INT NULL,
    `first_frame_time` DOUBLE NULL,
    `record_time` TIMESTAMP TIME INDEX,
    `iot_online` INT NULL,
    PRIMARY KEY (`device_model`,`connect_protocol`),
);

CREATE TABLE `live_connection_statistics_detail`
(
  `device_model` STRING NULL,
  `connect_protocol` INT NULL,
  `connect_mode` INT NULL,
  `avg_connect_retry_times` DOUBLE NULL,
  `total_connect_result_ok` INT64 NULL,
  `total_connect_result_fail` INT64 NULL,
  `total_connect` INT64 NULL,
  `conection_rate` DOUBLE NULL,
  `avg_first_frame_time` DOUBLE NULL,
  `max_first_frame_time` DOUBLE NULL,
  `ok_conection_rate` DOUBLE NULL,
  `record_time_window` TIMESTAMP TIME INDEX,
  `update_at` TIMESTAMP,
  PRIMARY KEY (`device_model`,`connect_protocol`),
);

CREATE FLOW live_connection_aggregation_detail
SINK TO live_connection_statistics_detail
AS
SELECT
    device_model,
    connect_protocol,
    connect_mode,
    avg(connect_retry_times) as avg_connect_retry_times,
    sum(case when connect_result = 1 then 1 else 0 end) as total_connect_result_ok,
    sum(case when connect_result = 0 then 1 else 0 end) as total_connect_result_fail,
    count(connect_result) as total_connect,
    sum(case when connect_result = 1 then 1 else 0 end)::double / count(connect_result) as conection_rate,
    avg(first_frame_time) as avg_first_frame_time,
    max(first_frame_time) as max_first_frame_time,
    sum(case when connect_result = 1 then 1 else 0 end)::double / count(connect_result) as ok_conection_rate,
    date_bin(INTERVAL '1 minutes', record_time) as record_time_window,
FROM live_connection_log
WHERE iot_online = 1
GROUP BY
    device_model,
    connect_protocol,
    connect_mode,
    record_time_window;

INSERT INTO
    live_connection_log
VALUES
    ("STM51", 1, 1, 0.5, 1, 0.1, 0, 1);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('live_connection_aggregation_detail');

SELECT device_model,
  connect_protocol,
  connect_mode,
  avg_connect_retry_times,
  total_connect_result_ok,
  total_connect_result_fail,
  total_connect,
  conection_rate,
  avg_first_frame_time,
  max_first_frame_time,
  ok_conection_rate,
  record_time_window FROM live_connection_statistics_detail;

select * from INFORMATION_SCHEMA.flows;

DROP FLOW live_connection_aggregation_detail;
DROP TABLE live_connection_log;
DROP TABLE live_connection_statistics_detail;
