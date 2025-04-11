-- user guide example
CREATE TABLE `ngx_access_log` (
    `client` STRING NULL,
    `ua_platform` STRING NULL,
    `referer` STRING NULL,
    `method` STRING NULL,
    `endpoint` STRING NULL,
    `trace_id` STRING NULL FULLTEXT INDEX,
    `protocol` STRING NULL,
    `status` SMALLINT UNSIGNED NULL,
    `size` DOUBLE NULL,
    `agent` STRING NULL,
    `access_time` TIMESTAMP(3) NOT NULL,
    TIME INDEX (`access_time`)
) WITH(append_mode = 'true');

CREATE TABLE `ngx_statistics` (
    `status` SMALLINT UNSIGNED NULL,
    `total_logs` BIGINT NULL,
    `min_size` DOUBLE NULL,
    `max_size` DOUBLE NULL,
    `avg_size` DOUBLE NULL,
    `high_size_count` BIGINT NULL,
    `time_window` TIMESTAMP time index,
    `update_at` TIMESTAMP NULL,
    PRIMARY KEY (`status`)
);

CREATE FLOW ngx_aggregation SINK TO ngx_statistics COMMENT 'Aggregate statistics for ngx_access_log' AS
SELECT
    status,
    count(client) AS total_logs,
    min(size) as min_size,
    max(size) as max_size,
    avg(size) as avg_size,
    sum(
        case
            when `size` > 550 then 1
            else 0
        end
    ) as high_size_count,
    date_bin(INTERVAL '1 minutes', access_time) as time_window,
FROM
    ngx_access_log
GROUP BY
    status,
    time_window;

INSERT INTO
    ngx_access_log
VALUES
    (
        "android",
        "Android",
        "referer",
        "GET",
        "/api/v1",
        "trace_id",
        "HTTP",
        200,
        1000,
        "agent",
        "2021-07-01 00:00:01.000"
    ),
    (
        "ios",
        "iOS",
        "referer",
        "GET",
        "/api/v1",
        "trace_id",
        "HTTP",
        200,
        500,
        "agent",
        "2021-07-01 00:00:30.500"
    ),
    (
        "android",
        "Android",
        "referer",
        "GET",
        "/api/v1",
        "trace_id",
        "HTTP",
        200,
        600,
        "agent",
        "2021-07-01 00:01:01.000"
    ),
    (
        "ios",
        "iOS",
        "referer",
        "GET",
        "/api/v1",
        "trace_id",
        "HTTP",
        404,
        700,
        "agent",
        "2021-07-01 00:01:01.500"
    );

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('ngx_aggregation');

SELECT
    status,
    total_logs,
    min_size,
    max_size,
    avg_size,
    high_size_count,
    time_window
FROM
    ngx_statistics;

INSERT INTO
    ngx_access_log
VALUES
    (
        "android",
        "Android",
        "referer",
        "GET",
        "/api/v1",
        "trace_id",
        "HTTP",
        200,
        500,
        "agent",
        "2021-07-01 00:01:01.000"
    ),
    (
        "ios",
        "iOS",
        "referer",
        "GET",
        "/api/v1",
        "trace_id",
        "HTTP",
        404,
        800,
        "agent",
        "2021-07-01 00:01:01.500"
    );

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('ngx_aggregation');

SELECT
    status,
    total_logs,
    min_size,
    max_size,
    avg_size,
    high_size_count,
    time_window
FROM
    ngx_statistics;

DROP FLOW ngx_aggregation;

DROP TABLE ngx_statistics;

DROP TABLE ngx_access_log;

/* Usecase example */
/* Real-time analytics example */
/* input table */
CREATE TABLE ngx_access_log (
    client STRING,
    country STRING,
    access_time TIMESTAMP TIME INDEX
);

/* sink table */
CREATE TABLE ngx_country (
    country STRING,
    update_at TIMESTAMP,
    __ts_placeholder TIMESTAMP TIME INDEX,
    PRIMARY KEY(country)
);

/* create flow task to calculate the distinct country */
CREATE FLOW calc_ngx_country SINK TO ngx_country AS
SELECT
    DISTINCT country,
FROM
    ngx_access_log;

/* insert some data */
INSERT INTO
    ngx_access_log
VALUES
    ("client1", "US", "2022-01-01 00:00:00"),
    ("client2", "US", "2022-01-01 00:00:01"),
    ("client3", "UK", "2022-01-01 00:00:02"),
    ("client4", "UK", "2022-01-01 00:00:03"),
    ("client5", "CN", "2022-01-01 00:00:04"),
    ("client6", "CN", "2022-01-01 00:00:05"),
    ("client7", "JP", "2022-01-01 00:00:06"),
    ("client8", "JP", "2022-01-01 00:00:07"),
    ("client9", "KR", "2022-01-01 00:00:08"),
    ("client10", "KR", "2022-01-01 00:00:09");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_ngx_country');

SELECT
    country
FROM
    ngx_country;

DROP FLOW calc_ngx_country;

DROP TABLE ngx_access_log;

DROP TABLE ngx_country;

/* Real-time analytics example: Time Window */
/* input table */
CREATE TABLE ngx_access_log (
    client STRING,
    country STRING,
    access_time TIMESTAMP TIME INDEX
);

/* input table create same as above */
/* sink table */
CREATE TABLE ngx_country (
    country STRING,
    time_window TIMESTAMP TIME INDEX,
    update_at TIMESTAMP,
    PRIMARY KEY(country)
);

CREATE FLOW calc_ngx_country SINK TO ngx_country AS
SELECT
    DISTINCT country,
    date_bin(INTERVAL '1 hour', access_time) as time_window,
FROM
    ngx_access_log
GROUP BY
    country,
    time_window;

/* insert data using the same data as above */
/* insert some data */
INSERT INTO
    ngx_access_log
VALUES
    ("client1", "US", "2022-01-01 00:00:00"),
    ("client2", "US", "2022-01-01 00:00:01"),
    ("client3", "UK", "2022-01-01 00:00:02"),
    ("client4", "UK", "2022-01-01 00:00:03"),
    ("client5", "CN", "2022-01-01 00:00:04"),
    ("client6", "CN", "2022-01-01 00:00:05"),
    ("client7", "JP", "2022-01-01 00:00:06"),
    ("client8", "JP", "2022-01-01 00:00:07"),
    ("client9", "KR", "2022-01-01 00:00:08"),
    ("client10", "KR", "2022-01-01 00:00:09");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_ngx_country');

SELECT
    country,
    time_window
FROM
    ngx_country;

DROP FLOW calc_ngx_country;

DROP TABLE ngx_access_log;

DROP TABLE ngx_country;

/* create input table */
CREATE TABLE temp_sensor_data (
    sensor_id INT,
    loc STRING,
    temperature DOUBLE,
    ts TIMESTAMP TIME INDEX
);

/* Real-time monitoring example */
/* create sink table */
CREATE TABLE temp_alerts (
    sensor_id INT,
    loc STRING,
    max_temp DOUBLE,
    event_ts TIMESTAMP TIME INDEX,
    PRIMARY KEY(sensor_id, loc)
);

CREATE FLOW temp_monitoring SINK TO temp_alerts AS
SELECT
    sensor_id,
    loc,
    max(temperature) as max_temp,
    max(ts) as event_ts,
FROM
    temp_sensor_data
GROUP BY
    sensor_id,
    loc
HAVING
    max_temp > 100;

INSERT INTO
    temp_sensor_data
VALUES
    (1, "room1", 98.5, "2022-01-01 00:00:00"),
    (2, "room2", 99.5, "2022-01-01 00:00:01");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('temp_monitoring');

SELECT
    sensor_id,
    loc,
    max_temp,
    event_ts
FROM
    temp_alerts;

INSERT INTO
    temp_sensor_data
VALUES
    (1, "room1", 101.5, "2022-01-01 00:00:02"),
    (2, "room2", 102.5, "2022-01-01 00:00:03");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('temp_monitoring');

/* wait at least one second for flow to update results to sink table */
SELECT
    sensor_id,
    loc,
    max_temp,
    event_ts
FROM
    temp_alerts;

DROP FLOW temp_monitoring;
DROP TABLE temp_sensor_data;
DROP TABLE temp_alerts;

/* Real-time dashboard */
/* create input table */
CREATE TABLE ngx_access_log (
    client STRING,
    stat INT,
    size INT,
    access_time TIMESTAMP TIME INDEX
);

/* create sink table */
CREATE TABLE ngx_distribution (
    stat INT,
    bucket_size INT,
    total_logs BIGINT,
    time_window TIMESTAMP TIME INDEX,
    update_at TIMESTAMP,
    /* auto generated column to store the last update time */
    PRIMARY KEY(stat, bucket_size)
);

/* create flow task to calculate the distribution of packet sizes for each status code */
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

INSERT INTO
    ngx_access_log
VALUES
    ("cli1", 200, 100, "2022-01-01 00:00:00"),
    ("cli2", 200, 104, "2022-01-01 00:00:01"),
    ("cli3", 200, 120, "2022-01-01 00:00:02"),
    ("cli4", 200, 124, "2022-01-01 00:00:03"),
    ("cli5", 200, 140, "2022-01-01 00:00:04"),
    ("cli6", 404, 144, "2022-01-01 00:00:05"),
    ("cli7", 404, 160, "2022-01-01 00:00:06"),
    ("cli8", 404, 164, "2022-01-01 00:00:07"),
    ("cli9", 404, 180, "2022-01-01 00:00:08"),
    ("cli10", 404, 184, "2022-01-01 00:00:09");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_ngx_distribution');

SELECT
    stat,
    bucket_size,
    total_logs,
    time_window
FROM
    ngx_distribution;

DROP FLOW calc_ngx_distribution;

DROP TABLE ngx_distribution;

DROP TABLE ngx_access_log;