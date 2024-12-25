-- test null handling in flow

-- test null handling in value part of key-value pair
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

DROP FLOW requests_long_term;

DROP TABLE requests_without_ip;

DROP TABLE requests;

-- test null handling in key part of key-value pair
CREATE TABLE ngx_access_log (
    client STRING,
    country STRING,
    access_time TIMESTAMP TIME INDEX
);

CREATE FLOW calc_ngx_country SINK TO ngx_country AS
SELECT
    country as 'country',
    count(1) as country_count,
    -- this distinct is not necessary, but it's a good test to see if it works
    date_bin(INTERVAL '1 hour', access_time) as time_window,
FROM
    ngx_access_log
GROUP BY
    country,
    time_window;

INSERT INTO
    ngx_access_log
VALUES
    ("cli1", null, 0),
    ("cli2", null, 0),
    ("cli3", null, 0),
    ("cli1", "b", 0);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_ngx_country');

SHOW CREATE TABLE ngx_country;

SELECT
    "ngx_access_log.country",
    country_count,
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
    "ngx_access_log.country",
    country_count,
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
    "ngx_access_log.country",
    country_count,
    time_window
FROM
    ngx_country;

DROP FLOW calc_ngx_country;

DROP TABLE ngx_access_log;

DROP TABLE ngx_country;