-- test null handling in flow

-- test null handling in value part of key-value pair
CREATE TABLE requests (
    service_name STRING,
    service_ip STRING,
    val INT,
    ts TIMESTAMP TIME INDEX
)WITH(
    append_mode = 'true'
);

CREATE TABLE sum_val_in_reqs (
    sum_val INT64,
    ts TIMESTAMP TIME INDEX
);

CREATE FLOW requests_long_term SINK TO sum_val_in_reqs AS
SELECT
    sum(val) as sum_val,
    date_bin(INTERVAL '30 seconds', ts) as time_window,
FROM
    requests
GROUP BY
    time_window;

INSERT INTO
    requests
VALUES
    (NULL, "10.0.0.1", NULL, "2024-10-18 19:00:00"),
    ("svc1", "10.0.0.2", 100, "2024-10-18 19:00:00"),
    (NULL, "10.0.0.1", NULL, "2024-10-18 19:00:30"),
    ("svc1", "10.0.0.2", 200, "2024-10-18 19:00:30"),
    (NULL, "10.0.0.1", 300, "2024-10-18 19:01:00"),
    (NULL, "10.0.0.2", NULL, "2024-10-18 19:01:01"),
    ("svc1", "10.0.0.1", 400, "2024-10-18 19:01:30"),
    ("svc1", "10.0.0.2", 200, "2024-10-18 19:01:31");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('requests_long_term');

SELECT
    *
FROM
    sum_val_in_reqs;

-- Test if FLOWS table works, but don't care about the result since it vary from runs
SELECT
    count(CASE WHEN state_size > 0 THEN 1 ELSE 0 END) as active_flows,
FROM
    INFORMATION_SCHEMA.FLOWS;

DROP FLOW requests_long_term;

DROP TABLE sum_val_in_reqs;

DROP TABLE requests;

-- test null handling in key part of key-value pair
CREATE TABLE ngx_access_log (
    client STRING,
    country STRING,
    access_time TIMESTAMP TIME INDEX
)WITH(
    append_mode = 'true'
);

CREATE FLOW calc_ngx_country SINK TO ngx_country AS
SELECT
    client,
    country as 'country',
    count(1) as country_count,
    date_bin(INTERVAL '1 hour', access_time) as time_window,
FROM
    ngx_access_log
GROUP BY
    client,
    country,
    time_window;

INSERT INTO
    ngx_access_log
VALUES
    ("cli1", null, 0),
    ("cli1", null, 0),
    ("cli2", null, 0),
    ("cli2", null, 1),
    ("cli1", "b", 0),
    ("cli1", "c", 0);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_ngx_country');

SELECT
    client,
    country,
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
    client,
    country,
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
    client,
    country,
    country_count,
    time_window
FROM
    ngx_country;

DROP FLOW calc_ngx_country;

DROP TABLE ngx_access_log;

DROP TABLE ngx_country;

-- test nullable pk with no default value
CREATE TABLE nullable_pk (
    pid INT NULL,
    client STRING,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY (pid)
) WITH (
    append_mode = 'true'
);

CREATE TABLE out_nullable_pk (
    pid INT NULL,
    client STRING,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY (pid, client)
);

CREATE FLOW calc_nullable_pk SINK TO out_nullable_pk AS
SELECT
    pid,
    client,
    ts
FROM
    nullable_pk;

INSERT INTO
    nullable_pk
VALUES
    (1, "name1", "2024-10-18 19:00:00"),
    (2, "name2", "2024-10-18 19:00:00"),
    (3, "name3", "2024-10-18 19:00:00");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_nullable_pk');

SELECT * FROM out_nullable_pk;

-- pk is nullable
INSERT INTO
    nullable_pk (client, ts)
VALUES
    ("name1", "2024-10-18 19:00:00"),
    ("name2", "2024-10-18 19:00:00"),
    ("name3", "2024-10-18 19:00:00");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_nullable_pk');

SELECT * FROM out_nullable_pk;

DROP FLOW calc_nullable_pk;

DROP TABLE nullable_pk;

DROP TABLE out_nullable_pk;
