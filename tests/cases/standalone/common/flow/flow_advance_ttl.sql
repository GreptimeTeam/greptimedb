-- test ttl = instant
CREATE TABLE distinct_basic (
    "number" INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
)WITH ('ttl' = 'instant');

-- request-local DISTINCT is supported in streaming mode
-- SQLNESS REPLACE id=\d+ id=REDACTED
CREATE FLOW test_distinct_basic SINK TO out_distinct_basic AS
SELECT
    DISTINCT number as dis
FROM
    distinct_basic;

-- instant-TTL sources reject non-stateless LIMIT plans
CREATE FLOW test_limit_instant_rejected SINK TO out_limit_instant_rejected AS
SELECT
    number
FROM
    distinct_basic
LIMIT 1;

-- flow_options should have a flow_type:streaming
-- since source table's ttl=instant and DISTINCT is request-local
SELECT flow_name, options FROM INFORMATION_SCHEMA.FLOWS;

SHOW CREATE TABLE distinct_basic;

-- SQLNESS REPLACE \d{4} REDACTED
SHOW CREATE TABLE out_distinct_basic;

-- SQLNESS SLEEP 3s
INSERT INTO
    distinct_basic
VALUES
    (20, "2021-07-01 00:00:00.200"),
    (20, "2021-07-01 00:00:00.200"),
    (22, "2021-07-01 00:00:00.600");

-- Mirror inserts reach the flownode asynchronously; wait before flushing.
-- SQLNESS SLEEP 3s
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_distinct_basic');

SELECT
    dis
FROM
    out_distinct_basic;

SELECT number FROM distinct_basic;

-- SQLNESS SLEEP 6s
ADMIN FLUSH_TABLE('distinct_basic');

-- Recover the persisted streaming DISTINCT flow, then replan its first write
-- against an extended source schema without recreating the flow.
-- SQLNESS ARG restart=true
SELECT 1;

ALTER TABLE distinct_basic ADD COLUMN extra INT NULL;

INSERT INTO
    distinct_basic (number, ts)
VALUES
    (23, "2021-07-01 00:00:01.600"),
    (23, "2021-07-01 00:00:01.600");

-- Mirror inserts reach the flownode asynchronously; wait before flushing.
-- SQLNESS SLEEP 3s
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_distinct_basic');

SELECT
    dis
FROM
    out_distinct_basic;

SELECT number FROM distinct_basic;

DROP FLOW test_distinct_basic;
DROP TABLE distinct_basic;
DROP TABLE out_distinct_basic;

-- test ttl = instant with EVAL INTERVAL must be rejected
-- since the batching scheduler cannot read instant-TTL source tables and the
-- streaming engine cannot honor the schedule
CREATE TABLE distinct_basic (
    "number" INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
)WITH ('ttl' = 'instant');

-- SQLNESS REPLACE id=\d+ id=REDACTED
CREATE FLOW test_distinct_basic SINK TO out_distinct_basic EVAL INTERVAL '1m' AS
SELECT
    DISTINCT number as dis
FROM
    distinct_basic;

SELECT count(*) FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name = 'test_distinct_basic';

DROP TABLE distinct_basic;

-- test ttl = 5s (DISTINCT remains batching for persisted sources)
CREATE TABLE distinct_basic (
    "number" INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
)WITH ('ttl' = '5s');

-- Without a schedule, persisted DISTINCT must reach batching validation,
-- not silently become request-local streaming.
CREATE FLOW test_distinct_persisted_unscheduled
SINK TO out_distinct_persisted_unscheduled AS
SELECT DISTINCT number AS dis FROM distinct_basic;

DROP FLOW IF EXISTS test_distinct_persisted_unscheduled;
DROP TABLE IF EXISTS out_distinct_persisted_unscheduled;

CREATE FLOW test_distinct_basic SINK TO out_distinct_basic EVAL INTERVAL '1m' AS
SELECT
    DISTINCT number as dis
FROM
    distinct_basic;

-- flow_options should have a flow_type:batching
-- persisted-source DISTINCT retains batching semantics
SELECT flow_name, options FROM INFORMATION_SCHEMA.FLOWS;

-- SQLNESS ARG restart=true
SELECT 1;

-- SQLNESS SLEEP 3s
INSERT INTO
    distinct_basic
VALUES
    (20, "2021-07-01 00:00:00.200"),
    (20, "2021-07-01 00:00:00.200"),
    (22, "2021-07-01 00:00:00.600");

-- Mirror inserts reach the flownode asynchronously; wait before flushing.
-- SQLNESS SLEEP 3s
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_distinct_basic');

SHOW CREATE TABLE distinct_basic;

SHOW CREATE TABLE out_distinct_basic;

SELECT
    dis
FROM
    out_distinct_basic;

SELECT number FROM distinct_basic;

-- SQLNESS SLEEP 6s
ADMIN FLUSH_TABLE('distinct_basic');

INSERT INTO
    distinct_basic
VALUES
    (23, "2021-07-01 00:00:01.600");

-- Mirror inserts reach the flownode asynchronously; wait before flushing.
-- SQLNESS SLEEP 3s
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_distinct_basic');

SELECT
    dis
FROM
    out_distinct_basic;

SELECT number FROM distinct_basic;

DROP FLOW test_distinct_basic;
DROP TABLE distinct_basic;
DROP TABLE out_distinct_basic;

-- Streaming DISTINCT auto-sink retains all 1000 output tuples.
CREATE TABLE distinct_auto_sink (
    v INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP TIME INDEX
) WITH ('ttl' = 'instant');

CREATE FLOW test_distinct_auto_sink SINK TO out_distinct_auto_sink AS
SELECT DISTINCT v FROM distinct_auto_sink;

INSERT INTO distinct_auto_sink (v)
SELECT number FROM numbers LIMIT 1000;

-- Mirror inserts reach the flownode asynchronously; wait before flushing.
-- SQLNESS SLEEP 3s
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_distinct_auto_sink');

SELECT count(*) AS rows, count(DISTINCT v) AS distinct_values, min(v) AS min_v, max(v) AS max_v
FROM out_distinct_auto_sink;

DROP FLOW test_distinct_auto_sink;
DROP TABLE distinct_auto_sink;
DROP TABLE out_distinct_auto_sink;

CREATE TABLE distinct_auto_sink_pk (
    v INT,
    k INT,
    extra_value INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP TIME INDEX,
    PRIMARY KEY (v, k)
) WITH ('ttl' = 'instant');

CREATE FLOW test_distinct_auto_sink_pk SINK TO out_distinct_auto_sink_pk AS
SELECT DISTINCT v, extra_value FROM distinct_auto_sink_pk;

INSERT INTO distinct_auto_sink_pk (v, k, extra_value)
SELECT number % 10 AS v, number AS k, number AS extra_value FROM numbers LIMIT 1000;

-- Mirror inserts reach the flownode asynchronously; wait before flushing.
-- SQLNESS SLEEP 3s
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_distinct_auto_sink_pk');

SELECT
    count(*) AS rows,
    count(DISTINCT extra_value) AS distinct_values,
    min(extra_value) AS min_value,
    max(extra_value) AS max_value,
    sum(CASE WHEN v = extra_value % 10 THEN 0 ELSE 1 END) AS mismatched_keys
FROM out_distinct_auto_sink_pk;

DROP FLOW test_distinct_auto_sink_pk;
DROP TABLE distinct_auto_sink_pk;
DROP TABLE out_distinct_auto_sink_pk;
