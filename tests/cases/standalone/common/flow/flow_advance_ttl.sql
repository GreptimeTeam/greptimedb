-- test ttl = instant
CREATE TABLE distinct_basic (
    "number" INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
)WITH ('ttl' = 'instant');

-- Instant-TTL source tables are no longer accepted as flow sources: a
-- stateless projection/DISTINCT query is rejected like any other flow.
CREATE FLOW test_distinct_basic SINK TO out_distinct_basic AS
SELECT
    DISTINCT number as dis
FROM
    distinct_basic;

-- An aggregate query over an instant-TTL source is rejected as well.
CREATE FLOW test_aggr_instant_rejected SINK TO out_aggr_instant_rejected AS
SELECT
    max(number) AS max_number
FROM
    distinct_basic;

-- EVAL INTERVAL does not bypass the rejection: the batching scheduler is the
-- only execution profile left and it cannot read instant-TTL source tables.
CREATE FLOW test_scheduled_instant_rejected
SINK TO out_scheduled_instant_rejected
EVAL INTERVAL '1m'
AS
SELECT
    DISTINCT number as dis
FROM
    distinct_basic;

SELECT count(*) FROM INFORMATION_SCHEMA.FLOWS
WHERE flow_name IN (
    'test_distinct_basic',
    'test_aggr_instant_rejected',
    'test_scheduled_instant_rejected'
);

DROP TABLE distinct_basic;

-- test ttl = 5s (persisted sources use the batching scheduler)
CREATE TABLE distinct_basic (
    "number" INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
)WITH ('ttl' = '5s');

-- Without a schedule, a window-less flow must reach batching validation
-- instead of silently becoming a request-local query.
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
