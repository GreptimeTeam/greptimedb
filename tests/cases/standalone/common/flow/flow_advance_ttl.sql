-- test ttl = instant
CREATE TABLE distinct_basic (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
)WITH ('ttl' = 'instant');

-- should fail
-- SQLNESS REPLACE id=\d+ id=REDACTED
CREATE FLOW test_distinct_basic SINK TO out_distinct_basic AS
SELECT
    DISTINCT number as dis
FROM
    distinct_basic;

ALTER TABLE distinct_basic SET 'ttl' = '5s';

CREATE FLOW test_distinct_basic SINK TO out_distinct_basic AS
SELECT
    DISTINCT number as dis
FROM
    distinct_basic;

-- SQLNESS ARG restart=true
INSERT INTO
    distinct_basic
VALUES
    (20, "2021-07-01 00:00:00.200"),
    (20, "2021-07-01 00:00:00.200"),
    (22, "2021-07-01 00:00:00.600");

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

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_distinct_basic');

SHOW CREATE TABLE distinct_basic;

SHOW CREATE TABLE out_distinct_basic;

SELECT
    dis
FROM
    out_distinct_basic;

SELECT number FROM distinct_basic;

DROP FLOW test_distinct_basic;
DROP TABLE distinct_basic;
DROP TABLE out_distinct_basic;