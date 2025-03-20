-- test ttl = 5s and expire after is shorter than ttl
CREATE TABLE distinct_basic (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
)WITH ('ttl' = '5s');

CREATE FLOW test_distinct_basic SINK TO out_distinct_basic
EXPIRE AFTER INTERVAL '3s'
AS
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

ADMIN FLUSH_TABLE('distinct_basic');

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