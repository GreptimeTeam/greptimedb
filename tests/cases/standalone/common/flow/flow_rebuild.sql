CREATE TABLE input_basic (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

CREATE FLOW test_wildcard_basic sink TO out_basic AS
SELECT
    COUNT(*) as wildcard
FROM
    input_basic;

INSERT INTO
    input_basic
VALUES
    (23, "2021-07-01 00:00:01.000"),
    (24, "2021-07-01 00:00:01.500");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_wildcard_basic');

SELECT wildcard FROM out_basic;

DROP TABLE input_basic;

DROP TABLE out_basic;

DROP FLOW test_wildcard_basic;

-- combination of different order of rebuild input table/flow

CREATE TABLE input_basic (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

CREATE FLOW test_wildcard_basic sink TO out_basic AS
SELECT
    COUNT(*) as wildcard
FROM
    input_basic;

INSERT INTO
    input_basic
VALUES
    (23, "2021-07-01 00:00:01.000"),
    (24, "2021-07-01 00:00:01.500");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_wildcard_basic');

SELECT wildcard FROM out_basic;

DROP TABLE input_basic;

CREATE TABLE input_basic (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

INSERT INTO
    input_basic
VALUES
    (23, "2021-07-01 00:00:01.000"),
    (24, "2021-07-01 00:00:01.500");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_wildcard_basic');

-- this is expected to be the same as above("2") since the new `input_basic` table
-- have different table id, so is a different table
SELECT wildcard FROM out_basic;

DROP FLOW test_wildcard_basic;

-- recreate flow so that it use new table id
CREATE FLOW test_wildcard_basic sink TO out_basic AS
SELECT
    COUNT(*) as wildcard
FROM
    input_basic;

INSERT INTO
    input_basic
VALUES
    (23, "2021-07-01 00:00:01.000"),
    (24, "2021-07-01 00:00:01.500"),
    (25, "2021-07-01 00:00:01.700");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_wildcard_basic');

-- 3 is also expected, since flow don't have persisent state
SELECT wildcard FROM out_basic;

DROP TABLE input_basic;
DROP FLOW test_wildcard_basic;
DROP TABLE out_basic;