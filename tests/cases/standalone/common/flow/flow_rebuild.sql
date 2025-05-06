CREATE TABLE input_basic (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
)WITH(
    append_mode = 'true'
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

-- flow batching mode
SELECT wildcard FROM out_basic;

SELECT count(*) FROM input_basic;

DROP TABLE input_basic;
DROP FLOW test_wildcard_basic;
DROP TABLE out_basic;

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

DROP FLOW test_wildcard_basic;

DROP TABLE out_basic;

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

SELECT wildcard FROM out_basic;

-- test again, this time with db restart
DROP TABLE input_basic;
DROP TABLE out_basic;
DROP FLOW test_wildcard_basic;

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

-- SQLNESS ARG restart=true
SELECT 1;

-- SQLNESS SLEEP 3s
INSERT INTO
    input_basic
VALUES
    (23, "2021-07-01 00:00:01.000"),
    (24, "2021-07-01 00:00:01.500");

-- give flownode a second to rebuild flow
-- SQLNESS SLEEP 3s
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

-- SQLNESS ARG restart=true
SELECT 1;

-- SQLNESS SLEEP 3s
INSERT INTO
    input_basic
VALUES
    (23, "2021-07-01 00:00:01.000"),
    (24, "2021-07-01 00:00:01.500");

-- give flownode a second to rebuild flow
-- SQLNESS SLEEP 3s
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

-- SQLNESS ARG restart=true
SELECT 1;

-- SQLNESS SLEEP 3s
INSERT INTO
    input_basic
VALUES
    (23, "2021-07-01 00:00:01.000"),
    (24, "2021-07-01 00:00:01.500"),
    (26, "2021-07-01 00:00:02.000");

-- give flownode a second to rebuild flow
-- SQLNESS SLEEP 3s
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

-- give flownode a second to rebuild flow
-- SQLNESS ARG restart=true
SELECT 1;

-- SQLNESS SLEEP 3s
INSERT INTO
    input_basic
VALUES
    (23, "2021-07-01 00:00:01.000"),
    (24, "2021-07-01 00:00:01.500"),
    (25, "2021-07-01 00:00:01.700");

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_wildcard_basic');

-- 4 is also expected, since flow batching mode
SELECT wildcard FROM out_basic;

SELECT count(*) FROM input_basic;

DROP TABLE input_basic;
DROP FLOW test_wildcard_basic;
DROP TABLE out_basic;

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

-- SQLNESS ARG restart=true
SELECT 1;

-- SQLNESS SLEEP 3s
INSERT INTO
    input_basic
VALUES
    (23, "2021-07-01 00:00:01.000"),
    (24, "2021-07-01 00:00:01.500");

-- give flownode a second to rebuild flow
-- SQLNESS SLEEP 3s
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_wildcard_basic');

SELECT wildcard FROM out_basic;

DROP FLOW test_wildcard_basic;

DROP TABLE out_basic;

CREATE FLOW test_wildcard_basic sink TO out_basic AS
SELECT
    COUNT(*) as wildcard
FROM
    input_basic;

-- SQLNESS ARG restart=true
SELECT 1;

-- SQLNESS SLEEP 3s
INSERT INTO
    input_basic
VALUES
    (23, "2021-07-01 00:00:01.000"),
    (24, "2021-07-01 00:00:01.500"),
    (25, "2021-07-01 00:00:01.700");

-- give flownode a second to rebuild flow
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_wildcard_basic');

SELECT wildcard FROM out_basic;

DROP FLOW test_wildcard_basic;

DROP TABLE input_basic;

DROP TABLE out_basic;

-- check if different schema is working as expected

CREATE DATABASE jsdp_log;
USE jsdp_log;

CREATE TABLE IF NOT EXISTS `api_log` (
  `time` TIMESTAMP(9) NOT NULL,
  `key` STRING NULL SKIPPING INDEX WITH(granularity = '1024', type = 'BLOOM'),
  `status_code` TINYINT NULL,
  `method` STRING NULL,
  `path` STRING NULL,
  `raw_query` STRING NULL,
  `user_agent` STRING NULL,
  `client_ip` STRING NULL,
  `duration` INT NULL,
  `count` INT NULL,
  TIME INDEX (`time`)
) ENGINE=mito WITH(
  append_mode = 'true'
);

CREATE TABLE IF NOT EXISTS `api_stats` (
  `time` TIMESTAMP(0) NOT NULL,
  `key` STRING NULL,
  `qpm` BIGINT NULL,
  `rpm` BIGINT NULL,
  `update_at` TIMESTAMP(3) NULL,
  TIME INDEX (`time`),
  PRIMARY KEY (`key`)
) ENGINE=mito WITH(
  append_mode = 'false',
  merge_mode = 'last_row'
);

CREATE FLOW IF NOT EXISTS api_stats_flow
SINK TO api_stats EXPIRE AFTER '10 minute'::INTERVAL AS
SELECT date_trunc('minute', `time`::TimestampSecond) AS `time1`, `key`, count(*), sum(`count`)
FROM api_log
GROUP BY `time1`, `key`;

INSERT INTO `api_log` (`time`, `key`, `status_code`, `method`, `path`, `raw_query`, `user_agent`, `client_ip`, `duration`, `count`) VALUES (now(), '1', 0, 'GET', '/lightning/v1/query', 'key=1&since=600', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36', '1', 21, 1);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('api_stats_flow');

SELECT key FROM api_stats;

-- SQLNESS ARG restart=true
SELECT 1;

-- SQLNESS SLEEP 5s
INSERT INTO `api_log` (`time`, `key`, `status_code`, `method`, `path`, `raw_query`, `user_agent`, `client_ip`, `duration`, `count`) VALUES (now(), '2', 0, 'GET', '/lightning/v1/query', 'key=1&since=600', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36', '1', 21, 1);

-- wait more time so flownode have time to recover flows
-- SQLNESS SLEEP 5s
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('api_stats_flow');

SELECT key FROM api_stats;

DROP FLOW api_stats_flow;

DROP TABLE api_log;
DROP TABLE api_stats;

USE public;
DROP DATABASE jsdp_log;
