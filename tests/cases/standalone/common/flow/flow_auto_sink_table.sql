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

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_numbers_basic');

-- SQLNESS ARG restart=true
SHOW CREATE TABLE out_num_cnt_basic;

SHOW CREATE FLOW test_numbers_basic;

DROP FLOW test_numbers_basic;

DROP TABLE numbers_input_basic;

DROP TABLE out_num_cnt_basic;

CREATE TABLE numbers_input_basic (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

CREATE FLOW test_numbers_basic SINK TO out_num_cnt_basic AS
SELECT
    sum(number) as sumup, ts as event_time
FROM
    numbers_input_basic
GROUP BY
    ts;

SHOW CREATE TABLE out_num_cnt_basic;

-- SQLNESS ARG restart=true
SHOW CREATE FLOW test_numbers_basic;

SHOW CREATE TABLE out_num_cnt_basic;

DROP FLOW test_numbers_basic;

DROP TABLE numbers_input_basic;

DROP TABLE out_num_cnt_basic;
