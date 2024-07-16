CREATE TABLE numbers_input (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

CREATE FLOW test_numbers 
SINK TO out_num_cnt
AS 
SELECT sum(number) FROM numbers_input GROUP BY tumble(ts, '1 second', '2021-07-01 00:00:00');

INSERT INTO numbers_input 
VALUES
    (20, "2021-07-01 00:00:00.200"),
    (22, "2021-07-01 00:00:00.600");

-- SQLNESS SLEEP 3s
SELECT col_0, window_start, window_end FROM out_num_cnt;

INSERT INTO numbers_input 
VALUES
    (23,"2021-07-01 00:00:01.000"),
    (24,"2021-07-01 00:00:01.500");

-- SQLNESS SLEEP 2s
SELECT col_0, window_start, window_end FROM out_num_cnt;

DROP FLOW test_numbers;
DROP TABLE numbers_input;
DROP TABLE out_num_cnt;

-- test interprete interval

CREATE TABLE numbers_input (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);
create table out_num_cnt (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP TIME INDEX);

CREATE FLOW filter_numbers SINK TO out_num_cnt AS SELECT INTERVAL '1 day 1 second', INTERVAL '1 month 1 day 1 second', INTERVAL '1 year 1 month' FROM numbers_input where number > 10;

SHOW CREATE FLOW filter_numbers;

drop flow filter_numbers;

drop table out_num_cnt;

drop table numbers_input;
