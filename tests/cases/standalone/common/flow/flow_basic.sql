CREATE TABLE numbers_input_basic (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

CREATE FLOW test_numbers_basic 
SINK TO out_num_cnt_basic
AS 
SELECT sum(number) FROM numbers_input_basic GROUP BY tumble(ts, '1 second', '2021-07-01 00:00:00');

-- TODO(discord9): confirm if it's necessary to flush flow here?
select flush_flow('test_numbers_basic')*0;

INSERT INTO numbers_input_basic 
VALUES
    (20, "2021-07-01 00:00:00.200"),
    (22, "2021-07-01 00:00:00.600");

select flush_flow('test_numbers_basic')*0;

SELECT col_0, window_start, window_end FROM out_num_cnt_basic;

select flush_flow('test_numbers_basic')*0;

INSERT INTO numbers_input_basic 
VALUES
    (23,"2021-07-01 00:00:01.000"),
    (24,"2021-07-01 00:00:01.500");

select flush_flow('test_numbers_basic')*0;

SELECT col_0, window_start, window_end FROM out_num_cnt_basic;

DROP FLOW test_numbers_basic;
DROP TABLE numbers_input_basic;
DROP TABLE out_num_cnt_basic;

-- test interprete interval

CREATE TABLE numbers_input_basic (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);
create table out_num_cnt_basic (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP TIME INDEX);

CREATE FLOW filter_numbers_basic SINK TO out_num_cnt_basic AS SELECT INTERVAL '1 day 1 second', INTERVAL '1 month 1 day 1 second', INTERVAL '1 year 1 month' FROM numbers_input_basic where number > 10;

SHOW CREATE FLOW filter_numbers_basic;

drop flow filter_numbers_basic;

drop table out_num_cnt_basic;

drop table numbers_input_basic;