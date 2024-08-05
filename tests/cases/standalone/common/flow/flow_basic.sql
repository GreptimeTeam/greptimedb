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
-- because flush_flow result is at most 1
select flush_flow('test_numbers_basic')<=1;

-- SQLNESS ARG restart=true
INSERT INTO numbers_input_basic 
VALUES
    (20, "2021-07-01 00:00:00.200"),
    (22, "2021-07-01 00:00:00.600");

select flush_flow('test_numbers_basic')<=1;

SELECT col_0, window_start, window_end FROM out_num_cnt_basic;

select flush_flow('test_numbers_basic')<=1;

INSERT INTO numbers_input_basic 
VALUES
    (23,"2021-07-01 00:00:01.000"),
    (24,"2021-07-01 00:00:01.500");

select flush_flow('test_numbers_basic')<=1;

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

CREATE TABLE bytes_log (
    byte INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- event time
    TIME INDEX(ts)
);

CREATE TABLE approx_rate (
    rate FLOAT,
    time_window TIMESTAMP,
    update_at TIMESTAMP,
    TIME INDEX(time_window)
);

CREATE FLOW find_approx_rate
SINK TO approx_rate
AS
SELECT CAST((max(byte) - min(byte)) AS FLOAT)/30.0, date_bin(INTERVAL '30 second', ts) as time_window from bytes_log GROUP BY time_window;

INSERT INTO bytes_log VALUES 
(101, '2025-01-01 00:00:01'),
(300, '2025-01-01 00:00:29');

SELECT flush_flow('find_approx_rate')<=1;

SELECT rate, time_window FROM approx_rate;

INSERT INTO bytes_log VALUES 
(450, '2025-01-01 00:00:32'),
(500, '2025-01-01 00:00:37');

SELECT flush_flow('find_approx_rate')<=1;

SELECT rate, time_window FROM approx_rate;

DROP TABLE bytes_log;
DROP FLOW find_approx_rate;
DROP TABLE approx_rate;