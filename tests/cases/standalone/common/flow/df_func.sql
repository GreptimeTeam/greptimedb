CREATE TABLE numbers_input_df_func (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

-- call `sum(abs(number))` where `abs` is DataFusion Function and `sum` is flow function
CREATE FLOW test_numbers_df_func 
SINK TO out_num_cnt_df_func
AS 
SELECT sum(abs(number)) FROM numbers_input_df_func GROUP BY tumble(ts, '1 second', '2021-07-01 00:00:00');

INSERT INTO numbers_input_df_func 
VALUES
    (-20, "2021-07-01 00:00:00.200"),
    (22, "2021-07-01 00:00:00.600");

-- sleep a little bit longer to make sure that table is created and data is inserted
-- SQLNESS SLEEP 3s
SELECT col_0, window_start, window_end FROM out_num_cnt_df_func;

INSERT INTO numbers_input_df_func 
VALUES
    (23,"2021-07-01 00:00:01.000"),
    (-24,"2021-07-01 00:00:01.500");

-- SQLNESS SLEEP 2s
SELECT col_0, window_start, window_end FROM out_num_cnt_df_func;

DROP FLOW test_numbers_df_func;
DROP TABLE numbers_input_df_func;
DROP TABLE out_num_cnt_df_func;

CREATE TABLE numbers_input_df_func (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

-- call `abs(sum(number))`to make sure that calling `abs` function(impl by datafusion) on `sum` function(impl by flow) is working
CREATE FLOW test_numbers_df_func 
SINK TO out_num_cnt_df_func
AS 
SELECT abs(sum(number)) FROM numbers_input_df_func GROUP BY tumble(ts, '1 second', '2021-07-01 00:00:00');

INSERT INTO numbers_input_df_func 
VALUES
    (-20, "2021-07-01 00:00:00.200"),
    (22, "2021-07-01 00:00:00.600");

-- sleep a little bit longer to make sure that table is created and data is inserted
-- SQLNESS SLEEP 3s
SELECT col_0, window_start, window_end FROM out_num_cnt_df_func;

INSERT INTO numbers_input_df_func 
VALUES
    (23,"2021-07-01 00:00:01.000"),
    (-24,"2021-07-01 00:00:01.500");

-- SQLNESS SLEEP 2s
SELECT col_0, window_start, window_end FROM out_num_cnt_df_func;

DROP FLOW test_numbers_df_func;
DROP TABLE numbers_input_df_func;
DROP TABLE out_num_cnt_df_func;

-- test date_bin
CREATE TABLE numbers_input (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

CREATE FLOW test_numbers 
SINK TO out_num_cnt
AS 
SELECT max(number) - min(number), date_bin(INTERVAL '1 second', ts, '2021-07-01 00:00:00'::TimestampNanosecond) FROM numbers_input GROUP BY date_bin(INTERVAL '1 second', ts, '2021-07-01 00:00:00'::TimestampNanosecond);

INSERT INTO numbers_input 
VALUES
    (20, "2021-07-01 00:00:00.200"),
    (22, "2021-07-01 00:00:00.600");

-- SQLNESS SLEEP 3s
SELECT col_0, col_1 FROM out_num_cnt;

INSERT INTO numbers_input 
VALUES
    (23,"2021-07-01 00:00:01.000"),
    (24,"2021-07-01 00:00:01.500");

-- SQLNESS SLEEP 2s
SELECT col_0, col_1 FROM out_num_cnt;

DROP FLOW test_numbers;
DROP TABLE numbers_input;
DROP TABLE out_num_cnt;


-- test date_trunc
CREATE TABLE numbers_input (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

CREATE FLOW test_numbers 
SINK TO out_num_cnt
AS 
SELECT date_trunc('second', ts), sum(number) FROM numbers_input GROUP BY date_trunc('second', ts);

INSERT INTO numbers_input 
VALUES
    (20, "2021-07-01 00:00:00.200"),
    (22, "2021-07-01 00:00:00.600");

-- SQLNESS SLEEP 3s
SELECT col_0, col_1 FROM out_num_cnt;

INSERT INTO numbers_input 
VALUES
    (23,"2021-07-01 00:00:01.000"),
    (24,"2021-07-01 00:00:01.500");

-- SQLNESS SLEEP 2s
SELECT col_0, col_1 FROM out_num_cnt;

DROP FLOW test_numbers;
DROP TABLE numbers_input;
DROP TABLE out_num_cnt;