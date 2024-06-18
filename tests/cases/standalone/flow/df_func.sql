CREATE TABLE numbers_input_df_func (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

CREATE FLOW test_numbers_df_func 
SINK TO out_num_cnt_df_func
AS 
SELECT sum(abs(number)) FROM numbers_input_df_func GROUP BY tumble(ts, '1 second', '2021-07-01 00:00:00');

INSERT INTO test_numbers_df_func 
VALUES
    (-20, "2021-07-01 00:00:00.200"),
    (22, "2021-07-01 00:00:00.600");

-- SQLNESS SLEEP 2s
SELECT col_0, window_start, window_end FROM out_num_cnt_df_func;

INSERT INTO test_numbers_df_func 
VALUES
    (23,"2021-07-01 00:00:01.000"),
    (-24,"2021-07-01 00:00:01.500");

-- SQLNESS SLEEP 2s
SELECT col_0, window_start, window_end FROM out_num_cnt_df_func;