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

-- SQLNESS SLEEP 2s
SELECT col_0, window_start, window_end FROM out_num_cnt;

INSERT INTO numbers_input 
VALUES
    (23,"2021-07-01 00:00:01.000"),
    (24,"2021-07-01 00:00:01.500");

-- SQLNESS SLEEP 2s
SELECT col_0, window_start, window_end FROM out_num_cnt;