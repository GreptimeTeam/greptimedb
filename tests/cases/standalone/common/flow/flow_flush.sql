-- test if flush_flow works and flush old data to flow for compute
CREATE TABLE numbers_input_basic (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);

INSERT INTO
    numbers_input_basic
VALUES
    (20, "2021-07-01 00:00:00.200"),
    (22, "2021-07-01 00:00:00.600");

CREATE FLOW test_numbers_basic SINK TO out_num_cnt_basic AS
SELECT
    sum(number),
    date_bin(INTERVAL '1 second', ts, '2021-07-01 00:00:00.1') as time_window
FROM
    numbers_input_basic
GROUP BY
    time_window;

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_numbers_basic');

SELECT
    "sum(numbers_input_basic.number)",
    time_window
FROM
    out_num_cnt_basic;

DROP FLOW test_numbers_basic;

DROP TABLE numbers_input_basic;

DROP TABLE out_num_cnt_basic;
