CREATE TABLE numbers_input (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);
create table out_num_cnt (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP TIME INDEX);

CREATE FLOW filter_numbers  SINK TO out_num_cnt AS SELECT number FROM numbers_input where number > 10;

SHOW CREATE FLOW filter_numbers;

drop flow filter_numbers;

drop table out_num_cnt;

drop table numbers_input;
