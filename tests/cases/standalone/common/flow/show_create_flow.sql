CREATE TABLE numbers_input_show (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);
create table out_num_cnt_show (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP TIME INDEX);


SELECT flow_name, table_catalog, flow_definition FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name='filter_numbers_show';

SHOW FLOWS LIKE 'filter_numbers_show';

CREATE FLOW filter_numbers_show SINK TO out_num_cnt_show AS SELECT number FROM numbers_input_show where number > 10;

SHOW CREATE FLOW filter_numbers_show;

SELECT flow_name, table_catalog, flow_definition FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name='filter_numbers_show';

SHOW FLOWS LIKE 'filter_numbers_show';

drop flow filter_numbers_show;

SELECT flow_name, table_catalog, flow_definition FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name='filter_numbers_show';

SHOW FLOWS LIKE 'filter_numbers_show';

drop table out_num_cnt_show;

drop table numbers_input_show;
