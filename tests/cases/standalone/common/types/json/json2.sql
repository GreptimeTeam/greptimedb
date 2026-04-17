create table json2_table (
    ts timestamp time index,
    j  json2
) with (
    'append_mode' = 'true',
    'sst_format' = 'flat',
);

drop table json2_table;
