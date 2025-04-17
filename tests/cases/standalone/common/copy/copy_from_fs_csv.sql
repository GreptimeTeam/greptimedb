CREATE TABLE demo(host string, cpu double, memory double, ts TIMESTAMP time index);

insert into
    demo(host, cpu, memory, ts)
values
    ('host1', 66.6, 1024, 1655276557000),
    ('host2', 88.8, 333.3, 1655276558000),
    ('host3', 99.9, 444.4, 1722077263000);

Copy demo TO '${SQLNESS_HOME}/demo/export/csv/demo.csv' with (format='csv');

CREATE TABLE with_filename(host string, cpu double, memory double, ts timestamp time index);

Copy with_filename FROM '${SQLNESS_HOME}/demo/export/csv/demo.csv' with (format='csv', start_time='2022-06-15 07:02:37', end_time='2022-06-15 07:02:39');

select * from with_filename order by ts;

CREATE TABLE with_path(host string, cpu double, memory double, ts timestamp time index);

Copy with_path FROM '${SQLNESS_HOME}/demo/export/csv/' with (format='csv', start_time='2023-06-15 07:02:37');

select * from with_path order by ts;

CREATE TABLE with_pattern(host string, cpu double, memory double, ts timestamp time index);

Copy with_pattern FROM '${SQLNESS_HOME}/demo/export/csv/' WITH (pattern = 'demo.*', format='csv', end_time='2025-06-15 07:02:39');

select * from with_pattern order by ts;

CREATE TABLE demo_with_external_column(host string, cpu double, memory double, ts timestamp time index, external_column string default 'default_value');

Copy demo_with_external_column FROM '${SQLNESS_HOME}/demo/export/csv/demo.csv' WITH (format='csv');

select * from demo_with_external_column order by ts;

CREATE TABLE demo_with_less_columns(host string, memory double, ts timestamp time index);

Copy demo_with_less_columns FROM '${SQLNESS_HOME}/demo/export/csv/demo.csv' WITH (format='csv');

select * from demo_with_less_columns order by ts;

drop table demo;

drop table with_filename;

drop table with_path;

drop table with_pattern;

drop table demo_with_external_column;

drop table demo_with_less_columns;
