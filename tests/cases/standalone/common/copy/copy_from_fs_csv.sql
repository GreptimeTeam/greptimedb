CREATE TABLE demo(host string, cpu double, memory double, jsons JSON, ts TIMESTAMP time index);

insert into
    demo(host, cpu, memory, jsons, ts)
values
    ('host1', 66.6, 1024, '{"foo":"bar"}', 1655276557000),
    ('host2', 88.8, 333.3, '{"a":null,"foo":"bar"}', 1655276558000);

insert into
    demo(host, cpu, memory, ts)
values
    ('host3', 99.9, 444.4, 1722077263000);


CREATE TABLE demo_2(host string, ts TIMESTAMP time index);

insert into
    demo_2(host, ts)
values
    ('host4', 1755276557000),
    ('host5', 1755276558000),
    ('host6', 1822077263000);

Copy demo_2 TO '${SQLNESS_HOME}/demo/export/csv/demo_2.csv' with (format='csv');

Copy demo TO '${SQLNESS_HOME}/demo/export/csv/demo.csv' with (format='csv');

CREATE TABLE has_header(host string, ts timestamp time index);

Copy has_header FROM '${SQLNESS_HOME}/demo/export/csv/' with (pattern = 'demo*', format='csv', has_header='true', continue_on_error='true');

select * from has_header order by ts;

CREATE TABLE with_filename(host string, cpu double, memory double, ts timestamp time index);

Copy with_filename FROM '${SQLNESS_HOME}/demo/export/csv/demo.csv' with (format='csv', start_time='2022-06-15 07:02:37', end_time='2022-06-15 07:02:39');

select * from with_filename order by ts;

CREATE TABLE with_json(host string, cpu double, memory double, jsons JSON, ts timestamp time index);

Copy with_json FROM '${SQLNESS_HOME}/demo/export/csv/demo.csv' with (format='csv');

select host, cpu, memory, json_to_string(jsons), ts from with_json order by ts;

-- SQLNESS PROTOCOL MYSQL
select host, cpu, memory, jsons, ts from demo where host != 'host3';

-- SQLNESS PROTOCOL POSTGRES
select host, cpu, memory, jsons, ts from demo where host != 'host3';

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

drop table demo_2;

drop table has_header;

drop table with_filename;

drop table with_path;

drop table with_pattern;

drop table demo_with_external_column;

drop table demo_with_less_columns;
