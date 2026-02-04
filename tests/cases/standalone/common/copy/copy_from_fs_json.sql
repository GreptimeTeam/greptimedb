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

Copy demo TO '${SQLNESS_HOME}/demo/export/json/demo.json' with (format='json');

CREATE TABLE with_filename(host string, cpu double, memory double, ts timestamp time index);

Copy with_filename FROM '${SQLNESS_HOME}/demo/export/json/demo.json' with (format='json', start_time='2022-06-15 07:02:37', end_time='2022-06-15 07:02:39');

select * from with_filename order by ts;

CREATE TABLE with_json(host string, cpu double, memory double, jsons JSON, ts timestamp time index);

Copy with_json FROM '${SQLNESS_HOME}/demo/export/json/demo.json' with (format='json');

select host, cpu, memory, json_to_string(jsons), ts from with_json order by ts;

-- SQLNESS PROTOCOL MYSQL
select host, cpu, memory, jsons, ts from demo where host != 'host3';

-- SQLNESS PROTOCOL POSTGRES
select host, cpu, memory, jsons, ts from demo where host != 'host3';

CREATE TABLE with_path(host string, cpu double, memory double, ts timestamp time index);

Copy with_path FROM '${SQLNESS_HOME}/demo/export/json/' with (format='json', start_time='2022-06-15 07:02:37', end_time='2022-06-15 07:02:39');

select * from with_path order by ts;

CREATE TABLE with_pattern(host string, cpu double, memory double, ts timestamp time index);

Copy with_pattern FROM '${SQLNESS_HOME}/demo/export/json/' WITH (pattern = 'demo.*',format='json', end_time='2022-06-15 07:02:39');

select * from with_pattern order by ts;

CREATE TABLE demo_with_external_column(host string, cpu double, memory double, ts timestamp time index, external_column string default 'default_value');

Copy demo_with_external_column FROM '${SQLNESS_HOME}/demo/export/json/demo.json' WITH (format='json');

select * from demo_with_external_column order by ts;

CREATE TABLE demo_with_less_columns(host string, memory double, ts timestamp time index);

Copy demo_with_less_columns FROM '${SQLNESS_HOME}/demo/export/json/demo.json' WITH (format='json');

select * from demo_with_less_columns order by ts;

CREATE TABLE json_null_prefix(note string, ts timestamp time index);

insert into
    json_null_prefix(note, ts)
values
    (NULL, 1700000000000),
    (NULL, 1700000001000),
    (NULL, 1700000002000),
    ('final', 1700000003000);

Copy json_null_prefix TO '${SQLNESS_HOME}/demo/export/json_null_prefix.json' with (format='json');

CREATE TABLE json_null_prefix_import(note string, ts timestamp time index);

Copy json_null_prefix_import FROM '${SQLNESS_HOME}/demo/export/json_null_prefix.json' with (format='json', schema_infer_max_record=2);

select * from json_null_prefix_import;

drop table demo;

drop table with_filename;

drop table with_json;

drop table with_path;

drop table with_pattern;

drop table demo_with_external_column;

drop table demo_with_less_columns;

drop table json_null_prefix;

drop table json_null_prefix_import;
