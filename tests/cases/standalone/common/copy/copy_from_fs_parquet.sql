CREATE TABLE demo(host string, cpu double, memory double, ts TIMESTAMP time index);

CREATE TABLE demo_2(host string, cpu double, memory double, ts TIMESTAMP time index);

insert into
    demo(host, cpu, memory, ts)
values
    ('host1', 66.6, 1024, 1655276557000),
    ('host2', 88.8, 333.3, 1655276558000),
    ('host3', 111.1, 444.4, 1722077263000);

insert into
    demo_2(host, cpu, memory, ts)
values
    ('host4', 77.7, 1111, 1655276555000),
    ('host5', 99.9, 444.4, 1655276556000),
    ('host6', 222.2, 555.5, 1722077264000);

Copy demo TO '/tmp/demo/export/parquet_files/demo.parquet';

Copy demo_2 TO '/tmp/demo/export/parquet_files/demo_2.parquet';

CREATE TABLE with_filename(host string, cpu double, memory double, ts timestamp time index);

Copy with_filename FROM '/tmp/demo/export/parquet_files/demo.parquet' with (start_time='2022-06-15 07:02:37', end_time='2022-06-15 07:02:39');

select * from with_filename order by ts;

CREATE TABLE with_path(host string, cpu double, memory double, ts timestamp time index);

Copy with_path FROM '/tmp/demo/export/parquet_files/';

select * from with_path order by ts;

CREATE TABLE with_pattern(host string, cpu double, memory double, ts timestamp time index);

Copy with_pattern FROM '/tmp/demo/export/parquet_files/' WITH (PATTERN = 'demo.*', start_time='2022-06-15 07:02:39');

select * from with_pattern order by ts;

CREATE TABLE without_limit_rows(host string, cpu double, memory double, ts timestamp time index);

Copy without_limit_rows FROM '/tmp/demo/export/parquet_files/';

select count(*) from without_limit_rows;

CREATE TABLE with_limit_rows_segment(host string, cpu double, memory double, ts timestamp time index);

Copy with_limit_rows_segment FROM '/tmp/demo/export/parquet_files/' LIMIT 3;

select count(*) from with_limit_rows_segment;

Copy with_limit_rows_segment FROM '/tmp/demo/export/parquet_files/' LIMIT hello;

drop table demo;

drop table demo_2;

drop table with_filename;

drop table with_path;

drop table with_pattern;

drop table without_limit_rows;

drop table with_limit_rows_segment;
