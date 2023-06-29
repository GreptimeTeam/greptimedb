CREATE TABLE demo(host string, cpu double, memory double, ts TIMESTAMP time index);

insert into demo(host, cpu, memory, ts) values ('host1', 66.6, 1024, 1655276557000), ('host2', 88.8,  333.3, 1655276558000);

Copy demo TO '/tmp/demo/export/parquet/demo.parquet';

CREATE TABLE with_filename(host string, cpu double, memory double, ts timestamp time index);

Copy with_filename FROM '/tmp/demo/export/parquet/demo.parquet';

select * from with_filename order by ts;

CREATE TABLE with_path(host string, cpu double, memory double, ts timestamp time index);

Copy with_path FROM '/tmp/demo/export/parquet/';

select * from with_path order by ts;

CREATE TABLE with_pattern(host string, cpu double, memory double, ts timestamp time index);

Copy with_pattern FROM '/tmp/demo/export/parquet/' WITH (PATTERN = 'demo.*');

select * from with_pattern order by ts;

drop table demo;

drop table with_filename;

drop table with_path;

drop table with_pattern;
