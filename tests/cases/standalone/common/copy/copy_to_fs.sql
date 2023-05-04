CREATE TABLE demo(host string, cpu double, memory double, ts TIMESTAMP time index);

insert into demo(host, cpu, memory, ts) values ('host1', 66.6, 1024, 1655276557000), ('host2', 88.8,  333.3, 1655276558000);

Copy demo TO '/tmp/export/demo.parquet';

Copy demo TO '/tmp/export/demo.csv' with (format='csv');

Copy demo TO '/tmp/export/demo.json' with (format='json');

drop table demo;
