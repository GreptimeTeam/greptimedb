CREATE TABLE demo(host string, cpu DOUBLE, memory DOUBLE, ts TIMESTAMP TIME INDEX);

insert into demo(host, cpu, memory, ts) values ('host1', 66.6, 1024, 1655276557000), ('host2', 88.8,  333.3, 1655276558000);

COPY demo TO '${SQLNESS_HOME}/export/demo.parquet' WITH (start_time='2022-06-15 07:02:37', end_time='2022-06-15 07:02:38');

COPY demo TO '${SQLNESS_HOME}/export/demo.csv' WITH (format='csv', start_time='2022-06-15 07:02:37', end_time='2022-06-15 07:02:38');

COPY demo TO '${SQLNESS_HOME}/export/demo.json' WITH (format='json', start_time='2022-06-15 07:02:37', end_time='2022-06-15 07:02:38');

drop table demo;
