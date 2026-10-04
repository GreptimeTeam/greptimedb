CREATE TABLE demo(host string, cpu DOUBLE, memory DOUBLE, jsons JSON, ts TIMESTAMP TIME INDEX, ts2 DATE DEFAULT '2022-06-15');

insert into demo(host, cpu, memory, jsons, ts) values ('host1', 66.6, 1024, '{"foo":"bar"}', 1655276557000), ('host2', 88.8,  333.3, '{"a":null,"foo":"bar"}', 1655276558000);

insert into demo(host, cpu, memory, ts) values ('host3', 111.1, 444.4, 1722077263000);

COPY demo TO '${SQLNESS_HOME}/export/demo.parquet' WITH (start_time='2022-06-15 07:02:37', end_time='2022-06-15 07:02:38');

COPY demo TO '${SQLNESS_HOME}/export/demo.csv' WITH (format='csv', start_time='2022-06-15 07:02:37', end_time='2022-06-15 07:02:38');

COPY demo TO '${SQLNESS_HOME}/export/demo.json' WITH (format='json', start_time='2022-06-15 07:02:37', end_time='2022-06-15 07:02:38');

COPY (select host, cpu, jsons, ts from demo where host = 'host2') TO '${SQLNESS_HOME}/export/demo.parquet';

COPY (select host, cpu, jsons, ts from demo where host = 'host2') TO '${SQLNESS_HOME}/export/demo.csv' WITH (format='csv');

COPY (select host, cpu, jsons, ts from demo where host = 'host2') TO '${SQLNESS_HOME}/export/demo.json' WITH (format='json');

COPY (select host, cpu, jsons, ts, ts2 from demo where host = 'host2') TO '${SQLNESS_HOME}/export/demo.csv' WITH (format='csv', timestamp_format='%m-%d-%Y', date_format='%Y/%m/%d');

COPY (select host, cpu, jsons, ts, ts2 from demo where host = 'host2') TO '${SQLNESS_HOME}/export/demo.json' WITH (format='json', timestamp_format='%m-%d-%Y', date_format='%Y/%m/%d');

drop table demo;
