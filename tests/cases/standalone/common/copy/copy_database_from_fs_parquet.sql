CREATE TABLE demo(host string, cpu double, memory double, ts TIMESTAMP time index);

INSERT INTO demo(host, cpu, memory, ts) values ('host1', 66.6, 1024, 1655276557000), ('host2', 88.8,  333.3, 1655276558000);

COPY DATABASE public TO '${SQLNESS_HOME}/demo/export/parquet/' WITH (FORMAT="parquet");

COPY DATABASE public TO '${SQLNESS_HOME}/demo/export/parquet_range/' WITH (FORMAT="parquet", start_time='2022-06-15 07:02:37.000Z', end_time='2022-06-15 07:02:37.1Z');

DELETE FROM demo;

SELECT * FROM demo ORDER BY ts;

COPY DATABASE public FROM '${SQLNESS_HOME}/demo/export/parquet/';

SELECT * FROM demo ORDER BY ts;

DELETE FROM demo;

COPY DATABASE public FROM '${SQLNESS_HOME}/demo/export/parquet_range/';

SELECT * FROM demo ORDER BY ts;

DELETE FROM demo;

COPY DATABASE public FROM '${SQLNESS_HOME}/demo/export/parquet_range/' LIMIT 2;

DROP TABLE demo;
