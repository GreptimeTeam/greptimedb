CREATE TABLE demo(host string, cpu double, memory double, ts TIMESTAMP time index);

INSERT INTO demo(host, cpu, memory, ts) values ('host1', 66.6, 1024, 1655276557000), ('host2', 88.8,  333.3, 1655276558000);

COPY DATABASE public TO '/tmp/demo/export/parquet/' WITH (FORMAT="parquet");

DELETE FROM demo;

SELECT * FROM demo ORDER BY ts;

COPY DATABASE public FROM '/tmp/demo/export/parquet/';

SELECT * FROM demo ORDER BY ts;

DROP TABLE demo;
