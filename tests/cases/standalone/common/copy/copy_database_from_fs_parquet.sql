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

CREATE TABLE cpu_metrics (
    host STRING,
    `usage` DOUBLE,
    ts TIMESTAMP TIME INDEX
);

INSERT INTO cpu_metrics 
VALUES
    ('host1', 66.6, 1655276557000),
    ('host2', 77.7, 1655276558000),
    ('host3', 88.8, 1655276559000);

CREATE TABLE memory_stats (
    host STRING,
    used DOUBLE,
    `free` DOUBLE,
    ts TIMESTAMP TIME INDEX
);

INSERT INTO memory_stats 
VALUES
    ('host1', 1024, 512, 1655276557000),
    ('host2', 2048, 1024, 1655276558000),
    ('host3', 4096, 2048, 1655276559000);

CREATE TABLE event_logs (
    `id` INT,
    `message` STRING,
    ts TIMESTAMP TIME INDEX
);

INSERT INTO event_logs 
VALUES
    (1, 'start', 1655276557000),
    (2, 'processing', 1655276558000),
    (3, 'finish', 1655276559000);

CREATE TABLE sensors (
    sensor_id STRING,
    temperature DOUBLE,
    pressure INT,
    ts TIMESTAMP TIME INDEX
);

INSERT INTO sensors 
VALUES
    ('s1', 36.5, 1001, 1655276557000),
    ('s2', 37.2, 1003, 1655276558000),
    ('s3', 35.9, 998, 1655276559000);


COPY DATABASE public TO '${SQLNESS_HOME}/export_parallel/' WITH (format='parquet', parallelism=2);

DELETE FROM cpu_metrics;

DELETE FROM memory_stats;

DELETE FROM event_logs;

DELETE FROM sensors;

COPY DATABASE public FROM '${SQLNESS_HOME}/export_parallel/' WITH (parallelism=2);

SELECT * FROM cpu_metrics;

SELECT * FROM memory_stats;

SELECT * FROM event_logs;

SELECT * FROM sensors;

DROP TABLE cpu_metrics;

DROP TABLE memory_stats;

DROP TABLE event_logs;

DROP TABLE sensors;
