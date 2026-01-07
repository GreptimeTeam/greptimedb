create table demo(ts timestamp time index, `value` double, host string,idc string, collector string, primary key(host, idc, collector));

insert into demo values(1,2,'test1', 'idc1', 'disk') ,(2,3,'test2', 'idc1', 'disk'), (3,4,'test3', 'idc2','memory');

select * from demo where host='test1';

select * from demo where host='test2';

select * from demo where host='test3';

select * from demo where host='test2' and idc='idc1';

select * from demo where host='test2' and idc='idc1' and collector='disk';

select * from demo where host='test2' and idc='idc2';

select * from demo where host='test3' and idc>'idc1';

select * from demo where idc='idc1' order by ts;

select * from demo where collector='disk' order by ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
explain analyze select * from demo where idc='idc1';

SELECT * FROM demo where host in ('test1');

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
explain analyze SELECT * FROM demo where host in ('test1');

drop table demo;

CREATE TABLE test_time_filter(
    host STRING,
    greptime_timestamp TIMESTAMP,
    TIME INDEX(greptime_timestamp)
)
WITH
(
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

INSERT INTO test_time_filter(host, greptime_timestamp) VALUES ('hello', '2026-01-07T00:00:00'), ('world', '2026-01-07T00:00:01'), ('test', '2026-01-07T00:00:00'), ('go', '2026-01-07T00:00:01');

SELECT host FROM test_time_filter WHERE greptime_timestamp > '2026-01-07 00:00:00';

SELECT host, greptime_timestamp FROM test_time_filter WHERE greptime_timestamp > '2026-01-07 00:00:00';

ADMIN flush_table('test_time_filter');

SELECT host FROM test_time_filter WHERE greptime_timestamp > '2026-01-07 00:00:00';

SELECT host, greptime_timestamp FROM test_time_filter WHERE greptime_timestamp > '2026-01-07 00:00:00';

DROP TABLE test_time_filter;
