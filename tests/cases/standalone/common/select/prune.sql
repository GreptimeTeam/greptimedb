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
-- SQLNESS REPLACE \\?"(dyn_filters|time_range|file_summary|max_part_range)\\?":.* "$1":REDACTED
explain analyze select * from demo where idc='idc1';

SELECT * FROM demo where host in ('test1');

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \\?"(dyn_filters|time_range|file_summary|max_part_range)\\?":.* "$1":REDACTED
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

-- Files whose primary key range excludes the predicate are pruned before reading them.
CREATE TABLE pk_range_prune(ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING, idc STRING, PRIMARY KEY(host, idc));

INSERT INTO pk_range_prune VALUES (1, 1, 'a', 'idc1'), (2, 2, 'b', 'idc2');

ADMIN flush_table('pk_range_prune');

INSERT INTO pk_range_prune VALUES (1, 3, 'c', 'idc1'), (2, 4, 'd', 'idc2');

ADMIN flush_table('pk_range_prune');

-- SQLNESS REPLACE (?s)^.*("files_time_range_pruned":\d+).*$ $1
EXPLAIN ANALYZE VERBOSE SELECT * FROM pk_range_prune WHERE host = 'c';

SELECT * FROM pk_range_prune WHERE host = 'c';

SELECT * FROM pk_range_prune WHERE host >= 'b' ORDER BY host;

SELECT * FROM pk_range_prune WHERE host < 'b';

DROP TABLE pk_range_prune;

CREATE TABLE pk_range_phy (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE = metric WITH ("physical_metric_table" = "");

CREATE TABLE pk_range_t1 (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING PRIMARY KEY) ENGINE = metric WITH ("on_physical_table" = "pk_range_phy");

CREATE TABLE pk_range_t2 (ts TIMESTAMP TIME INDEX, val DOUBLE, job STRING PRIMARY KEY) ENGINE = metric WITH ("on_physical_table" = "pk_range_phy");

INSERT INTO pk_range_t1 VALUES ('host1', 0, 1), ('host2', 1, 2);

ADMIN flush_table('pk_range_phy');

INSERT INTO pk_range_t2 VALUES ('job1', 0, 3), ('job2', 1, 4);

ADMIN flush_table('pk_range_phy');

-- SQLNESS REPLACE (?s)^.*("files_time_range_pruned":\d+).*$ $1
EXPLAIN ANALYZE VERBOSE SELECT * FROM pk_range_t1;

SELECT * FROM pk_range_t1 ORDER BY host;

SELECT * FROM pk_range_t2 ORDER BY job;

DROP TABLE pk_range_t1;

DROP TABLE pk_range_t2;

DROP TABLE pk_range_phy;
