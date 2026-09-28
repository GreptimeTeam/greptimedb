create table t (
    ts timestamp time index,
    val double,
    host string,
    idc string,
    primary key (host, idc),
)
partition on columns (host) (
    host < '1024',
    host >= '1024'
);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- might write to different partitions
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
explain analyze
select sum(val) from t group by host;

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- might write to different partitions
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
explain analyze
select sum(val) from t;

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- might write to different partitions
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
explain analyze
select sum(val) from t group by idc;

insert into t values
    (1000, 1, '1000-x', 'a'),
    (1000, 2, '1000-y', 'a'),
    (1000, 3, '2000-x', 'a'),
    (1000, 4, '2000-y', 'a');

-- group keys derived from the partition column span regions
select substr(host, 6, 1) as g, count(*), sum(val) from t group by g order by g;

-- grouping sets are computed on the frontend, only the PostgreSQL dialect parses them
-- SQLNESS PROTOCOL POSTGRES
select host, idc, sum(val) from t group by grouping sets ((host, idc), (host)) order by host, idc;

-- SQLNESS PROTOCOL POSTGRES
select host, sum(val) from t group by rollup(host) order by host;

drop table t;
