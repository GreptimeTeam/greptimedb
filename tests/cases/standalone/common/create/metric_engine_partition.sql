create table metric_engine_partition (
    ts timestamp time index,
    host string,
    cpu double,
    `one_partition_key` string,
    `another_partition_key` string,
    primary key(host, `one_partition_key`, `another_partition_key`)
)
partition on columns (host, `one_partition_key`, `another_partition_key`) (
    host <= 'host1',
    host > 'host1' and host <= 'host2',
    host > 'host2'
)
engine = metric
with (
    physical_metric_table = "true",
);

select count(*) from metric_engine_partition;

create table logical_table_1 (
    ts timestamp time index,
    host string primary key,
    cpu double,
)
partition on columns (host) ()
engine = metric
with (
    on_physical_table = "metric_engine_partition",
);

create table logical_table_2 (
    ts timestamp time index,
    host string primary key,
    cpu double,
)
engine = metric
with (
    on_physical_table = "metric_engine_partition",
);

create table logical_table_3 (
    ts timestamp time index,
    a string,
    z string,
    cpu double,
    primary key(a, z) -- trigger a physical table change with smaller and bigger column ids
)
engine = metric
with (
    on_physical_table = "metric_engine_partition",
);

show create table logical_table_2;

select count(*) from logical_table_2;

select count(*) from logical_table_2 GROUP BY host;

-- check if part col aggr push down works with only subset of phy part cols
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
EXPLAIN 
select count(*) from logical_table_2 GROUP BY host;

-- create a logical table without partition columns on physical table
create table logical_table_4 (
    ts timestamp time index,
    cpu double,
)
engine = metric
with (
    on_physical_table = "metric_engine_partition",
);

show create table logical_table_4;

-- this should only return one row
select count(*) from logical_table_4;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
EXPLAIN select count(*) from logical_table_4;

drop table logical_table_2;

drop table logical_table_3;

drop table logical_table_4;

drop table metric_engine_partition;
