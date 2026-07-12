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

create table invalid_logical_partition (
    ts timestamp time index,
    host string primary key,
    cpu double,
)
partition on columns (host) (
    host <= 'host1',
    host > 'host1' and host <= 'host2',
    host > 'host2' and host <= 'host3',
    host > 'host3'
)
engine = metric
with (
    on_physical_table = "metric_engine_partition",
);

create table logical_table_2 (
    ts timestamp time index,
    host string primary key,
    cpu double,
)
partition on columns (host) (
    host <= 'host1',
    host > 'host1' and host <= 'host2',
    host > 'host2'
)
engine = metric
with (
    on_physical_table = "metric_engine_partition",
);

insert into logical_table_2(ts, host, cpu) values
('2023-01-01 00:00:00', 'host1', 1.0),
('2023-01-01 00:00:01', 'host2', 2.0),
('2023-01-01 00:00:02', 'host3', 3.0);

show create table logical_table_2;

select count(*) from logical_table_2;

-- check if part col aggr push down works with only subset of phy part cols
select host, count(*) from logical_table_2 GROUP BY host ORDER BY host;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN 
select host, count(*) from logical_table_2 GROUP BY host ORDER BY host;

-- check if step aggr push down works with non-part col
select ts, count(*) from logical_table_2 GROUP BY ts ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN 
select ts, count(*) from logical_table_2 GROUP BY ts ORDER BY ts;

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

show create table logical_table_3;

insert into logical_table_3(ts, a, z, cpu) values
('2023-01-01 00:00:00', 'a1', 'z1', 1.0),
('2023-01-01 00:00:01', 'a2', 'z2', 2.0),
('2023-01-01 00:00:02', 'a3', 'z3', 3.0);

select count(*) from logical_table_3;

-- check if step aggr push down works with non-part col
select a, count(*) from logical_table_3 GROUP BY a ORDER BY a;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN 
select a, count(*) from logical_table_3 GROUP BY a ORDER BY a;

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

insert into logical_table_4(ts, cpu) values
('2023-01-01 00:00:00', 1.0),
('2023-01-01 00:00:01', 2.0),
('2023-01-01 00:00:02', 3.0);

-- this should only return one row
select count(*) from logical_table_4;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN select count(*) from logical_table_4;

-- check if step aggr push down works with non-part col
select ts, count(*) from logical_table_4 GROUP BY ts ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN 
select ts, count(*) from logical_table_4 GROUP BY ts ORDER BY ts;

select * from logical_table_4;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN select * from logical_table_4;

drop table logical_table_1;

drop table logical_table_2;

drop table logical_table_3;

drop table logical_table_4;

drop table metric_engine_partition;
