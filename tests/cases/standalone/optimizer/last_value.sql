create table t (
    ts timestamp time index,
    host string primary key,
    not_pk string,
    val double,
);

insert into t values
    (0, 'a', '🌕', 1.0),
    (1, 'b', '🌖', 2.0),
    (2, 'a', '🌗', 3.0),
    (3, 'c', '🌘', 4.0),
    (4, 'a', '🌑', 5.0),
    (5, 'b', '🌒', 6.0),
    (6, 'a', '🌓', 7.0),
    (7, 'c', '🌔', 8.0),
    (8, 'd', '🌕', 9.0);

select
    last_value(host order by ts) as ordered_host,
    last_value(not_pk order by ts),
    last_value(val order by ts)
from t
group by host
order by ordered_host;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
explain select
        last_value(host order by ts),
        last_value(not_pk order by ts),
        last_value(val order by ts)
    from t
    group by host;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
explain analyze
    select
        last_value(host order by ts),
        last_value(not_pk order by ts),
        last_value(val order by ts)
    from t
    group by host;

select last_value(ts order by ts) from t;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
explain select last_value(ts order by ts) from t;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
explain analyze
    select last_value(ts order by ts) from t;

drop table t;

create table t (
    ts timestamp time index,
    host string primary key,
    not_pk string,
    val double,
)
PARTITION ON COLUMNS (host) (
    host < '550-A',
    host >= '550-A' AND host < '550-W',
    host >= '550-W'
);

insert into t values
    (0, 'a', '🌕', 1.0),
    (1, 'b', '🌖', 2.0),
    (2, 'a', '🌗', 3.0),
    (3, 'c', '🌘', 4.0),
    (4, 'a', '🌑', 5.0),
    (5, 'b', '🌒', 6.0),
    (6, 'a', '🌓', 7.0),
    (7, 'c', '🌔', 8.0),
    (8, 'd', '🌕', 9.0);

select
    last_value(host order by ts) as ordered_host,
    last_value(not_pk order by ts),
    last_value(val order by ts)
from t
group by host
order by ordered_host;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
explain select
    last_value(host order by ts) as ordered_host,
    last_value(not_pk order by ts),
    last_value(val order by ts)
from t
group by host
order by ordered_host;


-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
explain analyze
select
    last_value(host order by ts) as ordered_host,
    last_value(not_pk order by ts),
    last_value(val order by ts)
from t
group by host
order by ordered_host;

select last_value(ts order by ts) from t;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
explain select last_value(ts order by ts) from t;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
explain analyze
    select last_value(ts order by ts) from t;

drop table t;

CREATE TABLE phy (ts timestamp time index, val double, host string primary key)
PARTITION ON COLUMNS (host) (
    host < '550-A',
    host >= '550-A' AND host < '550-W',
    host >= '550-W'
) engine=metric with ("physical_metric_table" = "");

CREATE TABLE t1 (ts timestamp time index, val double, host string primary key) engine = metric with ("on_physical_table" = "phy");


insert into
    t1
values
    (0, 1.0, 'a'),
    (1, 2.0, 'b'),
    (2, 3.0, 'a'),
    (3, 4.0, 'c'),
    (4, 5.0, 'a'),
    (5, 6.0, 'b'),
    (6, 7.0, 'a'),
    (7, 8.0, 'c'),
    (8, 9.0, 'd');

select last_value(ts order by ts) from t1;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
explain
    select last_value(ts order by ts) from t1;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
explain analyze
    select last_value(ts order by ts) from t1;

select
    last_value(host order by ts) as ordered_host,
    last_value(val order by ts)
from t1
group by host
order by ordered_host;


-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
explain 
select
    last_value(host order by ts) as ordered_host,
    last_value(val order by ts)
from t1
group by host
order by ordered_host;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
explain analyze 
select
    last_value(host order by ts) as ordered_host,
    last_value(val order by ts)
from t1
group by host
order by ordered_host;

drop table t1;
drop table phy;