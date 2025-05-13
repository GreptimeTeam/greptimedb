-- no partition
create table t (
    i double,
    j timestamp time index,
    k string primary key
);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
tql analyze (0, 10, '1s') 100 - (avg by (k) (irate(t[1m])) * 100);

drop table t;

-- partition on tag
create table t (
    i double,
    j timestamp time index,
    k string,
    l string,
    primary key (k, l)
) partition on columns (k, l) (k < 'a', k >= 'a');

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
tql analyze (0, 10, '1s') 100 - (avg by (k) (irate(t[1m])) * 100);

drop table t;

-- partition on value
create table t (
    i double,
    j timestamp time index,
    k string,
    l string,
    primary key (k, l)
) partition on columns (i) (i < 1.0, i >= 1.0);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
tql analyze (0, 10, '1s') 100 - (avg by (k) (irate(t[1m])) * 100);

drop table t;
