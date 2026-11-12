create table last_value_ignore_nulls (
    ts timestamp time index,
    host string primary key,
    v1 double,
    v2 double,
);

insert into last_value_ignore_nulls values
    (1000, 'a', 42.0, null),
    (2000, 'a', null, 20.0),
    (1000, 'b', null, null),
    (2000, 'b', null, null);

select
    host,
    last_value(v1 order by ts) ignore nulls as v1_ignore,
    last_value(v2 order by ts) ignore nulls as v2_ignore,
    last_value(v1 order by ts) as v1_default,
    last_value(v1 order by ts) respect nulls as v1_respect
from last_value_ignore_nulls
group by host
order by host;

-- Default and RESPECT NULLS return the same results in both modes.
select
    host,
    last_value(v1 order by ts) as v1_default,
    last_value(v1 order by ts) respect nulls as v1_respect
from last_value_ignore_nulls
group by host
order by host;

-- The IGNORE NULLS member rejects the shared selector, including when it is second.
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \\?"(dyn_filters|time_range|file_summary|max_part_range)\\?":.* "$1":REDACTED
explain analyze select
    last_value(v1 order by ts),
    last_value(v2 order by ts) ignore nulls
from last_value_ignore_nulls
group by host;

admin flush_table('last_value_ignore_nulls');

select
    host,
    last_value(v1 order by ts) ignore nulls as v1_ignore,
    last_value(v2 order by ts) ignore nulls as v2_ignore,
    last_value(v1 order by ts) as v1_default,
    last_value(v1 order by ts) respect nulls as v1_respect
from last_value_ignore_nulls
group by host
order by host;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \\?"(dyn_filters|time_range|file_summary|max_part_range)\\?":.* "$1":REDACTED
explain analyze select
    last_value(v1 order by ts),
    last_value(v2 order by ts) ignore nulls
from last_value_ignore_nulls
group by host;

drop table last_value_ignore_nulls;
