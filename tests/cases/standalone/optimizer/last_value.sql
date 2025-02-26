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

drop table t;
