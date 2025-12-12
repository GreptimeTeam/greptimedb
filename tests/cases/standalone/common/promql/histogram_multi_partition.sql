-- Minimal repro for histogram quantile over multi-partition input.
create table histogram_gap_bucket (
    ts timestamp time index,
    le string,
    shard string,
    val double,
    primary key (shard, le)
) partition on columns (shard) (
    shard < 'n',
    shard >= 'n'
);

insert into histogram_gap_bucket values
    (0, '0.5', 'a', 1),
    (0, '1', 'a', 2),
    (0, '+Inf', 'a', 2),
    (0, '0.5', 'z', 2),
    (0, '1', 'z', 4),
    (0, '+Inf', 'z', 4),
    (10000, '0.5', 'a', 1),
    (10000, '1', 'a', 2),
    (10000, '+Inf', 'a', 2),
    (10000, '0.5', 'z', 1),
    (10000, '1', 'z', 3),
    (10000, '+Inf', 'z', 3);

-- Ensure the physical plan keeps the required repartition/order before folding buckets.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
tql analyze (0, 10, '10s') histogram_quantile(0.5, sum by (le) (histogram_gap_bucket));

-- SQLNESS SORT_RESULT 2 1
tql eval (0, 10, '10s') histogram_quantile(0.5, sum by (le) (histogram_gap_bucket));

drop table histogram_gap_bucket;
