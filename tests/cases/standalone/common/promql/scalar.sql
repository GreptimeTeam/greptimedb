CREATE TABLE host (
  ts timestamp(3) time index,
  host STRING PRIMARY KEY,
  val BIGINT,
);

INSERT INTO TABLE host VALUES
    (0,     'host1', 1),
    (0,     'host2', 2),
    (5000,  'host1', 3),
    (5000,  'host2', 4),
    (10000, 'host1', 5),
    (10000, 'host2', 6),
    (15000, 'host1', 7),
    (15000, 'host2', 8);

-- case only have one time series, scalar return value

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host{host="host1"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host{host="host1"}) + 1;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') 1 + scalar(host{host="host1"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host{host="host1"}) + scalar(host{host="host2"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') host{host="host1"} + scalar(host{host="host2"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host{host="host1"}) + host{host="host2"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') host + scalar(host{host="host2"});
 
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host{host="host1"}) + host;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(count(count(host) by (host)));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(count(sum(host) by (host)));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(count(avg(host) by (host)));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(count(stddev(host) by (host)));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host{host="host1"} + scalar(host{host="host2"}));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(scalar(host{host="host2"}) + host{host="host1"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host + scalar(host{host="host2"}));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(scalar(host{host="host2"}) + host);

-- case have multiple time series, scalar return NaN

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host) + 1;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') 1 + scalar(host);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host) + scalar(host);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') host + scalar(host);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host) + host;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') host{host="host2"} + scalar(host);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host) + host{host="host2"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(host{host="host1"} + scalar(host));

-- No data input in scalar
TQL EVAL (350, 360, '5s') scalar(host{host="host1"});

DELETE from host where ts = 0;

-- Under this case, InstantManipulate will input a valid record batch but output a empty record batch (because no data will be selected in this batch)
-- Test input a empty record batch to ScalarCalculate plan
TQL EVAL (0, 1600, '6m40s') scalar(host{host="host1"});

-- error case

TQL EVAL (0, 15, '5s') scalar(1 + scalar(host{host="host2"}));

TQL EVAL (0, 15, '5s') scalar(scalar(host{host="host2"}) + 1);

TQL EVAL (0, 15, '5s') scalar(scalar(host{host="host1"}) + scalar(host{host="host2"}));

-- Test clamp functions with vector input and scalar bounds

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') clamp(host, 0, 12);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') clamp(host, 6 - 6, 6 + 6);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') clamp(host, 12, 0);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') clamp(host{host="host1"}, -1, 6);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') clamp_min(host{host="host1"}, 10);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') clamp_min(host{host="host1"}, 1);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') clamp_max(host{host="host1"}, 1);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') clamp_max(host{host="host1"}, 10);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') clamp_min(host, 1);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') clamp_max(host, 10);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(clamp(host{host="host1"}, 0, 6));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(clamp_min(host{host="host1"}, 1));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') scalar(clamp_max(host{host="host1"}, 10));

-- Test nested clamp functions
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') clamp(clamp_min(host{host="host1"}, 1), 0, 12);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') clamp_max(clamp(host{host="host1"}, 0, 15), 6);

DROP TABLE host;

CREATE TABLE presence_metric (
  ts timestamp(3) time index,
  instance STRING,
  cpu STRING,
  shard STRING,
  val DOUBLE,
  PRIMARY KEY (instance, cpu, shard),
);

INSERT INTO TABLE presence_metric VALUES
    (0,      'i1', 'cpu0', 'a', 1.0),
    (0,      'i1', 'cpu0', 'b', 2.0),
    (0,      'i1', 'cpu1', 'a', 10.0),
    (0,      'i1', 'cpu2', 'a', 20.0),
    (0,      'i2', 'cpu9', 'a', 100.0),
    (200000, 'i1', 'cpu0', 'a', 'NAN'::DOUBLE),
    (200000, 'i1', 'cpu0', 'b', 'NAN'::DOUBLE),
    (200000, 'i1', 'cpu1', 'a', 11.0),
    (200000, 'i1', 'cpu2', 'a', NULL),
    (200000, 'i2', 'cpu9', 'a', 101.0),
    (400000, 'i1', 'cpu1', 'a', 12.0),
    (400000, 'i2', 'cpu9', 'a', 102.0),
    (600000, 'i1', 'cpu0', 'a', 7.0),
    (600000, 'i1', 'cpu0', 'b', 8.0),
    (600000, 'i2', 'cpu9', 'a', 103.0);

-- NaN drops `cpu0` from the grouped count, while the NULL sample on `cpu2`
-- still leaves a zero-valued row in `count(...) by (cpu)`.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 600, '200s') count(presence_metric{instance="i1"}) by (cpu);

-- Nested-count rewrite should preserve grouped presence after stale-NaN filtering and null-value pruning.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 600, '200s') scalar(count(count(presence_metric{instance="i1"}) by (cpu)));

-- Non-count inner aggregates must drop NULL-only groups before the outer count.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 600, '200s') scalar(count(sum(presence_metric{instance="i1"}) by (cpu)));

-- False case: outer `by (instance)` keeps multiple series at the scalar input, so scalar should still yield NaN.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 600, '200s') scalar(count(count(presence_metric) by (instance, cpu)) by (instance));

DROP TABLE presence_metric;
