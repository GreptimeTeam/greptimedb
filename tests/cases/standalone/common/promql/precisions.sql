CREATE TABLE host_sec (
  ts timestamp(0) time index,
  host STRING PRIMARY KEY,
  val DOUBLE,
);

INSERT INTO TABLE host_sec VALUES
    (0,  'host1', 1),
    (0,  'host2', 2),
    (5,  'host1', 3),
    (5,  'host2', 4),
    (10, 'host1', 5),
    (10, 'host2', 6),
    (15, 'host1', 7),
    (15, 'host2', 8);

CREATE TABLE host_micro (
  ts timestamp(6) time index,
  host STRING PRIMARY KEY,
  val DOUBLE,
);

INSERT INTO TABLE host_micro VALUES
    (0,        'host1', 1),
    (0,        'host2', 2),
    (5000000,  'host1', 3),
    (5000000,  'host2', 4),
    (10000000, 'host1', 5),
    (10000000, 'host2', 6),
    (15000000, 'host1', 7),
    (15000000, 'host2', 8);

CREATE TABLE host_nano (
  ts timestamp(9) time index,
  host STRING PRIMARY KEY,
  val DOUBLE,
);

INSERT INTO TABLE host_nano VALUES
    (0,           'host1', 1),
    (0,           'host2', 2),
    (5000000000,  'host1', 3),
    (5000000000,  'host2', 4),
    (10000000000, 'host1', 5),
    (10000000000, 'host2', 6),
    (15000000000, 'host1', 7),
    (15000000000, 'host2', 8);

-- Test on Timestamps of different precisions

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') host_sec{host="host1"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') avg_over_time(host_sec{host="host1"}[5s]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') host_micro{host="host1"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') avg_over_time(host_micro{host="host1"}[5s]);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') host_sec{host="host1"} + host_micro{host="host1"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') avg_over_time(host_sec{host="host1"}[5s]) + avg_over_time(host_micro{host="host1"}[5s]);

-- Verify that PromQL time predicates on non-millisecond time indexes are
-- pushed into the scan as native timestamp range filters.
-- Original PromQL instant selector filter is built on the millisecond alias:
--   host = "host1" AND ts_ms >= -299999ms AND ts_ms <= 10000ms
-- After pushing through `CAST(ts_ns AS Timestamp(ms)) AS ts` and applying
-- DataFusion cast preimage, it becomes native nanosecond half-open bounds:
--   host = "host1" AND ts_ns >= -299999999999ns AND ts_ns < 10001000000ns
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
-- SQLNESS REPLACE host_nano.__table_id\s*=\s*UInt32\(\d+\) host_nano.__table_id=UInt32(REDACTED)
TQL EXPLAIN (0, 10, '5s') host_nano{host="host1"};

DROP TABLE host_sec;

DROP TABLE host_micro;

DROP TABLE host_nano;
