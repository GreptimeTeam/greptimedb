-- SQL cast predicate preimage / const-normalization filter pushdown.
-- This is deliberately plain SQL, not PromQL/TQL, so it covers the generic
-- planner path where cast predicates should become scan-local filters.

CREATE TABLE cast_preimage_ts (
  host STRING PRIMARY KEY,
  ts TIMESTAMP(9) TIME INDEX,
  v INTEGER,
);

INSERT INTO cast_preimage_ts VALUES
    ('host1', 0, 1),
    ('host1', 5000000000, 2),
    ('host1', 10000000000, 3),
    ('host2', 15000000000, 4);

-- Timestamp downcast comparison:
--   CAST(ts_ns AS TIMESTAMP(3)) >= TIMESTAMP(3) '1970-01-01 00:00:05'
-- should be pushed as a native nanosecond bound on the scan.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT host, v FROM cast_preimage_ts
WHERE CAST(ts AS TIMESTAMP(3)) >= '1970-01-01 00:00:05'::TIMESTAMP(3)
ORDER BY host, v;

-- Timestamp downcast BETWEEN should be lowered through DataFusion's BETWEEN
-- simplification and cast preimage, not left as a CAST predicate in scan filters.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT host, v FROM cast_preimage_ts
WHERE CAST(ts AS TIMESTAMP(3)) BETWEEN '1970-01-01 00:00:00'::TIMESTAMP(3)
                                  AND '1970-01-01 00:00:10'::TIMESTAMP(3)
ORDER BY host, v;

-- Integer widening comparison and IN-list cover the overlap with the old
-- Greptime ConstNormalizationRule's lossless-cast cases.
CREATE TABLE cast_preimage_int (
  ts TIMESTAMP TIME INDEX,
  host STRING PRIMARY KEY,
  v SMALLINT,
);

INSERT INTO cast_preimage_int VALUES
    (0, 'host1', 1),
    (1000, 'host2', 2),
    (2000, 'host3', 3);

-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT host, v FROM cast_preimage_int
WHERE CAST(v AS BIGINT) >= 2
ORDER BY host;

-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT host, v FROM cast_preimage_int
WHERE CAST(v AS BIGINT) IN (1, 3)
ORDER BY host;

DROP TABLE cast_preimage_ts;

DROP TABLE cast_preimage_int;
