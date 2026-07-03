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

-- Timestamp downcast equality should become a native half-open nanosecond range.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT host, v FROM cast_preimage_ts
WHERE CAST(ts AS TIMESTAMP(3)) = '1970-01-01 00:00:05'::TIMESTAMP(3)
ORDER BY host, v;

-- Literal-left timestamp inequality should normalize before cast preimage.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT host, v FROM cast_preimage_ts
WHERE '1970-01-01 00:00:05'::TIMESTAMP(3) < CAST(ts AS TIMESTAMP(3))
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

-- Swapped comparison and BETWEEN/NOT BETWEEN cover const-normalization overlap.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT host, v FROM cast_preimage_int
WHERE 2 <= CAST(v AS BIGINT)
ORDER BY host;

-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT host, v FROM cast_preimage_int
WHERE CAST(v AS BIGINT) BETWEEN 1 AND 2
ORDER BY host;

-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT host, v FROM cast_preimage_int
WHERE CAST(v AS BIGINT) NOT BETWEEN 1 AND 2
ORDER BY host;

-- Plain and casted constants should become scan-local typed literals.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT host, v FROM cast_preimage_int
WHERE v >= 2
ORDER BY host;

-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT host, v FROM cast_preimage_int
WHERE v >= CAST(2 AS BIGINT)
ORDER BY host;

-- TRY_CAST widening should also simplify to a scan-local typed literal.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT host, v FROM cast_preimage_int
WHERE TRY_CAST(v AS BIGINT) >= 2
ORDER BY host;

-- Unsafe narrowing and int-to-string roundtrip stay result-correct.
SELECT host, v FROM cast_preimage_int
WHERE CAST(CAST(v AS TINYINT) AS SMALLINT) = v
ORDER BY host;

SELECT host, v FROM cast_preimage_int
WHERE CAST(CAST(v AS STRING) AS SMALLINT) = v
ORDER BY host;

CREATE TABLE cast_preimage_ts_ms (
  host STRING PRIMARY KEY,
  ts TIMESTAMP(3) TIME INDEX,
  v INTEGER,
);

INSERT INTO cast_preimage_ts_ms VALUES
    ('host1', 0, 1),
    ('host2', 5000, 2),
    ('host3', 5001, 3);

-- Timestamp widening equality is exact at millisecond precision.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT host, v FROM cast_preimage_ts_ms
WHERE CAST(ts AS TIMESTAMP(9)) = '1970-01-01 00:00:05'::TIMESTAMP(9)
ORDER BY host;

-- Non-exact nanosecond literal should remain semantically correct.
SELECT host, v FROM cast_preimage_ts_ms
WHERE CAST(ts AS TIMESTAMP(9)) = '1970-01-01 00:00:05.000000001'::TIMESTAMP(9)
ORDER BY host;

DROP TABLE cast_preimage_ts;

DROP TABLE cast_preimage_int;

DROP TABLE cast_preimage_ts_ms;
