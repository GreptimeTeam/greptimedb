-- Edge-case coverage for the selected pre-MergeScan optimizer prepass.
-- These cases focus on keeping distributed pushdown safe around subqueries,
-- nullable join keys, deterministic expressions, and nested predicates.

CREATE TABLE fact (
    ts TIMESTAMP(3) TIME INDEX,
    k STRING,
    val DOUBLE,
    `region` STRING,
    PRIMARY KEY (k)
) ENGINE = mito;

CREATE TABLE dim (
    ts TIMESTAMP(3) TIME INDEX,
    k STRING,
    label STRING,
    PRIMARY KEY (k)
) ENGINE = mito;

CREATE TABLE vals (
    ts TIMESTAMP(3) TIME INDEX,
    x INT,
    y INT
) ENGINE = mito;

INSERT INTO fact VALUES
    ('2024-01-30 00:00:00', 'a', 1.0, 'us'),
    ('2024-01-30 01:00:00', 'b', 2.0, 'eu'),
    ('2024-01-30 02:00:00', 'c', 3.0, 'us'),
    ('2024-01-30 03:00:00', 'd', 0.5, 'eu');

INSERT INTO dim VALUES
    ('2024-01-30 00:00:00', 'a', 'label_a'),
    ('2024-01-31 00:00:00', 'c', 'label_c'),
    ('2024-02-01 00:00:00', NULL, 'label_null'),
    ('2024-02-02 00:00:00', 'e', 'label_e');

INSERT INTO vals VALUES
    ('2024-01-30 00:00:00', 1, 10),
    ('2024-01-30 01:00:00', 2, 20),
    ('2024-01-30 02:00:00', 3, NULL),
    ('2024-01-30 03:00:00', NULL, 40);

ADMIN FLUSH_TABLE('fact');
ADMIN FLUSH_TABLE('dim');
ADMIN FLUSH_TABLE('vals');

-- Correlated EXISTS plus LEFT JOIN: static fact-side filters should still be
-- pushed into the fact scan, while the correlated predicate is decorrelated
-- before PushDownFilter so no raw subquery or outer reference reaches
-- partial_filters.
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT f.k, f.val FROM fact f
  LEFT JOIN dim d ON f.k = d.k
  WHERE f.ts >= '2024-01-30 00:00:00'
    AND f.val > 1.0
    AND EXISTS (SELECT 1 FROM vals v WHERE v.x = CAST(f.val AS INT));

SELECT f.k, f.val, d.label FROM fact f
  LEFT JOIN dim d ON f.k = d.k
  WHERE f.ts >= '2024-01-30 00:00:00'
    AND f.val > 1.0
    AND EXISTS (SELECT 1 FROM vals v WHERE v.x = CAST(f.val AS INT))
  ORDER BY f.k;

-- Semi/anti joins with nullable build-side keys. NULL keys from dim must not
-- produce matches, and must not suppress NOT EXISTS results.
SELECT f.k, f.val FROM fact f
  WHERE f.k IN (SELECT k FROM dim)
  ORDER BY f.k;

SELECT f.k, f.val FROM fact f
  WHERE EXISTS (SELECT 1 FROM dim d WHERE d.k = f.k)
  ORDER BY f.k;

SELECT f.k, f.val FROM fact f
  WHERE NOT EXISTS (SELECT 1 FROM dim d WHERE d.k = f.k)
  ORDER BY f.k;

-- Deterministic expression pushdown: casts may be pushed, but only as scan-local
-- expressions that the remote scan can evaluate.
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT * FROM fact WHERE CAST(fact.val AS INT) > 0;

CREATE TABLE edge_events (
    ts TIMESTAMP(3) TIME INDEX,
    host STRING,
    rack INT,
    cpu DOUBLE,
    PRIMARY KEY (host)
)
PARTITION ON COLUMNS (rack) (
    rack < 10,
    rack >= 10 AND rack < 20,
    rack >= 20
) ENGINE = mito;

INSERT INTO edge_events VALUES
    ('2024-01-30 00:00:00', 'h1', 5,  0.5),
    ('2024-01-30 01:00:00', 'h2', 15, 0.8),
    ('2024-01-30 02:00:00', 'h3', 25, 0.3),
    ('2024-01-30 03:00:00', 'h4', 25, 0.1);

ADMIN FLUSH_TABLE('edge_events');

-- Nested OR/AND over a partition column plus non-partition predicates. This
-- covers filter extraction without treating conjunctive scan filters as
-- independent top-level pruning predicates.
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT * FROM edge_events
  WHERE (rack > 5 AND cpu > 0.4) OR (host = 'h3');

SELECT host, rack, cpu FROM edge_events
  WHERE (rack > 5 AND cpu > 0.4) OR (host = 'h3')
  ORDER BY host;

DROP TABLE fact;
DROP TABLE dim;
DROP TABLE vals;
DROP TABLE edge_events;
