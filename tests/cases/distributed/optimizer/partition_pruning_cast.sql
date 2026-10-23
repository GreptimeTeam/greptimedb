CREATE TABLE partition_pruning_cast_partitioned (
    ts TIMESTAMP TIME INDEX,
    k FLOAT PRIMARY KEY,
    v BIGINT,
)
PARTITION ON COLUMNS (k) (
    k < 1.25,
    k >= 1.25
);

CREATE TABLE partition_pruning_cast_control (
    ts TIMESTAMP TIME INDEX,
    k FLOAT PRIMARY KEY,
    v BIGINT,
);

INSERT INTO partition_pruning_cast_partitioned VALUES
    (1000, 1.0, 10),
    (1000, 1.5, 15),
    (1000, 2.5, 25);
INSERT INTO partition_pruning_cast_control VALUES
    (1000, 1.0, 10),
    (1000, 1.5, 15),
    (1000, 2.5, 25);

SELECT k, v, CAST(k AS BIGINT) AS cast_k
FROM partition_pruning_cast_partitioned ORDER BY k;
SELECT k, v, CAST(k AS BIGINT) AS cast_k
FROM partition_pruning_cast_control ORDER BY k;

-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
EXPLAIN SELECT k, v, CAST(k AS BIGINT) AS cast_k
FROM partition_pruning_cast_partitioned
WHERE CAST(k AS BIGINT) = 1;

SELECT ts, k, v, CAST(k AS BIGINT) AS cast_k
FROM partition_pruning_cast_partitioned
WHERE CAST(k AS BIGINT) = 1
ORDER BY k;
SELECT ts, k, v, CAST(k AS BIGINT) AS cast_k
FROM partition_pruning_cast_control
WHERE CAST(k AS BIGINT) = 1
ORDER BY k;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE SELECT k, v
FROM partition_pruning_cast_partitioned
WHERE k >= 1.25::FLOAT AND k < 2.5::FLOAT;
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE SELECT k, v
FROM partition_pruning_cast_partitioned
WHERE k >= 1.25::FLOAT AND k < 2.5::FLOAT AND CAST(v AS INT) IS NOT NULL;
SELECT k, v FROM partition_pruning_cast_partitioned
WHERE k >= 1.25::FLOAT AND k < 2.5::FLOAT AND CAST(v AS INT) IS NOT NULL;

SELECT k FROM partition_pruning_cast_partitioned
WHERE k = 1.0 OR CAST(k AS BIGINT) = 1 ORDER BY k;

DROP TABLE partition_pruning_cast_partitioned;
DROP TABLE partition_pruning_cast_control;
