-- Migrated from DuckDB test: test/sql/join/ hash join tests
-- Tests complex hash join scenarios

CREATE TABLE large_table_a("id" INTEGER, value_a VARCHAR, num_a INTEGER, ts TIMESTAMP TIME INDEX);

CREATE TABLE large_table_b("id" INTEGER, value_b VARCHAR, num_b INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO large_table_a VALUES
(1, 'alpha', 100, 1000), (2, 'beta', 200, 2000), (3, 'gamma', 300, 3000),
(4, 'delta', 400, 4000), (5, 'epsilon', 500, 5000), (6, 'zeta', 600, 6000),
(7, 'eta', 700, 7000), (8, 'theta', 800, 8000), (9, 'iota', 900, 9000),
(10, 'kappa', 1000, 10000);

INSERT INTO large_table_b VALUES
(2, 'second', 20, 1000), (4, 'fourth', 40, 2000), (6, 'sixth', 60, 3000),
(8, 'eighth', 80, 4000), (10, 'tenth', 100, 5000), (12, 'twelfth', 120, 6000),
(14, 'fourteenth', 140, 7000), (16, 'sixteenth', 160, 8000);

-- Hash join with exact match
SELECT
  a."id", a.value_a, a.num_a, b.value_b, b.num_b
FROM large_table_a a
INNER JOIN large_table_b b ON a."id" = b."id"
ORDER BY a."id";

-- Hash join with multiple key conditions
SELECT
  a."id", a.value_a, b.value_b
FROM large_table_a a
INNER JOIN large_table_b b ON a."id" = b."id" AND a.num_a > b.num_b * 5
ORDER BY a."id";

-- Hash join with aggregation on both sides
SELECT
  joined_data."id",
  joined_data.combined_num,
  joined_data.value_concat
FROM (
  SELECT
    a."id",
    a.num_a + b.num_b as combined_num,
    a.value_a || '-' || b.value_b as value_concat
  FROM large_table_a a
  INNER JOIN large_table_b b ON a."id" = b."id"
) joined_data
WHERE joined_data.combined_num > 500
ORDER BY joined_data.combined_num DESC;

-- Hash join with filtering on both tables
SELECT
  a.value_a, b.value_b, a.num_a, b.num_b
FROM large_table_a a
INNER JOIN large_table_b b ON a."id" = b."id"
WHERE a.num_a > 500 AND b.num_b < 100
ORDER BY a.num_a DESC;

-- Hash join for set operations
SELECT
  a."id",
  'Both Tables' as source,
  a.value_a as value_from_a,
  b.value_b as value_from_b
FROM large_table_a a
INNER JOIN large_table_b b ON a."id" = b."id"
UNION ALL
SELECT
  a."id",
  'Only Table A' as source,
  a.value_a,
  NULL as value_from_b
FROM large_table_a a
LEFT JOIN large_table_b b ON a."id" = b."id"
WHERE b."id" IS NULL
ORDER BY "id", source;

DROP TABLE large_table_a;

DROP TABLE large_table_b;
