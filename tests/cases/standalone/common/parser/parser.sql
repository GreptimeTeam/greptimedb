
-- columns aliases, from:
-- https://github.com/duckdb/duckdb/blob/9196dd9b0a163e6c8aada26218803d04be30c562/test/sql/parser/columns_aliases.test

CREATE TABLE integers (ts TIMESTAMP TIME INDEX, i INT, j INT);

INSERT INTO integers SELECT 0::TIMESTAMP ts, 42 i, 84 j UNION ALL SELECT 1::TIMESTAMP, 13, 14;

SELECT i, j FROM (SELECT COLUMNS(*)::VARCHAR FROM integers);

SELECT i, j FROM (SELECT * FROM integers);

SELECT min_i, min_j, max_i, max_j FROM (SELECT MIN(i) AS "min_i", MAX(i) AS "max_i", MIN(j) AS "min_j", MAX(j) AS "max_j" FROM integers);

DROP TABLE integers;

-- skipped, unsupported feature: digit separators
-- SELECT 1_000_000;

-- skipped, unsupported feature: division operator precedence
-- SELECT 6 + 1 // 2;

-- expression depth, from:
-- https://github.com/duckdb/duckdb/blob/9196dd9b0a163e6c8aada26218803d04be30c562/test/sql/parser/expression_depth_limit.test
SELECT (1+(1+(1+(1+(1+(1+(1+1)))))));

-- skipped, unsupported feature: dollar quotes
-- SELECT $$$$ = '';

-- skipped, unsupported feature: from_first, see also:
-- https://github.com/GreptimeTeam/greptimedb/issues/5012
-- FROM integers;

-- skipped, unsupported feature: function chaining
-- SELECT "abcd".upper().lower();
