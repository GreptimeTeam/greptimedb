-- Migrated from DuckDB test: test/sql/join/inner/test_join_with_nulls.test
-- Tests JOIN behavior with NULL values

CREATE TABLE null_left("id" INTEGER, val VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE null_right("id" INTEGER, val VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO null_left VALUES (1, 'A', 1000), (2, NULL, 2000), (NULL, 'C', 3000), (4, 'D', 4000);

INSERT INTO null_right VALUES (1, 'X', 5000), (NULL, 'Y', 6000), (3, 'Z', 7000), (4, NULL, 8000);

-- INNER JOIN with NULLs (NULLs don't match)
SELECT * FROM null_left l INNER JOIN null_right r ON l."id" = r."id" ORDER BY l.ts;

-- LEFT JOIN with NULLs
SELECT * FROM null_left l LEFT JOIN null_right r ON l."id" = r."id" ORDER BY l.ts;

-- JOIN on string columns with NULLs
SELECT * FROM null_left l INNER JOIN null_right r ON l.val = r.val ORDER BY l.ts;

-- JOIN with IS NOT DISTINCT FROM (treats NULL=NULL as true)
SELECT * FROM null_left l INNER JOIN null_right r ON l."id" IS NOT DISTINCT FROM r."id" ORDER BY l.ts;

-- JOIN with NULL-safe conditions
SELECT * FROM null_left l INNER JOIN null_right r ON (l."id" = r."id" OR (l."id" IS NULL AND r."id" IS NULL)) ORDER BY l.ts;

DROP TABLE null_right;

DROP TABLE null_left;
