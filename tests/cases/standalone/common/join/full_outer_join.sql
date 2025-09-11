-- Migrated from DuckDB test: test/sql/join/full_outer/test_full_outer_join.test
-- Tests FULL OUTER JOIN scenarios

CREATE TABLE left_full("id" INTEGER, "name" VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE right_full("id" INTEGER, "value" INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO left_full VALUES (1, 'Alice', 1000), (2, 'Bob', 2000), (3, 'Carol', 3000);

INSERT INTO right_full VALUES (2, 200, 4000), (3, 300, 5000), (4, 400, 6000);

-- Basic FULL OUTER JOIN
SELECT * FROM left_full l FULL OUTER JOIN right_full r ON l."id" = r."id" ORDER BY l."id" NULLS LAST, r."id" NULLS LAST;

-- FULL OUTER JOIN with WHERE on result
SELECT * FROM left_full l FULL OUTER JOIN right_full r ON l."id" = r."id" WHERE l."name" IS NULL OR r."value" IS NULL ORDER BY l."id" NULLS LAST, r."id" NULLS LAST;

-- FULL OUTER JOIN with complex conditions
SELECT * FROM left_full l FULL OUTER JOIN right_full r ON l."id" = r."id" AND r."value" > 250 ORDER BY l."id" NULLS LAST, r."id" NULLS LAST;

DROP TABLE right_full;

DROP TABLE left_full;
