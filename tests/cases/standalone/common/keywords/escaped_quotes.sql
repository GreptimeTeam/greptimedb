-- Migrated from DuckDB test: test/sql/keywords/escaped_quotes_expressions.test
-- Note: Schema names don't support escaped quotes in GreptimeDB

CREATE TABLE test_table("COL""UMN" VARCHAR, "NA""ME" VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO test_table VALUES ('ALL', 'test', 1000);

-- Column references with escaped quotes
SELECT "COL""UMN" FROM test_table;

-- Multiple escaped quote columns
SELECT "COL""UMN", "NA""ME" FROM test_table;

-- Table-qualified references
SELECT test_table."COL""UMN", test_table."NA""ME" FROM test_table;

DROP TABLE test_table;
