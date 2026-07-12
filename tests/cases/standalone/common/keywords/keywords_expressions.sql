-- Migrated from DuckDB test: test/sql/keywords/keywords_in_expressions.test

CREATE SCHEMA "SCHEMA";

CREATE TABLE "SCHEMA"."TABLE"("COLUMN" VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO "SCHEMA"."TABLE" VALUES ('ALL', 1000);

-- Column references with quoted keywords
SELECT "COLUMN" FROM "SCHEMA"."TABLE";

-- Table-qualified column reference
SELECT "TABLE"."COLUMN" FROM "SCHEMA"."TABLE";

-- Schema-qualified column reference
SELECT "SCHEMA"."TABLE"."COLUMN" FROM "SCHEMA"."TABLE";

DROP TABLE "SCHEMA"."TABLE";
DROP SCHEMA "SCHEMA";

-- Common reserved keywords
CREATE TABLE "SELECT"("WHERE" INTEGER, "FROM" VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO "SELECT" VALUES (1, 'test', 2000);

SELECT "WHERE", "FROM" FROM "SELECT";

-- Qualified references
SELECT "SELECT"."WHERE", "SELECT"."FROM" FROM "SELECT";

DROP TABLE "SELECT";
