--- test CREATE VIEW ---
CREATE TABLE test_table(a STRING, ts TIMESTAMP TIME INDEX);

CREATE VIEW test_view;

CREATE VIEW test_view as DELETE FROM public.numbers;

--- Table already exists ---
CREATE VIEW test_table as SELECT * FROM public.numbers;

--- Table already exists even when create_if_not_exists ---
CREATE VIEW IF NOT EXISTS test_table as SELECT * FROM public.numbers;

--- Table already exists even when or_replace ---
CREATE OR REPLACE VIEW test_table as SELECT * FROM public.numbers;

CREATE VIEW test_view as SELECT * FROM public.numbers;

--- View already exists ----
CREATE VIEW test_view as SELECT * FROM public.numbers;

CREATE VIEW IF NOT EXISTS test_view as SELECT * FROM public.numbers;

CREATE OR REPLACE VIEW test_view as SELECT * FROM public.numbers;

SHOW TABLES;

SHOW FULL TABLES;

-- psql: \dv
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('v','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;

-- SQLNESS REPLACE (\s\d+\s) ID
-- SQLNESS REPLACE (\s[\-0-9T:\.]{15,}) DATETIME
-- SQLNESS REPLACE [\u0020\-]+
SELECT * FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME, TABLE_TYPE;

-- SQLNESS REPLACE (\s\d+\s) ID
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'VIEW';

SHOW COLUMNS FROM test_view;

SHOW FULL COLUMNS FROM test_view;

SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'test_view';

SELECT * FROM test_view LIMIT 10;

DROP VIEW test_view;

DROP TABLE test_table;

SELECT * FROM test_view LIMIT 10;

SHOW TABLES;

-- psql: \dv
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('v','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;
