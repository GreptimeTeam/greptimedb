-- should not able to create pg_catalog
create database pg_catalog;

-- session_user because session_user is based on the current user so is not null is for test
-- SQLNESS PROTOCOL POSTGRES
SELECT session_user is not null;

-- SQLNESS REPLACE PostgreSQL.* VERSION
-- current_schema
-- SQLNESS PROTOCOL POSTGRES
select current_schema(), current_schemas(true), current_schemas(false), version(), current_database();

-- search_path for pg using schema for now FIXME when support real search_path
-- SQLNESS PROTOCOL POSTGRES
show search_path;

-- set search_path for pg using schema for now FIXME when support real search_path
create database test;
-- SQLNESS PROTOCOL POSTGRES
set search_path to 'test';
drop database test;
-- SQLNESS PROTOCOL POSTGRES
set search_path to 'public';
-- SQLNESS PROTOCOL POSTGRES
set search_path = public;

-- SQLNESS PROTOCOL POSTGRES
select current_schema();

-- make sure all the pg_catalog tables are only visible to postgres
select * from pg_catalog.pg_class;
select * from pg_catalog.pg_namespace;
select * from pg_catalog.pg_type;
select * from pg_catalog.pg_database;

-- SQLNESS PROTOCOL POSTGRES
select * from pg_catalog.pg_type order by oid;

-- SQLNESS PROTOCOL POSTGRES
-- SQLNESS REPLACE (\d+\s*) OID
select * from pg_catalog.pg_database where datname = 'public';

-- \d
-- SQLNESS PROTOCOL POSTGRES
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p','v','m','S','f','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;

-- \dt
-- SQLNESS PROTOCOL POSTGRES
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;

-- make sure oid of namespace keep stable
-- SQLNESS PROTOCOL POSTGRES
-- SQLNESS REPLACE (\d+\s*) OID
SELECT * FROM pg_namespace ORDER BY nspname;

-- SQLNESS PROTOCOL POSTGRES
create database my_db;

-- SQLNESS PROTOCOL POSTGRES
use my_db;

-- SQLNESS PROTOCOL POSTGRES
create table foo
(
    ts TIMESTAMP TIME INDEX
);

-- show tables in `my_db`
-- SQLNESS PROTOCOL POSTGRES
select relname
from pg_catalog.pg_class
where relnamespace = (
    select oid
    from pg_catalog.pg_namespace
    where nspname = 'my_db'
);

-- \dt
-- SQLNESS PROTOCOL POSTGRES
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;

-- show tables in `my_db`, `public`
-- SQLNESS PROTOCOL POSTGRES
select relname
from pg_catalog.pg_class
where relnamespace in (
    select oid
    from pg_catalog.pg_namespace
    where nspname = 'my_db' or nspname = 'public'
)
order by relname;

-- SQLNESS PROTOCOL POSTGRES
select relname
from pg_catalog.pg_class
where relnamespace in (
    select oid
    from pg_catalog.pg_namespace
    where nspname like 'my%'
);

-- SQLNESS PROTOCOL POSTGRES
-- SQLNESS REPLACE (\d+\s*) OID
select relnamespace, relname, relkind
from pg_catalog.pg_class
where relnamespace in (
    select oid
    from pg_catalog.pg_namespace
    where nspname <> 'public'
      and nspname <> 'information_schema'
      and nspname <> 'pg_catalog'
)
order by relnamespace, relname;

-- SQLNESS PROTOCOL POSTGRES
use public;

-- SQLNESS PROTOCOL POSTGRES
drop schema my_db;

-- SQLNESS PROTOCOL POSTGRES
use pg_catalog;

-- pg_class
-- SQLNESS PROTOCOL POSTGRES
desc table pg_class;

-- SQLNESS PROTOCOL POSTGRES
desc table pg_namespace;

-- SQLNESS PROTOCOL POSTGRES
drop table my_db.foo;

-- SQLNESS PROTOCOL POSTGRES
use public;

-- ======================================================
-- PostgreSQL description functions for connector compatibility
-- ======================================================

-- Create a table with comment for testing
-- SQLNESS PROTOCOL POSTGRES
CREATE TABLE test_desc_table (
  ts TIMESTAMP TIME INDEX,
  val INT COMMENT 'Value column',
  item_name STRING COMMENT 'Name of the item'
) COMMENT = 'Test table for description functions';

-- Test obj_description with table OID
-- Note: We need to get the table_id first. In tests, we can use a subquery approach.
-- SQLNESS PROTOCOL POSTGRES
-- SQLNESS REPLACE (\d+\s*) OID
SELECT table_id, table_comment FROM information_schema.tables WHERE table_name = 'test_desc_table';

-- Test obj_description with non-existent OID (should return NULL)
-- SQLNESS PROTOCOL POSTGRES
SELECT obj_description(99999999, 'pg_class') IS NULL AS is_null;

-- Test obj_description with non-pg_class catalog (should return NULL)
-- SQLNESS PROTOCOL POSTGRES
SELECT obj_description(1234, 'pg_namespace') IS NULL AS is_null;

-- Test shobj_description (should always return NULL in GreptimeDB)
-- SQLNESS PROTOCOL POSTGRES
SELECT shobj_description(1, 'pg_database') IS NULL AS is_null;

-- SQLNESS PROTOCOL POSTGRES
SELECT shobj_description(1, 'pg_authid') IS NULL AS is_null;

-- Clean up test table
-- SQLNESS PROTOCOL POSTGRES
DROP TABLE test_desc_table;
