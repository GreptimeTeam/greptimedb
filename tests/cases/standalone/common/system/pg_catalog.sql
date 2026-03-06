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
SELECT
    CASE WHEN
            quote_ident(table_schema) IN (
            SELECT
            CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END
            FROM
            generate_series(
                array_lower(string_to_array(current_setting('search_path'),','),1),
                array_upper(string_to_array(current_setting('search_path'),','),1)
            ) as i,
            string_to_array(current_setting('search_path'),',') s
            )
        THEN quote_ident(table_name)
        ELSE quote_ident(table_schema) || '.' || quote_ident(table_name)
    END AS "table"
    FROM information_schema.tables
    WHERE quote_ident(table_schema) NOT IN ('information_schema',
                                'pg_catalog',
                                '_timescaledb_cache',
                                '_timescaledb_catalog',
                                '_timescaledb_internal',
                                '_timescaledb_config',
                                'timescaledb_information',
                                'timescaledb_experimental')
    ORDER BY CASE WHEN
            quote_ident(table_schema) IN (
            SELECT
            CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END
            FROM
            generate_series(
                array_lower(string_to_array(current_setting('search_path'),','),1),
                array_upper(string_to_array(current_setting('search_path'),','),1)
            ) as i,
            string_to_array(current_setting('search_path'),',') s
            ) THEN 0 ELSE 1 END, 1;

-- SQLNESS PROTOCOL POSTGRES
SELECT quote_ident(column_name) AS "column", data_type AS "type"
    FROM information_schema.columns
    WHERE
        CASE WHEN array_length(parse_ident('my_db.foo'),1) = 2
        THEN quote_ident(table_schema) = (parse_ident('my_db.foo'))[1]
            AND quote_ident(table_name) = (parse_ident('my_db.foo'))[2]
        ELSE quote_ident(table_name) = 'my_db.foo'
            AND
            quote_ident(table_schema) IN (
            SELECT
            CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END
            FROM
            generate_series(
                array_lower(string_to_array(current_setting('search_path'),','),1),
                array_upper(string_to_array(current_setting('search_path'),','),1)
            ) as i,
            string_to_array(current_setting('search_path'),',') s
            )
        END;

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

-- PostgreSQL description functions - placeholder returning NULL for compatibility

-- SQLNESS PROTOCOL POSTGRES
SELECT obj_description((SELECT oid FROM pg_class LIMIT 1), 'pg_class') IS NULL AS is_null;

-- SQLNESS PROTOCOL POSTGRES
SELECT obj_description((SELECT oid FROM pg_class LIMIT 1)) IS NULL AS is_null;

-- SQLNESS PROTOCOL POSTGRES
SELECT col_description((SELECT oid FROM pg_class LIMIT 1), 1) IS NULL AS is_null;

-- SQLNESS PROTOCOL POSTGRES
SELECT shobj_description(1, 'pg_database') IS NULL AS is_null;

-- pg_my_temp_schema returns 0 (no temp schema support)
-- SQLNESS PROTOCOL POSTGRES
SELECT pg_my_temp_schema();

-- Issue 7313
-- SQLNESS PROTOCOL POSTGRES
-- SQLNESS REPLACE (\d+\s*) OID
SELECT
	oid
	,nspname
	,nspname = ANY (current_schemas(true)) AS is_on_search_path

	    ,obj_description(oid, 'pg_namespace') AS comment

FROM pg_namespace; SELECT
oid
,nspname
FROM pg_namespace
WHERE oid = pg_my_temp_schema();

-- SQLNESS PROTOCOL POSTGRES
CREATE table foo
(
    ts TIMESTAMP TIME INDEX,
    log_data TEXT,
    count_num BIGINT,
);

-- SQLNESS PROTOCOL POSTGRES
SELECT attname, atttypid FROM pg_catalog.pg_class AS cls INNER JOIN
pg_catalog.pg_attribute AS attr ON cls.oid = attr.attrelid INNER JOIN
pg_catalog.pg_type AS typ ON attr.atttypid = typ.oid WHERE attr.attnum >= 0 AND
cls.oid = 'foo'::regclass::oid ORDER BY attr.attnum;

-- SQLNESS PROTOCOL POSTGRES
DROP TABLE foo;
