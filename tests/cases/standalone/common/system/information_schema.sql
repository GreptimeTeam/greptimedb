-- should not able to create information_schema
create database information_schema;

-- scripts table has different table ids in different modes
-- SQLNESS REPLACE (\s[\-0-9T:\.]{15,}) DATETIME
-- SQLNESS REPLACE [\u0020\-]+
select *
from information_schema.tables
where table_name != 'scripts'
order by table_schema, table_name;

select * from information_schema.columns order by table_schema, table_name, column_name;

select table_schema, table_name from information_schema.tables order by table_name limit 5;

select table_schema, table_name from information_schema.tables order by table_name limit 5 offset 0;

create
database my_db;

use information_schema;

use my_db;

create table foo
(
    ts TIMESTAMP TIME INDEX
);

select table_name
from information_schema.tables
where table_schema = 'my_db'
order by table_name;

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'my_db'
  AND create_time > NOW() - INTERVAL 1 MINUTE;

select table_name
from information_schema.tables
where table_schema in ('my_db', 'public')
order by table_name;

select table_name
from information_schema.tables
where table_schema like 'my%'
order by table_name;

select table_name
from information_schema.tables
where table_schema not in ('my_db', 'information_schema', 'pg_catalog')
order by table_name;

select table_catalog, table_schema, table_name, table_type, engine
from information_schema.tables
where table_catalog = 'greptime'
  and table_schema != 'public'
  and table_schema != 'information_schema'
  and table_schema != 'pg_catalog'
order by table_schema, table_name;

select table_catalog, table_schema, table_name, column_name, data_type, semantic_type
from information_schema.columns
where table_catalog = 'greptime'
  and table_schema != 'public'
  and table_schema != 'information_schema'
  and table_schema != 'pg_catalog'
order by table_schema, table_name, column_name;

-- test query filter for columns --
select table_catalog, table_schema, table_name, column_name, data_type, semantic_type
from information_schema.columns
where table_catalog = 'greptime'
  and (table_schema in ('public')
      or
      table_schema == 'my_db')
order by table_schema, table_name, column_name;

use public;

drop schema my_db;

use information_schema;

-- test query filter for key_column_usage --
select * from KEY_COLUMN_USAGE where CONSTRAINT_NAME = 'TIME INDEX';

select * from KEY_COLUMN_USAGE where CONSTRAINT_NAME != 'TIME INDEX';

select * from KEY_COLUMN_USAGE where CONSTRAINT_NAME LIKE '%INDEX';

select * from KEY_COLUMN_USAGE where CONSTRAINT_NAME NOT LIKE '%INDEX';

select * from KEY_COLUMN_USAGE where CONSTRAINT_NAME == 'TIME INDEX' AND CONSTRAINT_SCHEMA != 'my_db';

-- schemata --

desc table schemata;

select * from schemata where catalog_name = 'greptime' and schema_name != 'public' order by catalog_name, schema_name;

-- test engines
select * from engines;

desc table build_info;

select count(*) from build_info;

desc table key_column_usage;

select * from key_column_usage;

-- tables not implemented
DESC TABLE COLUMN_PRIVILEGES;

SELECT * FROM COLUMN_PRIVILEGES;

DESC TABLE COLUMN_STATISTICS;

SELECT * FROM COLUMN_STATISTICS;

SELECT * FROM CHARACTER_SETS;

SELECT * FROM COLLATIONS;

SELECT * FROM COLLATION_CHARACTER_SET_APPLICABILITY;

DESC TABLE CHECK_CONSTRAINTS;

SELECT * FROM CHECK_CONSTRAINTS;

DESC TABLE REGION_PEERS;

USE INFORMATION_SCHEMA;

DESC COLUMNS;

drop table my_db.foo;

use public;
