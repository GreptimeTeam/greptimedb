-- scripts table has different table ids in different modes
select *
from information_schema.tables
where table_name != 'scripts'
order by table_schema, table_name;

select * from information_schema.columns order by table_schema, table_name;

create
database my_db;

use my_db;

create table foo
(
    ts TIMESTAMP TIME INDEX
);

select table_name
from information_schema.tables
where table_schema = 'my_db'
order by table_name;

select table_catalog, table_schema, table_name, table_type, engine
from information_schema.tables
where table_catalog = 'greptime'
  and table_schema != 'public'
  and table_schema != 'information_schema'
order by table_schema, table_name;

select table_catalog, table_schema, table_name, column_name, data_type, semantic_type
from information_schema.columns
where table_catalog = 'greptime'
  and table_schema != 'public'
  and table_schema != 'information_schema'
order by table_schema, table_name;

use public;

drop schema my_db;
