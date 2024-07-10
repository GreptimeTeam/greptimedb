--- test information_schema.views ----
create schema test_information_schema_views;

use test_information_schema_views;

USE test_information_schema_views;

create table t1 (ts timestamp time index, val1  int);

create table t2 (ts timestamp time index, val2 int primary key);

create view myview as select * from t1 where val1 > 5;

create view myview2 as select * from t2 inner join t1 on t1.val1 = t2.val2;

select table_catalog, table_schema, table_name, view_definition from information_schema.views order by table_name;

drop view myview;

select table_catalog, table_schema, table_name, view_definition from information_schema.views order by table_name;

drop view myview2;

drop table t1, t2;

USE public;

drop schema test_information_schema_views;
