create schema abc;

use abc;

create table t (ts timestamp time index);

create schema abcde;

use abcde;

create table t (ts timestamp time index);

select table_catalog, table_schema, table_name from information_schema.tables where table_schema != 'information_schema';

use public;

drop schema abc;

drop schema abcde;

drop schema information_schema;

use information_schema;

create table t (ts timestamp time index);

drop table build_info;

alter table build_info add q string;

truncate table build_info;

insert into build_info values ("", "", "", "", "");

delete from build_info;

use public;
