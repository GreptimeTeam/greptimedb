create schema test;

create table test.tbl(a int, b timestamp time index);

insert into test.tbl values (1,1), (2,2), (3,3);

select test.tbl.a from test.tbl;
