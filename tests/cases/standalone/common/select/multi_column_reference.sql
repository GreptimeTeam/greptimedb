create schema test;

create table test.tbl(a int, b timestamp time index);

insert into test.tbl values (1,1), (2,2), (3,3);

select test.tbl.* from test.tbl;

select test.tbl.a from test.tbl;

select test.tbl.b from test.tbl;

select test.t.a from test.tbl t;

select test.tbl.a from test.tbl t;

-- check how ties are resolved
-- we create a table called "t" in a schema called "t" with a column called "t" that has a field called "t"
create schema t;

-- unsupported
-- create table t.t(t ROW(t int), b timestamp time index);

-- unsupported
drop schema t cascade;

-- test multiple tables with the same name but a different schema
-- we allow this (duplicate alias in query)
create schema s1;

create schema s2;

create table s1.t1(
    a int,
    b timestamp time index,
);

insert into s1.t1 values (1,1);

create table s2.t1(
    a int,
    b timestamp time index,
);

insert into s2.t1 values (2,1);

-- statement ok
select t1.a from s1.t1,s2.t1;

-- statement ok
select s1.t1.a from s1.t1,s2.t1;

-- statement error
select s2.t1.a from s1.t1,s2.t1;

-- statement ok
select s2.t1.a from s2.t1, s1.t1;

-- test various failures
-- statement error
select testX.tbl.a from test.tbl;

-- statement error
select test.tblX.a from test.tbl;

-- statement error
select test.tbl.aX from test.tbl;
