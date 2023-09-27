-- statement ok
create schema s1;

-- statement ok
create table s1.tbl(i int, b timestamp time index);

-- statement ok
select s1.tbl.i from s1.tbl;

-- statement error
select s2.tbl.i from s1.tbl;

-- statement error
select a.tbl.i from range(10) tbl(i)