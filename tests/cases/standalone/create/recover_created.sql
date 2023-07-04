create table t1 (a timestamp time index);

create table t2 (b timestamp time index);

drop table t1;

drop table t2;

-- SQLNESS ARG restart=true
show tables;

create table t3 (c timestamp time index);

desc table t3;

drop table t3;
