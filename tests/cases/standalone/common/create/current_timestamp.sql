create table t1 (ts timestamp time index default CURRENT_TIMESTAMP);

show create table t1;

create table t2 (ts timestamp time index default currEnt_tImEsTamp());

show create table t2;

create table t3 (ts timestamp time index default now());

show create table t3;

create table t4 (ts timestamp time index default now);

drop table t1;
drop table t2;
drop table t3;
