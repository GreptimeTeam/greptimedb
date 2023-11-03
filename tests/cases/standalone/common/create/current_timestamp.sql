create table t1 (ts timestamp time index default CURRENT_TIMESTAMP);

create table t2 (ts timestamp time index default currEnt_tImEsTamp());

create table t3 (ts timestamp time index default now());

create table t4 (ts timestamp time index default now);

drop table t1;
drop table t2;
drop table t3;
