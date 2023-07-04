-- Port from operators.test L607 - L630, commit 001ee2620e094970e5657ce39275b2fccdbd1359
-- Include atan2

-- load 5m
--     trigy{} 10
--     trigx{} 20
--     trigNaN{} NaN
create table trigy (ts timestamp(3) time index, val double);

insert into trigy values (0,10);

create table trigx (ts timestamp(3) time index, val double);

insert into trigx values (0,20);

create table trignan (ts timestamp(3) time index, val double);

insert into trignan values (0, 'NaN'::double);

-- eval instant at 5m trigy atan2 trigx
--     trigy{} 0.4636476090008061
tql eval (300, 300, '1s') trigy atan2 trigx;

-- eval instant at 5m trigy atan2 trigNaN
--     trigy{} NaN
-- This query doesn't have result because `trignan` is NaN and will be filtered out.
tql eval (300, 300, '1s') trigy atan2 trignan;

-- eval instant at 5m 10 atan2 20
--     0.4636476090008061
tql eval (300, 300, '1s') 10 atan2 20;

-- eval instant at 5m 10 atan2 NaN
--     NaN
tql eval (300, 300, '1s') 10 atan2 NaN;

drop table trigx;

drop table trigy;

drop table trignan;
