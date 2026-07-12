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

-- eval instant at 1m trigy atan2 trigx
--     trigy{} 0.4636476090008061
tql eval (60, 60, '1s') trigy atan2 trigx;

-- eval instant at 1m trigy atan2 trigNaN
--     trigy{} NaN
-- This query doesn't have result because `trignan` is NaN and will be filtered out.
tql eval (60, 60, '1s') trigy atan2 trignan;

-- eval instant at 1m 10 atan2 20
--     0.4636476090008061
tql eval (60, 60, '1s') 10 atan2 20;

-- eval instant at 1m 10 atan2 NaN
--     NaN
tql eval (60, 60, '1s') 10 atan2 NaN;

drop table trigx;

drop table trigy;

drop table trignan;


-- About irate. Related to issue https://github.com/GreptimeTeam/greptimedb/issues/5880
CREATE TABLE t(
   greptime_timestamp TIMESTAMP(9) TIME INDEX,
   greptime_value DOUBLE
);

INSERT INTO t(greptime_timestamp, greptime_value)
VALUES
   ('2025-04-01T00:00:00.5Z', 1),
   ('2025-04-01T00:00:01Z', 2),
   ('2025-04-01T00:00:01.5Z', 3),
   ('2025-04-01T00:00:02Z', 4),
   ('2025-04-01T00:00:02.5Z', 5),
   ('2025-04-01T00:00:03Z', 6),
   ('2025-04-01T00:00:03.5Z', 7),
   ('2025-04-01T00:00:04Z', 8),
   ('2025-04-01T00:00:04.5Z', 9),
   ('2025-04-01T00:00:05Z', 10),
   ('2025-04-01T00:00:05.5Z', 11),
   ('2025-04-01T00:00:06Z', 12),
   ('2025-04-01T00:00:06.5Z', 13),
   ('2025-04-01T00:00:07Z', 14),
   ('2025-04-01T00:00:07.5Z', 15),
   ('2025-04-01T00:00:08Z', 16),
   ('2025-04-01T00:00:08.5Z', 17),
   ('2025-04-01T00:00:09Z', 18),
   ('2025-04-01T00:00:09.5Z', 19),
   ('2025-04-01T00:00:10Z', 20);

tql eval (1743465600.5, 1743465610, '1s') irate(t[2s]);

drop table t;

