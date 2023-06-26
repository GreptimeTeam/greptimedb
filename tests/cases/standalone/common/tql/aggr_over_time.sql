-- Port from functions.test L607 - L630, commit 001ee2620e094970e5657ce39275b2fccdbd1359
-- Include stddev/stdvar over time

-- clear
-- load 10s
--   metric 0 8 8 2 3
create table metric (ts timestamp(3) time index, val double);

insert into metric values
    (0,0),
    (10000,8),
    (20000,8),
    (30000,2),
    (40000,3);

select * from metric;

-- eval instant at 1m stdvar_over_time(metric[1m])
--   {} 10.56
tql eval (60, 61, '10s') stdvar_over_time(metric[1m]);

-- eval instant at 1m stddev_over_time(metric[1m])
--   {} 3.249615
tql eval (60, 60, '1s') stddev_over_time(metric[1m]);

-- eval instant at 1m stddev_over_time((metric[1m]))
--   {} 3.249615
tql eval (60, 60, '1s') stddev_over_time((metric[1m]));

drop table metric;