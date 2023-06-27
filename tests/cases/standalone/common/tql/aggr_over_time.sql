-- Port from functions.test L607 - L630, commit 001ee2620e094970e5657ce39275b2fccdbd1359
-- Include stddev/stdvar over time

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

-- load 10s
--   metric 1.5990505637277868 1.5990505637277868 1.5990505637277868
create table metric (ts timestamp(3) time index, val double);

insert into metric values
    (0,0),
    (10000,1.5990505637277868),
    (20000,1.5990505637277868),
    (30000,1.5990505637277868);

-- eval instant at 1m stdvar_over_time(metric[1m])
--   {} 0
tql eval (60, 60, '1s') stdvar_over_time(metric[1m]);

-- eval instant at 1m stddev_over_time(metric[1m])
--   {} 0
tql eval (60, 60, '1s') stddev_over_time(metric[1m]);

drop table metric;


-- Port from functions.test L632 - L680, commit 001ee2620e094970e5657ce39275b2fccdbd1359
-- Include quantile over time

-- load 10s
-- 	data{test="two samples"} 0 1
-- 	data{test="three samples"} 0 1 2
-- 	data{test="uneven samples"} 0 1 4
create table data (ts timestamp(3) time index, val double, test string primary key);

insert into data values
    (0, 0, "two samples"),
    (10000, 1, "two samples"),
    (0, 0, "three samples"),
    (10000, 1, "three samples"),
    (20000, 2, "three samples"),
    (0, 0, "uneven samples"),
    (10000, 1, "uneven samples"),
    (20000, 4, "uneven samples");

-- eval instant at 1m quantile_over_time(0, data[1m])
-- 	{test="two samples"} 0
-- 	{test="three samples"} 0
-- 	{test="uneven samples"} 0
-- tql eval (60, 60, '1s') quantile_over_time(0, data[1m]);

-- eval instant at 1m quantile_over_time(0.5, data[1m])
-- 	{test="two samples"} 0.5
-- 	{test="three samples"} 1
-- 	{test="uneven samples"} 1
-- tql eval (60, 60, '1s') quantile_over_time(0.5, data[1m]);

-- eval instant at 1m quantile_over_time(0.75, data[1m])
-- 	{test="two samples"} 0.75
-- 	{test="three samples"} 1.5
-- 	{test="uneven samples"} 2.5
-- tql eval (60, 60, '1s') quantile_over_time(0.75, data[1m]);

-- eval instant at 1m quantile_over_time(0.8, data[1m])
-- 	{test="two samples"} 0.8
-- 	{test="three samples"} 1.6
-- 	{test="uneven samples"} 2.8
-- tql eval (60, 60, '1s') quantile_over_time(0.8, data[1m]);

-- eval instant at 1m quantile_over_time(1, data[1m])
-- 	{test="two samples"} 1
-- 	{test="three samples"} 2
-- 	{test="uneven samples"} 4
-- tql eval (60, 60, '1s') quantile_over_time(1, data[1m]);

-- eval instant at 1m quantile_over_time(-1, data[1m])
-- 	{test="two samples"} -Inf
-- 	{test="three samples"} -Inf
-- 	{test="uneven samples"} -Inf
-- tql eval (60, 60, '1s') quantile_over_time(-1, data[1m]);

-- eval instant at 1m quantile_over_time(2, data[1m])
-- 	{test="two samples"} +Inf
-- 	{test="three samples"} +Inf
-- 	{test="uneven samples"} +Inf
-- tql eval (60, 60, '1s') quantile_over_time(2, data[1m]);

-- eval instant at 1m (quantile_over_time(2, (data[1m])))
-- 	{test="two samples"} +Inf
-- 	{test="three samples"} +Inf
-- 	{test="uneven samples"} +Inf
-- tql eval (60, 60, '1s') (quantile_over_time(2, (data[1m])));

drop table data;

-- Some cases are not ported because insert NaN/Inf is not supported yet
