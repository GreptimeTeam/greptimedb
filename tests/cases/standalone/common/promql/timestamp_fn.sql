-- Test `timestamp()` function
-- timestamp() returns the timestamp of each sample as seconds since Unix epoch

create table timestamp_test (ts timestamp time index, val double);

insert into timestamp_test values
  (0, 1.0),
  (1000, 2.0),
  (60000, 3.0),
  (3600000, 4.0),
   -- 2021-01-01 00:00:00
  (1609459200000, 5.0),
   -- 2021-01-01 00:01:00
  (1609459260000, 6.0);

-- Test timestamp() with time series
tql eval (0, 3600, '30s') timestamp(timestamp_test);

-- Test timestamp() with specific time range
tql eval (0, 60, '30s') timestamp(timestamp_test);

tql eval (0, 60, '30s') -timestamp(timestamp_test);

-- Test timestamp() with 2021 data
tql eval (1609459200, 1609459260, '30s') timestamp(timestamp_test);

-- Test timestamp() with arithmetic operations
tql eval (0, 60, '30s') timestamp(timestamp_test) + 1;

-- Test timestamp() with boolean operations
tql eval (0, 60, '30s') timestamp(timestamp_test) > bool 30;

-- Test timestamp() with time functions
tql eval (0, 60, '30s') timestamp(timestamp_test) - time();

-- Test timestamp() with other functions
tql eval (0, 60, '30s') abs(timestamp(timestamp_test) - avg(timestamp(timestamp_test))) > 20;

-- Test Issue 6707
tql eval timestamp(demo_memory_usage_bytes * 1);

tql eval timestamp(-demo_memory_usage_bytes);

tql eval (0, 60, '30s') timestamp(timestamp_test) == 60;

-- Test timestamp() with multiple metrics
create table timestamp_test2 (ts timestamp time index, val double);

insert into timestamp_test2 values
  (0, 10.0),
  (1000, 20.0),
  (60000, 30.0);

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 60, '30s') timestamp(timestamp_test) + timestamp(timestamp_test2);

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 60, '30s') timestamp(timestamp_test) == timestamp(timestamp_test2);

drop table timestamp_test;

drop table timestamp_test2;
