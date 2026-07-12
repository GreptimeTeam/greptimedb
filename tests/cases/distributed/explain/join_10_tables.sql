create table t_1 (ts timestamp time index, vin string, val int);
create table t_2 (ts timestamp time index, vin string, val int);
create table t_3 (ts timestamp time index, vin string, val int);
create table t_4 (ts timestamp time index, vin string, val int);
create table t_5 (ts timestamp time index, vin string, val int);
create table t_6 (ts timestamp time index, vin string, val int);
create table t_7 (ts timestamp time index, vin string, val int);
create table t_8 (ts timestamp time index, vin string, val int);
create table t_9 (ts timestamp time index, vin string, val int);
create table t_10 (ts timestamp time index, vin string, val int);

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
explain 
select * 
from 
  t_1 
  INNER JOIN t_2 ON t_2.ts = t_1.ts 
  AND t_2.vin = t_1.vin 
  INNER JOIN t_3 ON t_3.ts = t_2.ts 
  AND t_3.vin = t_2.vin 
  INNER JOIN t_4 ON t_4.ts = t_3.ts 
  AND t_4.vin = t_3.vin 
  INNER JOIN t_5 ON t_5.ts = t_4.ts 
  AND t_5.vin = t_4.vin 
  INNER JOIN t_6 ON t_6.ts = t_5.ts 
  AND t_6.vin = t_5.vin 
  INNER JOIN t_7 ON t_7.ts = t_6.ts 
  AND t_7.vin = t_6.vin 
  INNER JOIN t_8 ON t_8.ts = t_7.ts 
  AND t_8.vin = t_7.vin 
  INNER JOIN t_9 ON t_9.ts = t_8.ts 
  AND t_9.vin = t_8.vin 
  INNER JOIN t_10 ON t_10.ts = t_9.ts 
  AND t_10.vin = t_9.vin 
where 
  t_1.vin is not null 
order by t_1.ts desc 
limit 1;

drop table t_1;
drop table t_2;
drop table t_3;
drop table t_4;
drop table t_5;
drop table t_6;
drop table t_7;
drop table t_8;
drop table t_9;
drop table t_10;
