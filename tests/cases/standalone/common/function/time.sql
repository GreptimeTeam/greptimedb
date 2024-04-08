-- SQLNESS REPLACE (\d+:\d+:\d+\.\d+) TIME
-- SQLNESS REPLACE [\s\-]+
select current_time();

select GREATEST('1999-01-30', '2023-03-01');
select GREATEST('2000-02-11'::Date, '2020-12-30'::Date);

select to_timezone('2022-09-20T14:16:43.012345+08:00', 'Europe/Berlin');
select to_timezone('2022-09-20T14:16:43.012345+08:00'::Timestamp, 'Europe/Berlin');
select to_timezone('2024-03-29T14:16:43.012345Z', 'Asia/Shanghai');
select to_timezone('2024-03-29T14:16:43.012345Z'::Timestamp, 'Asia/Shanghai');

select to_timezone(1709992225, 'Asia/Shanghai');

select to_timezone(1711508510000::INT64, 'Asia/Shanghai');
