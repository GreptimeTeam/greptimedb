-- SQLNESS REPLACE (\d+:\d+:\d+\.\d+) TIME
-- SQLNESS REPLACE [\s\-]+
select current_time();

SELECT to_timezone('2022-09-20T14:16:43.012345+08:00', 'Europe/Berlin');
