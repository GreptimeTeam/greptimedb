select 1;

select 2 + 3;

select 4 + 0.5;

select "a";

select "A";

select * where "a" = "A";

select TO_UNIXTIME('2023-03-01T06:35:02Z');

select TO_UNIXTIME('    2023-03-01T06:35:02Z    ');

select TO_UNIXTIME(2);

create table test_unixtime(a int, b timestamp time index);

DESC TABLE test_unixtime;

insert into test_unixtime values(27, 27);

select * from test_unixtime;

select a from test_unixtime;

select b from test_unixtime;

select TO_UNIXTIME(b) from test_unixtime;

DROP TABLE test_unixtime;

select INTERVAL '1 year 2 months 3 days 4 hours 5 minutes 6 seconds 100 microseconds';

select INTERVAL '1 year 2 months 3 days 4 hours' + INTERVAL '1 year';

select INTERVAL '1 year 2 months 3 days 4 hours' - INTERVAL '1 year';
