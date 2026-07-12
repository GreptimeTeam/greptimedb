select 1;

select 2 + 3;

select 4 + 0.5;

select "a";

select "A";

select * where "a" = "A";

select TO_UNIXTIME('2023-03-01T06:35:02Z');

select TO_UNIXTIME('    2023-03-01T06:35:02Z    ');

select TO_UNIXTIME(2);

select TO_UNIXTIME('2023-03-01');

select TO_UNIXTIME('2023-03-01'::date);

select TO_UNIXTIME('2023-03-01 08:00:00+0000');

create table test_unixtime(a int, b timestamp_sec time index);

DESC TABLE test_unixtime;

insert into test_unixtime values(27, 27);

select * from test_unixtime;

select a from test_unixtime;

select b from test_unixtime;

select TO_UNIXTIME(b) from test_unixtime;

-- TEST tailing commas support
select a, b, from test_unixtime;

DROP TABLE test_unixtime;
