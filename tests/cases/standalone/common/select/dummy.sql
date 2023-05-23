select 1;

select 2 + 3;

select 4 + 0.5;

select "a";

select "A";

select * where "a" = "A";

select TO_UNIXTIME('2023-03-01T06:35:02Z');

select TO_UNIXTIME(2);

create table test_a(a int, b timestamp time index);

DESC TABLE test_a;

insert into test_a values(27, 27);

select * from test_a;

select a from test_a;

select b from test_a;

select TO_UNIXTIME(b) from test_a;

DROP TABLE test_a;