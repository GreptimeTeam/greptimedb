create table "HelloWorld" (a string, b timestamp time index);

insert into "HelloWorld" values ("a", 1) ,("b", 2);

select count(*) from "HelloWorld";

create table test (a string, "BbB" timestamp time index);

insert into test values ("c", 1) ;

select count(*) from test;

select count(*) from (select count(*) from test where a = 'a');

select count(*) from (select * from test cross join "HelloWorld");

drop table "HelloWorld";

drop table test;

-- Append table

create table count_where_bug (
    `tag` String,
    ts TimestampMillisecond time index,
    num Int64,
    primary key (`tag`),
) engine=mito with('append_mode'='true');

insert into count_where_bug (`tag`, ts, num)
values  ('a', '2024-09-06T06:00:01Z', 1),
        ('a', '2024-09-06T06:00:02Z', 2),
        ('a', '2024-09-06T06:00:03Z', 3),
        ('b', '2024-09-06T06:00:04Z', 4),
        ('b', '2024-09-06T06:00:05Z', 5);

select count(1) from count_where_bug where `tag` = 'b';

select count(1) from count_where_bug where ts > '2024-09-06T06:00:04Z';

select count(1) from count_where_bug where num != 3;

drop table count_where_bug;
