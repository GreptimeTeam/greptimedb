create table json2_table (
    ts timestamp time index,
    j  json2
) with (
    'append_mode' = 'true',
    'sst_format' = 'flat',
);

insert into json2_table (ts, j)
values (1, '{"a": {"b": 1}, "c": "s1"}'),
       (2, '{"a": {"b": -2}, "c": "s2"}');

admin flush_table('json2_table');

insert into json2_table (ts, j)
values (3, '{"a": {"b": 3}, "c": "s3"}');

insert into json2_table
values (4, '{"a": {"b": -4}}'),
       (5, '{"a": {}, "c": "s5"}'),
       (6, '{"c": "s6"}');

admin flush_table('json2_table');

insert into json2_table
values (7, '{"a": {"b": "s7"}, "c": [1]}'),
       (8, '{"a": {"b": 8}, "c": "s8"}');

insert into json2_table
values (9, '{"a": {"x": true}, "c": "s9"}'),
       (10, '{"a": {"b": 10}, "y": false}');

select j.a.b from json2_table order by ts;

select j.a, j.a.x from json2_table order by ts;

select j.c, j.y from json2_table order by ts;

select j from json2_table order by ts;

select * from json2_table order by ts;

select j.a.b + 1 from json2_table order by ts;

select abs(j.a.b) from json2_table order by ts;

-- "j.c" is of type "String", "abs" is expected to be all "null"s.
select abs(j.c) from json2_table order by ts;

drop table json2_table;
