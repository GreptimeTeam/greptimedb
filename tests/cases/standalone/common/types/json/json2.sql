create table json2_table (
    ts timestamp time index,
    j  json2
) with (
    'append_mode' = 'true',
    'sst_format' = 'flat',
);

insert into json2_table (ts, j) values (101, '[1, 2, 3]');

insert into json2_table (ts, j) values (102, '"hello"');

insert into json2_table (ts, j) values (103, '42');

insert into json2_table (ts, j) values (104, 'true');

insert into json2_table (ts, j) values (105, 'null');

insert into json2_table (ts, j)
values (1, '{"a": {"b": 1}, "c": "s1", "d": [{"e": {"f": 0.1}}]}'),
       (2, '{"a": {"b": -2}, "c": "s2", "d": [{"e": {"f": 0.2}}]}');

admin flush_table('json2_table');

insert into json2_table (ts, j)
values (3, '{"a": {"b": 3}, "c": "s3"}');

insert into json2_table
values (4, '{"a": {"b": -4}, "d": [{"e": {"g": -0.4}}]}'),
       (5, '{"a": {}, "c": "s5"}'),
       (6, '{"c": "s6"}');

admin flush_table('json2_table');

admin compact_table('json2_table', 'swcs', '86400');

insert into json2_table
values (7, '{"a": {"b": "s7"}, "c": [1], "d": [{"e": {"g": -0.7}}]}'),
       (8, '{"a": {"b": 8}, "c": "s8"}');

admin flush_table('json2_table');

insert into json2_table
values (9, '{"a": {"x": true}, "c": "s9", "d": [{"e": {"g": -0.9}}]}'),
       (10, '{"a": {"b": 10}, "y": false}');

-- SQLNESS REPLACE (peers.*) REDACTED
explain select j.a.b from json2_table;

-- SQLNESS REPLACE (peers.*) REDACTED
explain select j.a.x::bool from json2_table;

select j.a.b from json2_table order by ts;

select j.a, j.a.x from json2_table order by ts;

select j.c, j.y from json2_table order by ts;

select j from json2_table order by ts;

select * from json2_table order by ts;

select j.a.b + 1 from json2_table order by ts;

select abs(j.a.b) from json2_table order by ts;

-- "j.c" is of type "String", "abs" is expected to be all "null"s.
select abs(j.c) from json2_table order by ts;

select j.d from json2_table order by ts;

drop table json2_table;

create table json2_root_schema_merge (
    ts timestamp time index,
    j  json2
) with (
    'sst_format' = 'flat',
);

insert into json2_root_schema_merge
values (1, '{"a": {"b": 1}, "c": "sst1"}'),
       (2, '{"a": {"b": 2}, "d": true}');

admin flush_table('json2_root_schema_merge');

insert into json2_root_schema_merge
values (1, '{"a": {"b": 10}, "e": {"x": 1}}'),
       (3, '{"a": {"x": false}, "c": "sst2"}');

admin flush_table('json2_root_schema_merge');

insert into json2_root_schema_merge
values (2, '{"a": {"b": 20}, "f": [1, 2]}'),
       (4, '{"z": "mem"}');

select json_get(j, '') from json2_root_schema_merge order by ts;

select json_get(j, ''), json_get(j, 'a.b') from json2_root_schema_merge order by ts;

drop table json2_root_schema_merge;

create table json2_default_null_ok (
    ts timestamp time index,
    j json2(
        a int64 null default null
    )
);

drop table json2_default_null_ok;

create table json2_default_null_check (
    ts timestamp time index,
    j json2(
        a int64 not null default null
    )
);
