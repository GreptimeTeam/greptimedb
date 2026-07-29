create table json2_disable_non_object_insert (
    ts timestamp time index,
    j json2
)
with (
    'append_mode' = 'true'
);

insert into json2_disable_non_object_insert values (1, '[1, 2, 3]');

insert into json2_disable_non_object_insert values (2, '"hello"');

insert into json2_disable_non_object_insert values (3, '42');

insert into json2_disable_non_object_insert values (4, 'true');

insert into json2_disable_non_object_insert values (5, 'null');

insert into json2_disable_non_object_insert values (6, '{}');

drop table json2_disable_non_object_insert;

create table json2_disable_whole_column_read (
    ts timestamp time index,
    j json2
)
with (
    'append_mode' = 'true'
);

insert into json2_disable_whole_column_read values
    (1, '{"a": {"b": 1}}'),
    (2, '{"a": {"b": 2}}');

-- Whole JSON2 uses are unsupported (case 1): direct projection.
select j from json2_disable_whole_column_read order by ts;

-- Whole JSON2 uses are unsupported (case 2): wildcard projection.
select * from json2_disable_whole_column_read order by ts;

-- Whole JSON2 uses are unsupported (case 3): json_get with an empty path.
select json_get(j, '') from json2_disable_whole_column_read;

select json_get(j, '$') from json2_disable_whole_column_read;

select json_get(j, '.') from json2_disable_whole_column_read;

select json_get(j, '$.') from json2_disable_whole_column_read;

-- Whole JSON2 uses are unsupported (case 4): use in an intermediate plan node.
select count(*)
from (
    select j
    from json2_disable_whole_column_read
    group by j
);

-- JSON2 field projection remains supported (case 5): use in an intermediate plan node.
select json_get(j, 'a.b'), count(*)
from json2_disable_whole_column_read
group by json_get(j, 'a.b')
order by json_get(j, 'a.b');

-- Whole JSON2 uses are unsupported (case 6): output after an intermediate projection.
select ts, j
from (
    select ts, j
    from json2_disable_whole_column_read
)
order by ts;

-- Whole JSON2 uses are unsupported (case 7): DISTINCT in an intermediate plan node.
select count(*)
from (
    select distinct j
    from json2_disable_whole_column_read
);

drop table json2_disable_whole_column_read;

create table json2_without_append_mode (
    ts timestamp time index,
    j json2
);

create table json2_append_mode_false (
    ts timestamp time index,
    j json2
) with (
    'append_mode' = 'false'
);

create table json2_alter_non_append (
    ts timestamp time index
);

alter table json2_alter_non_append add column j json2;

drop table json2_alter_non_append;

create table json2_set_append_mode_false (
    ts timestamp time index,
    j json2
) with (
    'append_mode' = 'true'
);

alter table json2_set_append_mode_false set 'append_mode' = 'false';

drop table json2_set_append_mode_false;
