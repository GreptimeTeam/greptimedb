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

drop table json2_disable_non_object_insert;

create table json2_whole_and_path_read (
    ts timestamp time index,
    j json2
)
with (
    'append_mode' = 'true'
);

insert into json2_whole_and_path_read values
    (1, '{"a": {"b": 1}}'),
    (2, '{"a": {"b": 2}}');

-- JSON2 field projection remains supported (case 5): use in an intermediate plan node.
select json_get(j, 'a.b'), count(*)
from json2_whole_and_path_read
group by json_get(j, 'a.b')
order by json_get(j, 'a.b');

drop table json2_whole_and_path_read;

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
