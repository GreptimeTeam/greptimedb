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

-- JSON2 requires flat SST output even when append mode is enabled.
create table json2_primary_key_format (
    ts timestamp time index,
    j json2
) with (
    'append_mode' = 'true',
    'sst_format' = 'primary_key'
);

create table json2_alter_primary_key_format (
    ts timestamp time index,
    j json2
) with (
    'append_mode' = 'true',
    'memtable.type' = 'bulk'
);

insert into json2_alter_primary_key_format values (1, '{"a": 1}');

alter table json2_alter_primary_key_format set 'sst_format' = 'primary_key';

show create table json2_alter_primary_key_format;

insert into json2_alter_primary_key_format values (2, '{"a": 2}');

admin flush_table('json2_alter_primary_key_format');

select ts, json_get(j, 'a') from json2_alter_primary_key_format order by ts;

drop table json2_alter_primary_key_format;

create table json2_add_to_primary_key_format (
    ts timestamp time index
) with (
    'append_mode' = 'true',
    'sst_format' = 'primary_key'
);

alter table json2_add_to_primary_key_format add column j json2;

show create table json2_add_to_primary_key_format;

drop table json2_add_to_primary_key_format;
