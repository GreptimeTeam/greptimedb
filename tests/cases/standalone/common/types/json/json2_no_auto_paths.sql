-- With no type hints and automatic path expansion disabled, every JSON value
-- is stored in the remainder column.
create table json2_no_auto_paths (
    ts timestamp time index,
    j json2(
        max_auto_expanded_paths = 0
    )
) with (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

show create table json2_no_auto_paths;

insert into json2_no_auto_paths values
    (1, '{"profile":{"name":"alice","contact":{"address":{"city":"Paris"}}},"groups":[{"members":[{"name":"a0"}]},{"members":[{"name":"a1"},{"name":"a2"}]}],"matrix":[[1,2],[3,4]]}'),
    (2, '{"profile":{"name":"bob","contact":{"address":{"city":"Berlin"}}},"groups":[{"members":[]},{"members":[{"name":"b1"}]}],"matrix":[[10],[20,21]]}');

admin flush_table('json2_no_auto_paths');

insert into json2_no_auto_paths values
    (3, '{}'),
    (4, '{"profile":{"name":"carol","contact":{"address":{}}},"groups":[null,{"members":[null,{"name":"c1"}]}],"matrix":[[],[30]]}');

admin flush_table('json2_no_auto_paths');

admin compact_table('json2_no_auto_paths', 'swcs', '86400');

select ts, j from json2_no_auto_paths order by ts;

select j as empty_object from json2_no_auto_paths where ts = 3;

select ts, upper(j.profile.name) as upper_name
from json2_no_auto_paths
order by ts;

select ts, j.profile.contact.address.city as city
from json2_no_auto_paths
order by ts;

select
    ts,
    j.matrix[0][1] as matrix_value,
    j.groups[1].members[0].name as member_name
from json2_no_auto_paths
order by ts;

drop table json2_no_auto_paths;
