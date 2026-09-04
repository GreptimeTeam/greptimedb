create table json2_join_same_name_left (
    ts timestamp time index,
    k string,
    j json2
)
with (
    'append_mode' = 'true'
);

create table json2_join_same_name_right (
    ts timestamp time index,
    k string,
    j json2
)
with (
    'append_mode' = 'true'
);

insert into json2_join_same_name_left values
    (1, 'a', '{"a": 1, "left_only": "kept"}');

insert into json2_join_same_name_right values
    (1, 'a', '{"a": "right", "right_only": "should be kept"}');

admin flush_table('json2_join_same_name_left');

admin flush_table('json2_join_same_name_right');

-- Conflicting hints for same-named JSON2 columns preserve both values as Variant.
select json_get(r.j, 'a')::string, json_get(l.j, 'a')::int64
from json2_join_same_name_left l
join json2_join_same_name_right r
on l.k = r.k;

select
    l.j as left_json,
    r.j as right_json
from json2_join_same_name_left l
join json2_join_same_name_right r
on l.k = r.k;

drop table json2_join_same_name_left;

drop table json2_join_same_name_right;
