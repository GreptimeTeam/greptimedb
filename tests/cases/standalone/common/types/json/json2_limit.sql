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
