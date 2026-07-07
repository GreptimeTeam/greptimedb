create table json2_root_align (
    ts timestamp time index,
    col_x int64,
    j json2
) with (
    'sst_format' = 'flat',
);

-- SST data
insert into json2_root_align values (1, 100, '{"a": 1}');
insert into json2_root_align values (2, 200, '{"a": 2}');

admin flush_table('json2_root_align');

-- memtable data (unflushed)
insert into json2_root_align values (3, 300, '{"b": 3}');

-- Root read: only the JSON2 column in projection
select json_get(j, '') from json2_root_align order by ts;

drop table json2_root_align;
