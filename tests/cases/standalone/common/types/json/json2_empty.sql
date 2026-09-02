-- Empty-object JSON2 values are stored in the JSONB remainder column and must
-- survive memtable flush and compaction unchanged, both as whole values and
-- through path access.

-- Whole-value empty objects: insert, flush, then mix with expanded documents
-- before compacting.
create table json_empty (
    ts timestamp time index,
    j json2
) with (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

insert into json_empty values (1, '{}');

select ts, j from json_empty order by ts;

select ts, j.a from json_empty order by ts;

admin flush_table('json_empty');

select ts, j from json_empty order by ts;

insert into json_empty values (2, '{}'), (3, '{"a": 1}');

select ts, j, j.a from json_empty order by ts;

admin flush_table('json_empty');

admin compact_table('json_empty');

select ts, j, j.a from json_empty order by ts;

select count(*) from json_empty;

drop table json_empty;

-- Nested empty objects and arrays, which also produce only remainder content,
-- must round trip through flush and compaction as whole values.
create table json_empty_nested (
    ts timestamp time index,
    j json2
) with (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

insert into json_empty_nested values
    (1, '{"a": {}}'),
    (2, '{"a": {"b": {}}}');

select ts, j from json_empty_nested order by ts;

admin flush_table('json_empty_nested');

select ts, j from json_empty_nested order by ts;

insert into json_empty_nested values (3, '{"x": []}');

admin flush_table('json_empty_nested');

admin compact_table('json_empty_nested');

select ts, j from json_empty_nested order by ts;

drop table json_empty_nested;

-- Empty objects, SQL NULL values, and JSON null values must remain distinct
-- through memtable flush and compaction.
create table json_empty_null_mixed (
    ts timestamp time index,
    j json2
) with (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

insert into json_empty_null_mixed values
    (1, '{}'),
    (2, NULL),
    (3, '{"a": {}}'),
    (4, '{"a": null}');

select ts, j, j is null from json_empty_null_mixed order by ts;

admin flush_table('json_empty_null_mixed');

select ts, j, j is null from json_empty_null_mixed order by ts;

insert into json_empty_null_mixed values
    (5, '{"a": 1}'),
    (6, NULL);

select ts, j, j.a, j is null from json_empty_null_mixed order by ts;

admin flush_table('json_empty_null_mixed');

admin compact_table('json_empty_null_mixed');

select ts, j, j.a, j is null from json_empty_null_mixed order by ts;

select count(*) from json_empty_null_mixed;

drop table json_empty_null_mixed;
