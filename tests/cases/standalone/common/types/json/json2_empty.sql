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

-- Lists containing empty objects cannot be materialized as Parquet list fields because that
-- would produce an empty Struct child. They must stay in the remainder and preserve the empty
-- objects through flush and compaction.
create table json_empty_in_lists (
    ts timestamp time index,
    j json2
) with (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

insert into json_empty_in_lists values
    (1, '{"items":[{}],"mixed":[{"id":1},{}],"nested":[{"meta":{"value":1}},{"meta":{}}]}');

admin flush_table('json_empty_in_lists');

select ts, j, j.items[0] as item0, j.items[1] as item1,
       j.mixed[1] as mixed, j.nested[1].meta as meta
from json_empty_in_lists
order by ts;

insert into json_empty_in_lists values
    (2, '{"items":[{}],"mixed":[{"id":2},{}],"nested":[{"meta":{"value":2}},{"meta":{}}],"name":"second"}');

admin flush_table('json_empty_in_lists');

-- Empty objects mixed with non-empty objects in the same list must also stay in
-- the remainder. Otherwise the first item may be reconstructed as {"id":null}.
insert into json_empty_in_lists values
    (3, '{"items":[{},{"id":1}]}');

admin flush_table('json_empty_in_lists');

admin compact_table('json_empty_in_lists');

select ts, j, j.items[0] as item0, j.items[1] as item1,
       j.mixed[1] as mixed, j.nested[1].meta as meta
from json_empty_in_lists
order by ts;

drop table json_empty_in_lists;

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

-- Explicit SQL NULL and omitted-column inserts must be accepted and round trip
-- as NULL (not panic), remaining distinct from empty objects.
create table json_empty_null_forms (
    ts timestamp time index,
    j json2
) with (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

insert into json_empty_null_forms (ts, j) values (1, NULL);

insert into json_empty_null_forms (ts) values (2);

insert into json_empty_null_forms (ts, j) values (3, '{}');

select ts, j, j is null from json_empty_null_forms order by ts;

admin flush_table('json_empty_null_forms');

admin compact_table('json_empty_null_forms');

select ts, j, j is null from json_empty_null_forms order by ts;

drop table json_empty_null_forms;
