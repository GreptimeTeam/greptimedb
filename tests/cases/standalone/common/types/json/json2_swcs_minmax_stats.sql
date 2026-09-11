-- Regression test: min-max pruning must look up row group statistics by
-- parquet leaf column index. On flat-format tables with a JSON2 column, the
-- JSON2 struct expands to multiple leaf columns, so columns after it were
-- previously misaligned with leaves of the JSON2 struct when reading row
-- group statistics. Here `event_time` is the 8th logical column (2 tags + 5
-- fields), so its statistics were read from the 8th parquet leaf, which is
-- the small-integer `ns_edge` path of the payload. SWCS compaction reads
-- inputs with a time window predicate, so min-max pruning compared the window
-- against the tiny `ns_edge` statistics, dropped every row group and silently
-- lost all rows. The same misaligned statistics also affected plain queries
-- with time-range predicates.

create table json2_swcs_minmax_stats (
    workspace_id string not null,
    session_id string not null,
    seq bigint not null,
    event_time timestamp(9) not null time index,
    entry_kind string not null,
    payload json2 not null,
    schema_version int not null,
    is_error boolean not null,
    primary key (workspace_id, session_id)
) with (
    'append_mode' = 'true',
    'sst_format' = 'flat',
    'ttl' = 'forever'
);

-- 9 rows in the 2026-09-10 window and 2 rows in the 2026-09-11 window.
insert into json2_swcs_minmax_stats values
    ('ws', 's1', 1, 1788998400000000001, 'synthetic', '{"case":"swcs-repro","nested":[1,true],"ns_edge":0,"ordinal":1,"synthetic":true}', 1, false),
    ('ws', 's2', 2, 1788998400000000002, 'synthetic', '{"case":"swcs-repro","nested":[2,false],"ns_edge":1,"ordinal":2,"synthetic":true}', 1, false),
    ('ws', 's3', 3, 1788998400000000003, 'synthetic', '{"case":"swcs-repro","nested":[3,null],"ns_edge":2,"ordinal":3,"synthetic":true}', 1, false),
    ('ws', 's1', 4, 1788998400000000004, 'synthetic', '{"case":"swcs-repro","nested":[4,true],"ns_edge":3,"ordinal":4,"synthetic":true}', 1, false),
    ('ws', 's2', 5, 1788998400000000005, 'synthetic', '{"case":"swcs-repro","nested":[5,false],"ns_edge":4,"ordinal":5,"synthetic":true}', 1, false),
    ('ws', 's3', 6, 1788998400000000006, 'synthetic', '{"case":"swcs-repro","nested":[6,null],"ns_edge":5,"ordinal":6,"synthetic":true}', 1, false),
    ('ws', 's1', 7, 1788998400000000007, 'synthetic', '{"case":"swcs-repro","nested":[7,true],"ns_edge":6,"ordinal":7,"synthetic":true}', 1, false),
    ('ws', 's2', 8, 1788998400000000008, 'synthetic', '{"case":"swcs-repro","nested":[8,false],"ns_edge":7,"ordinal":8,"synthetic":true}', 1, false),
    ('ws', 's3', 9, 1788998400000000009, 'synthetic', '{"case":"swcs-repro","nested":[9,null],"ns_edge":8,"ordinal":9,"synthetic":true}', 1, false),
    ('ws', 's1', 10, 1789084800000000001, 'synthetic', '{"case":"swcs-repro","nested":[10,true],"ns_edge":9,"ordinal":10,"synthetic":true}', 1, false),
    ('ws', 's2', 11, 1789084800000000002, 'synthetic', '{"case":"swcs-repro","nested":[11,false],"ns_edge":10,"ordinal":11,"synthetic":true}', 1, false);

select count(*) from json2_swcs_minmax_stats;

admin flush_table('json2_swcs_minmax_stats');

-- Check pruning on flush-written SSTs before any manual compaction.
select count(*) from json2_swcs_minmax_stats
where event_time >= 1788998400000000000 and event_time < 1789084800000000000;

-- SWCS compaction reads the SST with a time window predicate. Before the
-- fix, min-max pruning compared the window against the small `ns_edge`
-- statistics and dropped every row group.
admin compact_table('json2_swcs_minmax_stats', 'swcs', '86400');

select count(*) from json2_swcs_minmax_stats;

-- Compact once more to also exercise reading compaction-written SSTs.
admin compact_table('json2_swcs_minmax_stats', 'swcs', '86400');

select count(*) from json2_swcs_minmax_stats;

-- Time-range predicates use the same statistics and must not prune the rows
-- either.
select count(*) from json2_swcs_minmax_stats
where event_time >= 1788998400000000000 and event_time < 1789084800000000000;

select seq, event_time, payload
from json2_swcs_minmax_stats
order by seq;

drop table json2_swcs_minmax_stats;
