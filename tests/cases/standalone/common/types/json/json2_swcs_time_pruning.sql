-- JSON2 expands into multiple Parquet leaf columns. The time index follows the
-- JSON2 root column, so SWCS must use the time index's physical leaf position
-- when applying a time-range predicate to compaction input.
CREATE TABLE json2_swcs_time_pruning (
    workspace_id STRING,
    session_id STRING,
    seq BIGINT,
    entry_kind STRING,
    payload JSON2,
    schema_version INT,
    is_error BOOLEAN,
    event_time TIMESTAMP TIME INDEX
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

-- The time index has logical root index 7. JSON2 expands to the physical
-- leaves `remainder.metadata`, `remainder.value`, `case`, and `ordinal`; a
-- broken root-to-leaf mapping therefore reads `payload.ordinal` as event_time.
INSERT INTO json2_swcs_time_pruning VALUES
    ('workspace', 'session-1', 1, 'event', '{"case":"one","ordinal":1}', 1, false, 1704067200000);
ADMIN FLUSH_TABLE('json2_swcs_time_pruning');

INSERT INTO json2_swcs_time_pruning VALUES
    ('workspace', 'session-2', 2, 'event', '{"case":"two","ordinal":2}', 1, false, 1704153600000);
ADMIN FLUSH_TABLE('json2_swcs_time_pruning');

INSERT INTO json2_swcs_time_pruning VALUES
    ('workspace', 'session-3', 3, 'event', '{"case":"three","ordinal":3}', 1, false, 1704240000000);
ADMIN FLUSH_TABLE('json2_swcs_time_pruning');

INSERT INTO json2_swcs_time_pruning VALUES
    ('workspace', 'session-1', 4, 'event', '{"case":"four","ordinal":4}', 1, false, 1704326400000);
ADMIN FLUSH_TABLE('json2_swcs_time_pruning');

SELECT COUNT(*) AS row_count_before_compaction FROM json2_swcs_time_pruning;

ADMIN COMPACT_TABLE('json2_swcs_time_pruning', 'swcs', '86400');

-- This returns fewer than four rows on the buggy path.
SELECT COUNT(*) AS row_count_after_compaction FROM json2_swcs_time_pruning;

DROP TABLE json2_swcs_time_pruning;
