-- Regression coverage for statistics pruning with JSON2 columns.
--
-- A time-range query must use the event_time Parquet leaf statistics, not a
-- JSON2 child leaf that happens to have the same logical root-column index.
CREATE TABLE json2_statistics_time (
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

-- event_time has logical root index 7. After JSON2 expansion, physical leaf 7
-- is payload.ordinal, whose min/max must not be used to prune event_time.
INSERT INTO json2_statistics_time VALUES
    ('workspace', 'session-1', 1, 'event', '{"case":"one","ordinal":1}', 1, false, 1704067200000),
    ('workspace', 'session-2', 2, 'event', '{"case":"two","ordinal":2}', 1, false, 1704153600000),
    ('workspace', 'session-3', 3, 'event', '{"case":"three","ordinal":3}', 1, false, 1704240000000),
    ('workspace', 'session-1', 4, 'event', '{"case":"four","ordinal":4}', 1, false, 1704326400000);

ADMIN FLUSH_TABLE('json2_statistics_time');

SELECT COUNT(*) AS row_count_without_time_filter FROM json2_statistics_time;

SELECT COUNT(*) AS row_count_with_time_filter
FROM json2_statistics_time
WHERE event_time >= 1704067200000
  AND event_time < 1704412800000;

DROP TABLE json2_statistics_time;

-- A type-hinted child has a dedicated, typed Parquet leaf. Its predicate must
-- keep the matching row after a flush.
CREATE TABLE json2_statistics_type_hint (
    ts TIMESTAMP TIME INDEX,
    payload JSON2 (
        service STRING,
        latency BIGINT
    )
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

INSERT INTO json2_statistics_type_hint VALUES
    (1, '{"service":"api","latency":10}'),
    (2, '{"service":"api","latency":20}'),
    (3, '{"service":"api","latency":30}');

ADMIN FLUSH_TABLE('json2_statistics_type_hint');

SELECT COUNT(*) AS row_count_with_type_hint_filter
FROM json2_statistics_type_hint
WHERE payload.latency >= 20;

DROP TABLE json2_statistics_type_hint;

-- An unhinted child is stored in the JSON2 v2 remainder. It must remain
-- filterable after a flush even though it has no dedicated typed leaf.
CREATE TABLE json2_statistics_remainder (
    ts TIMESTAMP TIME INDEX,
    payload JSON2 (
        service STRING
    )
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

INSERT INTO json2_statistics_remainder VALUES
    (1, '{"service":"api","remainder_label":"discard"}'),
    (2, '{"service":"api","remainder_label":"keep"}'),
    (3, '{"service":"api","remainder_label":"discard"}');

ADMIN FLUSH_TABLE('json2_statistics_remainder');

SELECT COUNT(*) AS row_count_with_remainder_filter
FROM json2_statistics_remainder
WHERE payload.remainder_label = 'keep';

DROP TABLE json2_statistics_remainder;
