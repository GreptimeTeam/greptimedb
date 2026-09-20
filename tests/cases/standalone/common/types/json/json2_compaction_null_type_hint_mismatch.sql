CREATE TABLE json2_compaction_null_type_hint_mismatch (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

INSERT INTO json2_compaction_null_type_hint_mismatch VALUES
    (1, '{"kind":1}'),
    (2, '{"kind":"invalid","message":"keep"}');

ADMIN FLUSH_TABLE('json2_compaction_null_type_hint_mismatch');

ALTER TABLE json2_compaction_null_type_hint_mismatch
    MODIFY COLUMN attrs JSON2 (
        kind INT64
    );

INSERT INTO json2_compaction_null_type_hint_mismatch VALUES
    (3, '{"kind":3}');

ADMIN FLUSH_TABLE('json2_compaction_null_type_hint_mismatch');

SELECT ts, attrs.kind, attrs.message
FROM json2_compaction_null_type_hint_mismatch
ORDER BY ts;

ADMIN COMPACT_TABLE('json2_compaction_null_type_hint_mismatch', 'swcs', '86400');

SELECT ts, attrs.kind, attrs.message
FROM json2_compaction_null_type_hint_mismatch
ORDER BY ts;

DROP TABLE json2_compaction_null_type_hint_mismatch;
