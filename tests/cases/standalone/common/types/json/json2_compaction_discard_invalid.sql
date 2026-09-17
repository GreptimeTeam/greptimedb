CREATE TABLE json2_compaction_discard_invalid (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

INSERT INTO json2_compaction_discard_invalid VALUES
    (1, '{"kind":1}'),
    (2, '{"kind":"invalid"}');

ADMIN FLUSH_TABLE('json2_compaction_discard_invalid');

ALTER TABLE json2_compaction_discard_invalid
    MODIFY COLUMN attrs JSON2 (
        kind INT64
    );

INSERT INTO json2_compaction_discard_invalid VALUES
    (3, '{"kind":3}');

ADMIN FLUSH_TABLE('json2_compaction_discard_invalid');

SELECT ts, attrs.kind
FROM json2_compaction_discard_invalid
ORDER BY ts;

ADMIN COMPACT_TABLE('json2_compaction_discard_invalid', 'swcs', '86400');

SELECT ts, attrs.kind
FROM json2_compaction_discard_invalid
ORDER BY ts;

DROP TABLE json2_compaction_discard_invalid;
