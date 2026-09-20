CREATE TABLE json2_alter_type_hints_compaction (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

INSERT INTO json2_alter_type_hints_compaction VALUES
    (1, '{"kind":1,"message":"before-has"}'),
    (2, '{"message":"before-missing"}');

ADMIN FLUSH_TABLE('json2_alter_type_hints_compaction');

ALTER TABLE json2_alter_type_hints_compaction
    MODIFY COLUMN attrs JSON2 (
        kind INT64
    );

INSERT INTO json2_alter_type_hints_compaction VALUES
    (3, '{"kind":3,"message":"after-has"}'),
    (4, '{"message":"after-missing"}');

ADMIN FLUSH_TABLE('json2_alter_type_hints_compaction');

SELECT ts, attrs.kind, attrs.message
FROM json2_alter_type_hints_compaction
ORDER BY ts;

ADMIN COMPACT_TABLE('json2_alter_type_hints_compaction', 'swcs', '86400');

SELECT ts, attrs.kind, attrs.message
FROM json2_alter_type_hints_compaction
ORDER BY ts;

DROP TABLE json2_alter_type_hints_compaction;
