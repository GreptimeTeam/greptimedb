CREATE TABLE json2_alter_type_hints_compaction_conversion (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

INSERT INTO json2_alter_type_hints_compaction_conversion VALUES
    (1, '{"to_string":123,"to_int":"456"}');

ADMIN FLUSH_TABLE('json2_alter_type_hints_compaction_conversion');

ALTER TABLE json2_alter_type_hints_compaction_conversion
    MODIFY COLUMN attrs JSON2 (
        to_string STRING,
        to_int BIGINT
    );

INSERT INTO json2_alter_type_hints_compaction_conversion VALUES
    (2, '{"to_string":"after","to_int":789}');

ADMIN FLUSH_TABLE('json2_alter_type_hints_compaction_conversion');

SELECT ts, attrs.to_string, attrs.to_int
FROM json2_alter_type_hints_compaction_conversion
ORDER BY ts;

ADMIN COMPACT_TABLE('json2_alter_type_hints_compaction_conversion', 'swcs', '86400');

SELECT ts, attrs.to_string, attrs.to_int
FROM json2_alter_type_hints_compaction_conversion
ORDER BY ts;

DROP TABLE json2_alter_type_hints_compaction_conversion;
