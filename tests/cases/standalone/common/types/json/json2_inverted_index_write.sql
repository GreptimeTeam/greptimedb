CREATE TABLE json2_index_write (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (
        max_auto_expanded_paths = 0,
        "service.name" STRING INVERTED INDEX,
        service.name STRING INVERTED INDEX,
        status BIGINT INVERTED INDEX,
        bytes BIGINT UNSIGNED INVERTED INDEX,
        duration DOUBLE INVERTED INDEX,
        success BOOLEAN INVERTED INDEX,
        absent STRING INVERTED INDEX,
        unindexed STRING
    ),
    ordinary STRING INVERTED INDEX
) WITH ('append_mode' = 'true', 'sst_format' = 'flat');

INSERT INTO json2_index_write VALUES
    (1, '{"service.name":"api","service":{"name":"nested"},"status":200,"bytes":18446744073709551615,"duration":1.5,"success":true,"unindexed":"ignored"}', 'a'),
    (2, '{"service":{}}', 'b'),
    (3, '{}', 'c'),
    (4, NULL, 'd');

ADMIN FLUSH_TABLE('json2_index_write');

-- Read physical index metadata, not the table's declared index settings.
SELECT index_type, target_type, target_json, blob_size > 0 AS has_index_data
FROM information_schema.ssts_index_meta
WHERE table_id = (SELECT table_id FROM information_schema.tables WHERE table_name = 'json2_index_write')
ORDER BY target_type, target_json;

INSERT INTO json2_index_write VALUES
    (5, '{"service.name":"worker","service":{"name":"batch"},"status":500}', 'e');

ADMIN FLUSH_TABLE('json2_index_write');

SELECT target_type, COUNT(*) AS index_count
FROM information_schema.ssts_index_meta
WHERE table_id = (SELECT table_id FROM information_schema.tables WHERE table_name = 'json2_index_write')
GROUP BY target_type ORDER BY target_type;

ADMIN COMPACT_TABLE('json2_index_write', 'swcs', '86400');

SELECT index_type, target_type, target_json, blob_size > 0 AS has_index_data
FROM information_schema.ssts_index_meta
WHERE table_id = (SELECT table_id FROM information_schema.tables WHERE table_name = 'json2_index_write')
ORDER BY target_type, target_json;

SELECT COUNT(*) FROM json2_index_write;

DROP TABLE json2_index_write;

-- A JSON-only index must still produce an index file and file-level availability.
CREATE TABLE json2_index_only (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (value BIGINT INVERTED INDEX)
) WITH ('append_mode' = 'true');

INSERT INTO json2_index_only VALUES (1, '{"value":42}'), (2, '{}');

ADMIN FLUSH_TABLE('json2_index_only');

SELECT index_type, target_type, target_json, index_file_size > 0 AS has_index_file
FROM information_schema.ssts_index_meta
WHERE table_id = (SELECT table_id FROM information_schema.tables WHERE table_name = 'json2_index_only');

DROP TABLE json2_index_only;

-- Ignoring the JSON root column disables all of its hinted indexes.
CREATE TABLE json2_index_ignored (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (value BIGINT INVERTED INDEX)
) WITH ('append_mode' = 'true', 'index.inverted_index.ignore_column_ids' = '1');

INSERT INTO json2_index_ignored VALUES (1, '{"value":42}');

ADMIN FLUSH_TABLE('json2_index_ignored');

SELECT COUNT(*) AS index_count
FROM information_schema.ssts_index_meta
WHERE table_id = (SELECT table_id FROM information_schema.tables WHERE table_name = 'json2_index_ignored');

DROP TABLE json2_index_ignored;
