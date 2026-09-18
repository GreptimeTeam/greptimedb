CREATE TABLE json2_type_hint_read_type (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (
        a BIGINT,
        b STRING
    )
) WITH (
    'append_mode' = 'true'
);

INSERT INTO json2_type_hint_read_type VALUES
    (1, '{"a":42,"b":"42"}'),
    (2, '{"a":7,"b":"7"}');

ADMIN FLUSH_TABLE('json2_type_hint_read_type');

-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT j.a AS hinted_bigint
FROM json2_type_hint_read_type;

-- Uncast paths use their JSON2 type hints.
SELECT
    j.a AS hinted_bigint,
    j.b AS hinted_string
FROM json2_type_hint_read_type
ORDER BY ts;

-- An explicit CAST overrides the JSON2 type hint.
SELECT
    j.a::STRING AS overridden_string,
    j.b::BIGINT AS overridden_bigint
FROM json2_type_hint_read_type
ORDER BY ts;

DROP TABLE json2_type_hint_read_type;
