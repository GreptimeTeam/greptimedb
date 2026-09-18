CREATE TABLE json2_type_hint_read_type (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (
        a BIGINT,
        b STRING
    ),
    exponent DOUBLE
) WITH (
    'append_mode' = 'true'
);

INSERT INTO json2_type_hint_read_type VALUES
    (1, '{"a":42,"b":"three"}', 2),
    (2, '{"a":7,"b":"7"}', 2);

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

-- A matching JSON2 type hint takes precedence over function-context inference.
SELECT coalesce(j.b, exponent) AS hinted_string
FROM json2_type_hint_read_type
ORDER BY ts;

-- An explicit CAST overrides the JSON2 type hint.
SELECT
    j.a::STRING AS overridden_string
FROM json2_type_hint_read_type
ORDER BY ts;

DROP TABLE json2_type_hint_read_type;
