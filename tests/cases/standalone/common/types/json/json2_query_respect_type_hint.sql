CREATE TABLE json2_query_respect_type_hint (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (
        a BIGINT,
        b STRING
    ),
    exponent DOUBLE
) WITH (
    'append_mode' = 'true'
);

INSERT INTO json2_query_respect_type_hint VALUES
    (1, '{"a":9007199254740993,"b":"three"}', 2),
    (2, '{"a":7,"b":"7"}', 2);

ADMIN FLUSH_TABLE('json2_query_respect_type_hint');

-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT j.a AS hinted_bigint
FROM json2_query_respect_type_hint;

-- Uncast paths use their JSON2 type hints.
SELECT
    j.a AS hinted_bigint,
    j.b AS hinted_string
FROM json2_query_respect_type_hint
ORDER BY ts;

-- A JSON2 type hint takes precedence over an explicit SQL cast for the native
-- read type.
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT j.a::STRING
FROM json2_query_respect_type_hint;

-- An explicit CAST changes the expression result type.
SELECT
    j.a::STRING AS cast_string
FROM json2_query_respect_type_hint
ORDER BY ts;

-- A JSON2 type hint takes precedence over expression-context inference.
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT coalesce(j.b, exponent)
FROM json2_query_respect_type_hint;

-- A STRING type hint is respected, so numeric arithmetic requires an explicit cast.
SELECT j.b + exponent
FROM json2_query_respect_type_hint;

-- An explicit cast changes the expression type while preserving the STRING native read type.
-- The invalid string value still reports a normal SQL cast error.
SELECT j.b::DOUBLE + exponent
FROM json2_query_respect_type_hint;

DROP TABLE json2_query_respect_type_hint;
