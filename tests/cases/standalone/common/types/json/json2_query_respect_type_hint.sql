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
    (2, '{"a":7,"b":"7"}', 2),
    (3, '{}', 2),
    (4, '{"a":null,"b":null}', 2);

-- Both syntaxes use the hint when reading the memtable, including missing/null paths.
SELECT j.a AS dotted, json_get(j, 'a') AS direct,
       arrow_typeof(j.a) AS dotted_type, arrow_typeof(json_get(j, 'a')) AS direct_type
FROM json2_query_respect_type_hint
ORDER BY ts;

-- An explicit third argument takes precedence over the BIGINT path hint in memtable reads.
SELECT json_get(j, 'a') AS hinted_bigint,
       json_get(j, 'a', NULL::DOUBLE) AS explicit_double,
       arrow_typeof(json_get(j, 'a')) AS hinted_type,
       arrow_typeof(json_get(j, 'a', NULL::DOUBLE)) AS explicit_type
FROM json2_query_respect_type_hint
ORDER BY ts;

ADMIN FLUSH_TABLE('json2_query_respect_type_hint');

-- The explicit read type also overrides the hint in the plan and SST reads.
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT json_get(j, 'a', NULL::DOUBLE) AS explicit_double
FROM json2_query_respect_type_hint;

SELECT json_get(j, 'a') AS hinted_bigint,
       json_get(j, 'a', NULL::DOUBLE) AS explicit_double,
       arrow_typeof(json_get(j, 'a')) AS hinted_type,
       arrow_typeof(json_get(j, 'a', NULL::DOUBLE)) AS explicit_type
FROM json2_query_respect_type_hint
ORDER BY ts;

-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT j.a AS hinted_bigint, json_get(j, 'a') AS direct_bigint
FROM json2_query_respect_type_hint;

-- Uncast paths use their JSON2 type hints.
SELECT
    j.a AS hinted_bigint,
    json_get(j, 'a') AS direct_bigint,
    json_get(j, '$.a') AS rooted_bigint,
    j.b AS hinted_string,
    json_get(j, 'b') AS direct_string
FROM json2_query_respect_type_hint
ORDER BY ts;

-- The SST read keeps the same result types as the memtable read.
SELECT arrow_typeof(j.a) AS dotted_type, arrow_typeof(json_get(j, 'a')) AS direct_type,
       arrow_typeof(j.b) AS dotted_string_type, arrow_typeof(json_get(j, 'b')) AS direct_string_type
FROM json2_query_respect_type_hint LIMIT 1;

-- A JSON2 type hint takes precedence over an explicit SQL cast for the native
-- read type.
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT j.a::STRING, json_get(j, 'a')::STRING
FROM json2_query_respect_type_hint;

-- An explicit CAST changes the expression result type.
SELECT
    j.a::STRING AS cast_string,
    json_get(j, 'a')::STRING AS direct_cast_string,
    arrow_typeof(json_get(j, 'a')::STRING) AS cast_type
FROM json2_query_respect_type_hint
ORDER BY ts;

-- A JSON2 type hint takes precedence over expression-context inference.
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT coalesce(j.b, exponent), coalesce(json_get(j, 'b'), exponent)
FROM json2_query_respect_type_hint;

-- Hint injection precedes scalar, aggregate, window and binary-expression inference.
SELECT abs(j.a) AS dotted_abs, abs(json_get(j, 'a')) AS direct_abs,
       j.a + 1 AS dotted_plus, json_get(j, 'a') + 1 AS direct_plus
FROM json2_query_respect_type_hint ORDER BY ts;

SELECT sum(j.a) AS dotted_sum, sum(json_get(j, 'a')) AS direct_sum
FROM json2_query_respect_type_hint;

SELECT sum(j.a) OVER (ORDER BY ts) AS dotted_sum,
       sum(json_get(j, 'a')) OVER (ORDER BY ts) AS direct_sum
FROM json2_query_respect_type_hint ORDER BY ts;

-- A STRING type hint is respected, so numeric arithmetic requires an explicit cast.
SELECT j.b + exponent
FROM json2_query_respect_type_hint;

SELECT json_get(j, 'b') + exponent
FROM json2_query_respect_type_hint;

-- An explicit cast changes the expression type while preserving the STRING native read type.
-- The invalid string value still reports a normal SQL cast error.
SELECT j.b::DOUBLE + exponent
FROM json2_query_respect_type_hint;

SELECT json_get(j, 'b')::DOUBLE + exponent
FROM json2_query_respect_type_hint;

-- Resolve the input field through table aliases and a renamed derived-table column.
SELECT t.j.a AS dotted, json_get(t.j, 'a') AS direct
FROM json2_query_respect_type_hint AS t ORDER BY t.ts;

SELECT q.payload.a AS dotted, json_get(q.payload, 'a') AS direct
FROM (SELECT ts, j AS payload FROM json2_query_respect_type_hint) AS q ORDER BY q.ts;

DROP TABLE json2_query_respect_type_hint;

CREATE TABLE json2_hint_paths (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (nested.a BIGINT, "nested.a" BOOLEAN)
) WITH (
    'append_mode' = 'true'
);

INSERT INTO json2_hint_paths VALUES
    (1, '{"nested":{"a":42},"nested.a":true,"unhinted":7}');

-- Quoted keys are literal names, not nested paths; unhinted paths retain their default type.
SELECT j.nested.a AS dotted_nested, json_get(j, '$.nested.a') AS direct_nested,
       j."nested.a" AS dotted_key, json_get(j, '$."nested.a"') AS direct_key,
       arrow_typeof(json_get(j, 'nested.a')) AS nested_type,
       arrow_typeof(json_get(j, '$."nested.a"')) AS key_type,
       arrow_typeof(j.unhinted) AS dotted_unhinted_type,
       arrow_typeof(json_get(j, 'unhinted')) AS direct_unhinted_type
FROM json2_hint_paths;

DROP TABLE json2_hint_paths;

-- Hint lookup must defer unqualified JOIN USING columns to DataFusion, even for legacy JSON.
SELECT json_get(j, 'a') AS value
FROM (SELECT parse_json('{"a":"x"}') AS j) AS l
JOIN (SELECT parse_json('{"a":"x"}') AS j) AS r USING (j);

-- Without USING, the unqualified column is genuinely ambiguous and must still fail.
SELECT json_get(j, 'a') AS value
FROM (SELECT parse_json('{"a":"x"}') AS j) AS l
JOIN (SELECT parse_json('{"a":"x"}') AS j) AS r ON l.j = r.j;
