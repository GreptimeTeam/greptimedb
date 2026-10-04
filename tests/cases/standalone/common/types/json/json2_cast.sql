-- Read-time scalar casts. json_get(..., path)::type is rewritten to typed json_get.
-- Keep each value in a separate SST so mixed JSON2 physical layouts are exercised.
CREATE TABLE json2_cast_scalar (
    ts TIMESTAMP TIME INDEX,
    j JSON2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

INSERT INTO json2_cast_scalar VALUES (1, '{"a":42}');
ADMIN FLUSH_TABLE('json2_cast_scalar');

INSERT INTO json2_cast_scalar VALUES (2, '{"a":"42"}');
ADMIN FLUSH_TABLE('json2_cast_scalar');

INSERT INTO json2_cast_scalar VALUES (3, '{"a":"bad"}');
ADMIN FLUSH_TABLE('json2_cast_scalar');

INSERT INTO json2_cast_scalar VALUES (4, '{"a":3.14}');
ADMIN FLUSH_TABLE('json2_cast_scalar');

INSERT INTO json2_cast_scalar VALUES (5, '{"a":true}');
ADMIN FLUSH_TABLE('json2_cast_scalar');

INSERT INTO json2_cast_scalar VALUES (6, '{"a":null}');
ADMIN FLUSH_TABLE('json2_cast_scalar');

INSERT INTO json2_cast_scalar VALUES (7, '{"a":{"b":1}}');
ADMIN FLUSH_TABLE('json2_cast_scalar');

INSERT INTO json2_cast_scalar VALUES (8, '{"a":[1,2]}');
ADMIN FLUSH_TABLE('json2_cast_scalar');

INSERT INTO json2_cast_scalar VALUES (9, '{"z":0}');
ADMIN FLUSH_TABLE('json2_cast_scalar');

SELECT
    ts,
    json_get(j, 'a')::BIGINT AS a_bigint,
    json_get(j, 'a')::DOUBLE AS a_double,
    json_get(j, 'a')::BOOLEAN AS a_bool,
    json_get(j, 'a')::STRING AS a_string
FROM json2_cast_scalar
ORDER BY ts;

DROP TABLE json2_cast_scalar;

-- Read-time parent projection while only child leaves are stored.
CREATE TABLE json2_cast_parent (
    ts TIMESTAMP TIME INDEX,
    j JSON2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

INSERT INTO json2_cast_parent VALUES (1, '{"a":{"b":1,"c":2}}');
ADMIN FLUSH_TABLE('json2_cast_parent');

INSERT INTO json2_cast_parent VALUES (2, '{"a":{"b":"1","c":"2"}}');
ADMIN FLUSH_TABLE('json2_cast_parent');

INSERT INTO json2_cast_parent VALUES (3, '{"a":{"b":true,"c":false}}');
ADMIN FLUSH_TABLE('json2_cast_parent');

SELECT
    ts,
    json_get(j, 'a')::STRING AS a_string,
    json_get(j, 'a')::INT AS a_int,
    json_get(j, 'a.b')::BIGINT AS ab_bigint,
    json_get(j, 'a.c')::STRING AS ac_string
FROM json2_cast_parent
ORDER BY ts;

DROP TABLE json2_cast_parent;

-- Write-time schema alignment across different ingest batches.
CREATE TABLE json2_cast_write_alignment (
    ts TIMESTAMP TIME INDEX,
    j JSON2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

INSERT INTO json2_cast_write_alignment VALUES
    (1, '{"s":null,"b":null,"n":null,"o":null,"l":null,"ui":18446744073709551615,"uf":9007199254740993,"if":-9007199254740993}');

INSERT INTO json2_cast_write_alignment VALUES
    (2, '{"s":"text","b":true,"n":42,"o":{"x":1},"l":[1,2],"ui":-1,"uf":1.5,"if":1.5}');

ADMIN FLUSH_TABLE('json2_cast_write_alignment');

SELECT
    ts,
    json_get(j, 's')::STRING AS null_to_string,
    json_get(j, 'b')::BOOLEAN AS null_to_bool,
    json_get(j, 'n')::UINT64 AS null_to_uint,
    json_get(j, 'o')::STRING AS null_to_object,
    json_get(j, 'l')::STRING AS null_to_list,
    json_get(j, 'ui')::STRING AS uint_int_variant,
    json_get(j, 'uf')::STRING AS uint_float_variant,
    json_get(j, 'if')::STRING AS int_float_variant
FROM json2_cast_write_alignment
ORDER BY ts;

DROP TABLE json2_cast_write_alignment;
