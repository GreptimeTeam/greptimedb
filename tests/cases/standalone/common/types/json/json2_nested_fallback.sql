CREATE TABLE json2_nested_fallback (
    ts timestamp time index,
    j json2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat',
);

-- This file stores j.a as a variant parent because a is both scalar and object.
INSERT INTO json2_nested_fallback
VALUES
    (1, '{"a": 1}'),
    (2, '{"a": {"b": 2}}');

ADMIN FLUSH_TABLE('json2_nested_fallback');

-- This file gives the query a concrete j.a.b shape.
INSERT INTO json2_nested_fallback
VALUES
    (3, '{"a": {"b": 3}}');

ADMIN FLUSH_TABLE('json2_nested_fallback');

SELECT j.a.b FROM json2_nested_fallback ORDER BY ts;

DROP TABLE json2_nested_fallback;

CREATE TABLE json2_nested_fallback_multi (
    ts timestamp time index,
    j json2,
    k json2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat',
);

-- This file mixes scalar and object values so j.a, j.d, and k.x are stored as
-- variant parents. j.b.x still has an exact nested leaf in the same root.
INSERT INTO json2_nested_fallback_multi
VALUES
    (1, '{"a": 1, "d": {"e": 10}, "b": {"x": "exact1"}}', '{"x": 1}'),
    (2, '{"a": {"b": 2, "c": "two"}, "d": 5, "b": {"x": "exact2"}}', '{"x": {"y": "ky2"}}');

ADMIN FLUSH_TABLE('json2_nested_fallback_multi');

-- This file gives the query concrete nested shapes for the fallback paths.
INSERT INTO json2_nested_fallback_multi
VALUES
    (3, '{"a": {"b": 3, "c": "three"}, "d": {"e": 30}, "b": {"x": "exact3"}}', '{"x": {"y": "ky3"}}');

ADMIN FLUSH_TABLE('json2_nested_fallback_multi');

SELECT j.a.b, j.a.c, j.d.e, j.b.x, k.x.y FROM json2_nested_fallback_multi ORDER BY ts;

DROP TABLE json2_nested_fallback_multi;
