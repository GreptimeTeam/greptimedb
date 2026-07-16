-- Basic nested path fallback to a variant parent.
CREATE TABLE json2_nested_fallback (
    ts timestamp time index,
    j json2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat',
);

INSERT INTO json2_nested_fallback
VALUES
    (1, '{"a": 1}'),
    (2, '{"a": {"b": 2}}');

ADMIN FLUSH_TABLE('json2_nested_fallback');

INSERT INTO json2_nested_fallback
VALUES
    (3, '{"a": {"b": 3}}');

ADMIN FLUSH_TABLE('json2_nested_fallback');

SELECT j.a.b FROM json2_nested_fallback ORDER BY ts;

DROP TABLE json2_nested_fallback;

-- Multiple roots and nested paths with mixed exact and fallback reads.
CREATE TABLE json2_nested_fallback_multi (
    ts timestamp time index,
    j json2,
    k json2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat',
);

INSERT INTO json2_nested_fallback_multi
VALUES
    (1, '{"a": 1, "d": {"e": 10}, "b": {"x": "exact1"}}', '{"x": 1}'),
    (2, '{"a": {"b": 2, "c": "two"}, "d": 5, "b": {"x": "exact2"}}', '{"x": {"y": "ky2"}}');

ADMIN FLUSH_TABLE('json2_nested_fallback_multi');

INSERT INTO json2_nested_fallback_multi
VALUES
    (3, '{"a": {"b": 3, "c": "three"}, "d": {"e": 30}, "b": {"x": "exact3"}}', '{"x": {"y": "ky3"}}');

ADMIN FLUSH_TABLE('json2_nested_fallback_multi');

SELECT j.a.b, j.a.c, j.d.e, j.b.x, k.x.y FROM json2_nested_fallback_multi ORDER BY ts;

DROP TABLE json2_nested_fallback_multi;

-- Deep nested path fallback to an intermediate variant parent.
CREATE TABLE json2_nested_fallback_to_parent (
    ts timestamp time index,
    j json2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat',
);

INSERT INTO json2_nested_fallback_to_parent
VALUES
    (1, '{"a": 1}'),
    (2, '{"a": {"b": {"c": "from_parent"}}}');

ADMIN FLUSH_TABLE('json2_nested_fallback_to_parent');

INSERT INTO json2_nested_fallback_to_parent
VALUES
    (3, '{"a": {"b": {"c": "exact_parent"}}}');

ADMIN FLUSH_TABLE('json2_nested_fallback_to_parent');

SELECT j.a.b.c FROM json2_nested_fallback_to_parent ORDER BY ts;

DROP TABLE json2_nested_fallback_to_parent;

-- Multiple deep nested paths from the same fallback parent.
CREATE TABLE json2_nested_deep_multi (
    ts timestamp time index,
    j json2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat',
);

INSERT INTO json2_nested_deep_multi
VALUES
    (1, '{"a": 1}'),
    (2, '{"a": {"b": {"c": "c2", "d": "d2"}}}');

ADMIN FLUSH_TABLE('json2_nested_deep_multi');

INSERT INTO json2_nested_deep_multi
VALUES
    (3, '{"a": {"b": {"c": "c3", "d": "d3"}}}');

ADMIN FLUSH_TABLE('json2_nested_deep_multi');

SELECT j.a.b.c, j.a.b.d FROM json2_nested_deep_multi ORDER BY ts;

DROP TABLE json2_nested_deep_multi;

-- Null parent value when reading a child nested path.
CREATE TABLE json2_nested_null_parent (
    ts timestamp time index,
    j json2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat',
);

INSERT INTO json2_nested_null_parent
VALUES
    (1, '{"a": null}'),
    (2, '{"a": {"b": "from_null_parent"}}');

ADMIN FLUSH_TABLE('json2_nested_null_parent');

INSERT INTO json2_nested_null_parent
VALUES
    (3, '{"a": {"b": "exact_null_parent"}}');

ADMIN FLUSH_TABLE('json2_nested_null_parent');

SELECT j.a.b FROM json2_nested_null_parent ORDER BY ts;

DROP TABLE json2_nested_null_parent;

-- Array parent value when reading an object child nested path.
CREATE TABLE json2_nested_array_parent (
    ts timestamp time index,
    j json2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat',
);

INSERT INTO json2_nested_array_parent
VALUES
    (1, '{"a": [1, 2]}'),
    (2, '{"a": {"b": "from_array_parent"}}');

ADMIN FLUSH_TABLE('json2_nested_array_parent');

INSERT INTO json2_nested_array_parent
VALUES
    (3, '{"a": {"b": "exact_array_parent"}}');

ADMIN FLUSH_TABLE('json2_nested_array_parent');

SELECT j.a.b FROM json2_nested_array_parent ORDER BY ts;

DROP TABLE json2_nested_array_parent;

-- Typed casts after reading nested paths through fallback.
CREATE TABLE json2_nested_typed_cast (
    ts timestamp time index,
    j json2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat',
);

INSERT INTO json2_nested_typed_cast
VALUES
    (1, '{"a": 1}'),
    (2, '{"a": {"flag": true, "score": 1.5, "n": 10}}'),
    (3, '{"a": {"flag": "bad", "score": {"x": 1}, "n": "bad"}}');

ADMIN FLUSH_TABLE('json2_nested_typed_cast');

INSERT INTO json2_nested_typed_cast
VALUES
    (4, '{"a": {"flag": false, "score": 2.5, "n": 20}}');

ADMIN FLUSH_TABLE('json2_nested_typed_cast');

SELECT j.a.flag::BOOLEAN, j.a.score::DOUBLE, j.a.n::INT64 FROM json2_nested_typed_cast ORDER BY ts;

DROP TABLE json2_nested_typed_cast;
