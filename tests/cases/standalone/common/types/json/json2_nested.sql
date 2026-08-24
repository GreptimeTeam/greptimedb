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
    (3, '{"a": {"b": 3}}'),
    (4, '{"a": 2}');

SELECT j.a.b FROM json2_nested_fallback ORDER BY ts;

SELECT j.a, j.a.b FROM json2_nested_fallback ORDER BY ts;

DROP TABLE json2_nested_fallback;

-- Whole JSON2 root and nested paths in the same query.
CREATE TABLE json2_nested_whole_and_path_read (
    ts timestamp time index,
    j json2
) WITH (
    'append_mode' = 'true'
);

INSERT INTO json2_nested_whole_and_path_read
VALUES
    (1, '{"a": {"b": 1}}'),
    (2, '{"a": {"b": 2}}');

-- JSON2 field projection remains supported in an intermediate plan node.
SELECT json_get(j, 'a.b'), count(*)
FROM json2_nested_whole_and_path_read
GROUP BY json_get(j, 'a.b')
ORDER BY json_get(j, 'a.b');

SELECT j, j.a FROM json2_nested_whole_and_path_read;

SELECT j FROM json2_nested_whole_and_path_read WHERE j.a.b = 1;

DROP TABLE json2_nested_whole_and_path_read;

-- JSON2 hints must not mix same-named columns from different join inputs.
CREATE TABLE json2_nested_join_same_name_left (
    ts timestamp time index,
    k string,
    j json2
) WITH (
    'append_mode' = 'true'
);

CREATE TABLE json2_nested_join_same_name_right (
    ts timestamp time index,
    k string,
    j json2
) WITH (
    'append_mode' = 'true'
);

INSERT INTO json2_nested_join_same_name_left
VALUES
    (1, 'a', '{"a": 1, "left_only": "kept"}');

INSERT INTO json2_nested_join_same_name_right
VALUES
    (1, 'a', '{"a": "right", "right_only": "should be kept"}');

ADMIN FLUSH_TABLE('json2_nested_join_same_name_left');

ADMIN FLUSH_TABLE('json2_nested_join_same_name_right');

SELECT json_get(r.j, 'a')::string, json_get(l.j, 'a')::int64
FROM json2_nested_join_same_name_left l
JOIN json2_nested_join_same_name_right r
ON l.k = r.k;

DROP TABLE json2_nested_join_same_name_left;

DROP TABLE json2_nested_join_same_name_right;

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
