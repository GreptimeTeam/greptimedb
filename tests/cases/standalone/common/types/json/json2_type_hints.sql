CREATE TABLE json2_type_hints (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (
        user.age BIGINT,
        user.name STRING,
        user.active BOOLEAN,
        score DOUBLE
    )
) WITH (
    'append_mode' = 'true'
);

SHOW CREATE TABLE json2_type_hints;

INSERT INTO json2_type_hints
VALUES
    (1, '{"user":{"age":42,"name":"Alice","active":true},"score":3.25}'),
    (2, '{"user":{"name":"Bob"}}'),
    (3, '{}');

SELECT
    j.user.age,
    j.user.name,
    j.user.active,
    j.score
FROM json2_type_hints
ORDER BY ts;

INSERT INTO json2_type_hints
VALUES (4, '{"user":{"age":"bad"}}');

CREATE TABLE json2_type_hints_timestamp (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (
        event_time TIMESTAMP
    )
);

-- A type hint at the maximum supported depth is accepted.
CREATE TABLE json2_type_hint_depth_50 (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (
        l01.l02.l03.l04.l05.l06.l07.l08.l09.l10.l11.l12.l13.l14.l15.l16.l17.l18.l19.l20.l21.l22.l23.l24.l25.l26.l27.l28.l29.l30.l31.l32.l33.l34.l35.l36.l37.l38.l39.l40.l41.l42.l43.l44.l45.l46.l47.l48.l49.l50 STRING
    )
) WITH (
    'append_mode' = 'true'
);

-- A type hint beyond the structured depth limit is rejected at CREATE TABLE.
CREATE TABLE json2_type_hint_depth_51 (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (
        l01.l02.l03.l04.l05.l06.l07.l08.l09.l10.l11.l12.l13.l14.l15.l16.l17.l18.l19.l20.l21.l22.l23.l24.l25.l26.l27.l28.l29.l30.l31.l32.l33.l34.l35.l36.l37.l38.l39.l40.l41.l42.l43.l44.l45.l46.l47.l48.l49.l50.l51 STRING
    )
) WITH (
    'append_mode' = 'true'
);

DROP TABLE json2_type_hints;

DROP TABLE json2_type_hint_depth_50;

-- Type hints reject unsupported types and aliases.
CREATE TABLE json2_hint_int8 (ts TIMESTAMP TIME INDEX, j JSON2 (value INT8));

CREATE TABLE json2_hint_tinyint (ts TIMESTAMP TIME INDEX, j JSON2 (value TINYINT));

CREATE TABLE json2_hint_uint8 (ts TIMESTAMP TIME INDEX, j JSON2 (value UINT8));

CREATE TABLE json2_hint_float (ts TIMESTAMP TIME INDEX, j JSON2 (value FLOAT));

CREATE TABLE json2_hint_text (ts TIMESTAMP TIME INDEX, j JSON2 (value TEXT));

CREATE TABLE json2_hint_unsigned (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (value BIGINT UNSIGNED)
) WITH ('append_mode' = 'true');

INSERT INTO json2_hint_unsigned VALUES (1, '{"value":18446744073709551615}');

SELECT j.value FROM json2_hint_unsigned;

ALTER TABLE json2_hint_unsigned MODIFY COLUMN j JSON2 (value INT8);

DROP TABLE json2_hint_unsigned;

-- These aliases preserve the exact type and are displayed using canonical names.
CREATE TABLE json2_hint_aliases (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (i INT64, u UINT64, f FLOAT64)
) WITH ('append_mode' = 'true');

SHOW CREATE TABLE json2_hint_aliases;

INSERT INTO json2_hint_aliases VALUES
    (1, '{"i":-9223372036854775808,"u":18446744073709551615,"f":1.5}');

SELECT j.i AS i, j.u AS u, j.f AS f FROM json2_hint_aliases;

ALTER TABLE json2_hint_aliases MODIFY COLUMN j JSON2 (i INT64, u UINT64, f FLOAT64);

SHOW CREATE TABLE json2_hint_aliases;

SELECT j.i AS i, j.u AS u, j.f AS f FROM json2_hint_aliases;

DROP TABLE json2_hint_aliases;
