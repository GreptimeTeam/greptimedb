CREATE TABLE json2_type_hints (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (
        user.age BIGINT NOT NULL DEFAULT 18,
        user.name STRING DEFAULT 'unknown',
        user.active BOOLEAN NULL,
        score DOUBLE NULL DEFAULT 1.5
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

CREATE TABLE json2_type_hints_required (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (
        user.age BIGINT NOT NULL
    )
) WITH (
    'append_mode' = 'true'
);

INSERT INTO json2_type_hints_required
VALUES (1, '{}');

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

DROP TABLE json2_type_hints_required;

DROP TABLE json2_type_hint_depth_50;
