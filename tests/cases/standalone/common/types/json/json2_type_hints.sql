CREATE TABLE json2_type_hints (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (
        user.age BIGINT NOT NULL DEFAULT 18,
        user.name STRING DEFAULT 'unknown',
        user.active BOOLEAN NULL,
        score DOUBLE NULL DEFAULT 1.5
    )
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
);

INSERT INTO json2_type_hints_required
VALUES (1, '{}');

CREATE TABLE json2_type_hints_timestamp (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (
        event_time TIMESTAMP
    )
);

DROP TABLE json2_type_hints;

DROP TABLE json2_type_hints_required;
