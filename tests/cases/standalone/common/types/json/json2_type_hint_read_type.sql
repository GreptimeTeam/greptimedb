CREATE TABLE json2_type_hint_read_type (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (
        a BIGINT,
        b STRING
    )
) WITH (
    'append_mode' = 'true'
);

INSERT INTO json2_type_hint_read_type VALUES
    (1, '{"a":42,"b":"42"}'),
    (2, '{"a":7,"b":"7"}');

ADMIN FLUSH_TABLE('json2_type_hint_read_type');

-- Uncast paths use their JSON2 type hints.
SELECT j.a, j.b
FROM json2_type_hint_read_type
ORDER BY ts;

-- An explicit CAST overrides the JSON2 type hint.
SELECT j.a::STRING, j.b::BIGINT
FROM json2_type_hint_read_type
ORDER BY ts;

DROP TABLE json2_type_hint_read_type;
