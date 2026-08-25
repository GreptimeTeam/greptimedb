CREATE TABLE json_empty_no_type_hints (
    ts TIMESTAMP TIME INDEX,
    app_name STRING,
    log_level STRING,
    log_message STRING,
    attrs JSON2
) WITH (
    'append_mode' = 'true'
);

INSERT INTO json_empty_no_type_hints
VALUES
    (1, 'checkout', 'INFO', 'request completed', '{}');

SELECT *
FROM json_empty_no_type_hints
ORDER BY ts;

INSERT INTO json_empty_no_type_hints
VALUES
    (2, 'checkout', 'INFO', 'request completed', '{"a": 1}');

SELECT
    ts,
    app_name,
    log_level,
    log_message,
    json_get(attrs, 'a')::int64
FROM json_empty_no_type_hints
ORDER BY ts;

SELECT *
FROM json_empty_no_type_hints
ORDER BY ts;

ADMIN FLUSH_TABLE('json_empty_no_type_hints');

SELECT
    ts,
    app_name,
    log_level,
    log_message,
    json_get(attrs, 'a')::int64
FROM json_empty_no_type_hints
ORDER BY ts;

INSERT INTO json_empty_no_type_hints
VALUES
    (3, 'checkout', 'INFO', 'request completed', '{"a": 2}');

SELECT
    ts,
    app_name,
    log_level,
    log_message,
    json_get(attrs, 'a')::int64
FROM json_empty_no_type_hints
ORDER BY ts;

ADMIN FLUSH_TABLE('json_empty_no_type_hints');

ADMIN COMPACT_TABLE('json_empty_no_type_hints', 'swcs', '86400');

SELECT
    ts,
    app_name,
    log_level,
    log_message,
    json_get(attrs, 'a')::int64
FROM json_empty_no_type_hints
ORDER BY ts;

CREATE TABLE json_empty_after_object (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2
) WITH (
    'append_mode' = 'true'
);

INSERT INTO json_empty_after_object
VALUES
    (1, '{"a": 1}'),
    (2, '{}');

SELECT ts, json_get(attrs, 'a')::int64
FROM json_empty_after_object
ORDER BY ts;

SELECT *
FROM json_empty_after_object
ORDER BY ts;

ADMIN FLUSH_TABLE('json_empty_after_object');

SELECT ts, json_get(attrs, 'a')::int64
FROM json_empty_after_object
ORDER BY ts;

INSERT INTO json_empty_after_object
VALUES (3, '{}');

ADMIN FLUSH_TABLE('json_empty_after_object');

ADMIN COMPACT_TABLE('json_empty_after_object', 'swcs', '86400');

SELECT ts, json_get(attrs, 'a')::int64
FROM json_empty_after_object
ORDER BY ts;

SELECT *
FROM json_empty_after_object
ORDER BY ts;

CREATE TABLE json_empty_only_sst (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2
) WITH (
    'append_mode' = 'true'
);

INSERT INTO json_empty_only_sst
VALUES (1, '{}');

ADMIN FLUSH_TABLE('json_empty_only_sst');

SELECT *
FROM json_empty_only_sst
ORDER BY ts;

ADMIN COMPACT_TABLE('json_empty_only_sst', 'swcs', '86400');

SELECT *
FROM json_empty_only_sst
ORDER BY ts;

CREATE TABLE json_empty_conflict (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2
) WITH (
    'append_mode' = 'true'
);

INSERT INTO json_empty_conflict
VALUES
    (1, '{}'),
    (2, '{"a": 1}');

ADMIN FLUSH_TABLE('json_empty_conflict');

INSERT INTO json_empty_conflict
VALUES (3, '{"a": "x"}');

SELECT ts, json_get(attrs, 'a')
FROM json_empty_conflict
ORDER BY ts;

SELECT *
FROM json_empty_conflict
ORDER BY ts;

ADMIN FLUSH_TABLE('json_empty_conflict');

ADMIN COMPACT_TABLE('json_empty_conflict', 'swcs', '86400');

SELECT ts, json_get(attrs, 'a')
FROM json_empty_conflict
ORDER BY ts;

SELECT *
FROM json_empty_conflict
ORDER BY ts;

CREATE TABLE json_empty_type_hints_nullable (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (
        user.age BIGINT NULL
    )
) WITH (
    'append_mode' = 'true'
);

INSERT INTO json_empty_type_hints_nullable
VALUES (1, '{}');

ADMIN FLUSH_TABLE('json_empty_type_hints_nullable');

INSERT INTO json_empty_type_hints_nullable
VALUES (2, '{}');

SELECT j.user.age
FROM json_empty_type_hints_nullable
ORDER BY ts;

CREATE TABLE json_empty_type_hints_default (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (
        user.age BIGINT NULL DEFAULT 18
    )
) WITH (
    'append_mode' = 'true'
);

INSERT INTO json_empty_type_hints_default
VALUES (1, '{}');

ADMIN FLUSH_TABLE('json_empty_type_hints_default');

INSERT INTO json_empty_type_hints_default
VALUES (2, '{}');

SELECT j.user.age
FROM json_empty_type_hints_default
ORDER BY ts;

CREATE TABLE json_empty_type_hints_required (
    ts TIMESTAMP TIME INDEX,
    j JSON2 (
        user.age BIGINT NOT NULL
    )
) WITH (
    'append_mode' = 'true'
);

INSERT INTO json_empty_type_hints_required
VALUES (1, '{}');

DROP TABLE json_empty_no_type_hints;

DROP TABLE json_empty_after_object;

DROP TABLE json_empty_only_sst;

DROP TABLE json_empty_conflict;

DROP TABLE json_empty_type_hints_nullable;

DROP TABLE json_empty_type_hints_default;

DROP TABLE json_empty_type_hints_required;
