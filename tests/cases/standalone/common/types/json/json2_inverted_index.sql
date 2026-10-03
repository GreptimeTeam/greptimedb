-- Index declarations round-trip through table metadata and SHOW CREATE TABLE.
CREATE TABLE json2_inverted_index (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (
        max_auto_expanded_paths = 0,
        "service.name" STRING INVERTED INDEX,
        service.name STRING INVERTED INDEX,
        http.status_code INT INVERTED INDEX,
        request.bytes BIGINT UNSIGNED INVERTED INDEX,
        duration DOUBLE INVERTED INDEX,
        success BOOLEAN INVERTED INDEX,
        message STRING
    )
) WITH ('append_mode' = 'true');

SHOW CREATE TABLE json2_inverted_index;

-- A rejected ALTER must preserve the existing declarations.
ALTER TABLE json2_inverted_index MODIFY COLUMN attrs JSON2 (
    message STRING INVERTED INDEX INVERTED INDEX
);

ALTER TABLE json2_inverted_index MODIFY COLUMN attrs JSON2 (
    nested."!__remainder__!" STRING INVERTED INDEX
);

SHOW CREATE TABLE json2_inverted_index;

DROP TABLE json2_inverted_index;

-- Reserved field names cannot occur anywhere in an indexed hint path.
CREATE TABLE json2_inverted_invalid (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 ("!__remainder__!" STRING INVERTED INDEX)
);

CREATE TABLE json2_inverted_invalid (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (nested."!__remainder__!" STRING INVERTED INDEX)
);

CREATE TABLE json2_inverted_invalid (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (nested."!__remainder__!".value STRING INVERTED INDEX)
);

-- Invalid index declarations and unsupported paths/types are rejected.
CREATE TABLE json2_inverted_invalid (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (message STRING INVERTED INDEX INVERTED INDEX)
);

CREATE TABLE json2_inverted_invalid (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (message STRING INVERTED)
);

CREATE TABLE json2_inverted_invalid (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (items[0].name STRING INVERTED INDEX)
);

CREATE TABLE json2_inverted_invalid (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (event_time TIMESTAMP INVERTED INDEX)
);

CREATE TABLE json2_inverted_invalid (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (message STRING SKIPPING INDEX)
);

CREATE TABLE json2_inverted_invalid (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (message STRING FULLTEXT INDEX)
);
