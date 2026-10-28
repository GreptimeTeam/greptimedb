create table json2_without_append_mode (
    ts timestamp time index,
    j json2
);

drop table json2_without_append_mode;

create table json2_append_mode_false (
    ts timestamp time index,
    j json2
) with (
    'append_mode' = 'false'
);

drop table json2_append_mode_false;

create table json2_alter_non_append (
    ts timestamp time index
);

insert into json2_alter_non_append values (1);

alter table json2_alter_non_append add column j json2;

insert into json2_alter_non_append values (1, '{"a":1}');

select ts, j.a from json2_alter_non_append;

drop table json2_alter_non_append;

-- JSON2 is merged as a whole field, including empty objects and explicit JSON nulls.
CREATE TABLE json2_last_row (
    host STRING,
    ts TIMESTAMP TIME INDEX,
    j JSON2,
    k JSON2(max_auto_expanded_paths = 0),
    f DOUBLE,
    PRIMARY KEY (host)
) WITH (
    'append_mode' = 'false',
    'merge_mode' = 'last_row'
);

INSERT INTO json2_last_row VALUES
    ('one', 1, '{"a":1}', '{"a":1}', 10),
    ('one', 2, '{"a":2}', '{"a":2}', 10),
    ('one', 3, '{"a":3}', '{"a":3}', 10),
    ('one', 4, '{"a":4}', '{"a":4}', 10),
    ('one', 5, '{"a":{"x":5}}', '{"a":{"x":5}}', 10),
    ('one', 6, '{"a":{"x":6}}', '{"a":{"x":6}}', 10),
    ('one', 7, '{"a":{"x":8}}', '{"a":{"x":8}}', 10),
    ('two', 1, '{"a":99}', '{"a":99}', 99);

ADMIN FLUSH_TABLE('json2_last_row');

-- New objects replace old objects; only SQL NULL can fall back in last_non_null.
INSERT INTO json2_last_row VALUES
    ('one', 1, '{"b":2}', '{"b":2}', NULL),
    ('one', 2, '{}', '{}', NULL),
    ('one', 3, '{"a":null}', '{"a":null}', NULL),
    ('one', 4, NULL, NULL, NULL),
    ('one', 5, '{"a":42}', '{"a":42}', NULL),
    ('one', 6, '{"a":{"x":7}}', '{"a":{"x":7}}', NULL),
    ('one', 7, NULL, NULL, NULL);

-- Path-only reads must keep the new nested value and distinguish SQL NULL fallback.
SELECT host, ts, j, j.a AS a, j.a.x AS ax, k, k.a AS ka, f
FROM json2_last_row ORDER BY host, ts;
SELECT host, ts, j.a.x AS ax, k.a.x AS kax FROM json2_last_row ORDER BY host, ts;
SELECT host, ts FROM json2_last_row WHERE j.a = '1' ORDER BY host, ts;

ADMIN FLUSH_TABLE('json2_last_row');

SELECT host, ts, j, j.a AS a, j.a.x AS ax, k, k.a AS ka, f
FROM json2_last_row ORDER BY host, ts;
SELECT host, ts, j.a.x AS ax, k.a.x AS kax FROM json2_last_row ORDER BY host, ts;

ADMIN COMPACT_TABLE('json2_last_row');

SELECT host, ts, j, j.a AS a, j.a.x AS ax, k, k.a AS ka, f
FROM json2_last_row ORDER BY host, ts;
SELECT host, ts, j.a.x AS ax, k.a.x AS kax FROM json2_last_row ORDER BY host, ts;

-- SQLNESS ARG restart=true
SELECT host, ts, j, j.a AS a, j.a.x AS ax, k, k.a AS ka, f
FROM json2_last_row ORDER BY host, ts;
SELECT host, ts, j.a.x AS ax, k.a.x AS kax FROM json2_last_row ORDER BY host, ts;

-- A delete must prevent last_non_null from filling fields from older versions.
DELETE FROM json2_last_row WHERE host = 'one' AND ts = 1;
INSERT INTO json2_last_row VALUES ('one', 1, '{"c":3}', '{"c":3}', NULL);

SELECT host, ts, j, j.a AS a, j.a.x AS ax, k, k.a AS ka, f
FROM json2_last_row ORDER BY host, ts;

ADMIN FLUSH_TABLE('json2_last_row');
ADMIN COMPACT_TABLE('json2_last_row');

SELECT host, ts, j, j.a AS a, j.a.x AS ax, k, k.a AS ka, f
FROM json2_last_row ORDER BY host, ts;

DELETE FROM json2_last_row WHERE j.a = '99';
SELECT COUNT(*) FROM json2_last_row WHERE host = 'two';

DROP TABLE json2_last_row;

-- JSON2 is merged as a whole field, including empty objects and explicit JSON nulls.
CREATE TABLE json2_last_non_null (
    host STRING,
    ts TIMESTAMP TIME INDEX,
    j JSON2,
    k JSON2(max_auto_expanded_paths = 0),
    f DOUBLE,
    PRIMARY KEY (host)
) WITH (
    'append_mode' = 'false',
    'merge_mode' = 'last_non_null'
);

INSERT INTO json2_last_non_null VALUES
    ('one', 1, '{"a":1}', '{"a":1}', 10),
    ('one', 2, '{"a":2}', '{"a":2}', 10),
    ('one', 3, '{"a":3}', '{"a":3}', 10),
    ('one', 4, '{"a":4}', '{"a":4}', 10),
    ('one', 5, '{"a":{"x":5}}', '{"a":{"x":5}}', 10),
    ('one', 6, '{"a":{"x":6}}', '{"a":{"x":6}}', 10),
    ('one', 7, '{"a":{"x":8}}', '{"a":{"x":8}}', 10),
    ('two', 1, '{"a":99}', '{"a":99}', 99);

ADMIN FLUSH_TABLE('json2_last_non_null');

-- New objects replace old objects; only SQL NULL can fall back in last_non_null.
INSERT INTO json2_last_non_null VALUES
    ('one', 1, '{"b":2}', '{"b":2}', NULL),
    ('one', 2, '{}', '{}', NULL),
    ('one', 3, '{"a":null}', '{"a":null}', NULL),
    ('one', 4, NULL, NULL, NULL),
    ('one', 5, '{"a":42}', '{"a":42}', NULL),
    ('one', 6, '{"a":{"x":7}}', '{"a":{"x":7}}', NULL),
    ('one', 7, NULL, NULL, NULL);

-- Path-only reads must keep the new nested value and distinguish SQL NULL fallback.
SELECT host, ts, j, j.a AS a, j.a.x AS ax, k, k.a AS ka, f
FROM json2_last_non_null ORDER BY host, ts;
SELECT host, ts, j.a.x AS ax, k.a.x AS kax FROM json2_last_non_null ORDER BY host, ts;
SELECT host, ts FROM json2_last_non_null WHERE j.a = '1' ORDER BY host, ts;

ADMIN FLUSH_TABLE('json2_last_non_null');

SELECT host, ts, j, j.a AS a, j.a.x AS ax, k, k.a AS ka, f
FROM json2_last_non_null ORDER BY host, ts;
SELECT host, ts, j.a.x AS ax, k.a.x AS kax FROM json2_last_non_null ORDER BY host, ts;

ADMIN COMPACT_TABLE('json2_last_non_null');

SELECT host, ts, j, j.a AS a, j.a.x AS ax, k, k.a AS ka, f
FROM json2_last_non_null ORDER BY host, ts;
SELECT host, ts, j.a.x AS ax, k.a.x AS kax FROM json2_last_non_null ORDER BY host, ts;

-- SQLNESS ARG restart=true
SELECT host, ts, j, j.a AS a, j.a.x AS ax, k, k.a AS ka, f
FROM json2_last_non_null ORDER BY host, ts;
SELECT host, ts, j.a.x AS ax, k.a.x AS kax FROM json2_last_non_null ORDER BY host, ts;

-- A delete must prevent last_non_null from filling fields from older versions.
DELETE FROM json2_last_non_null WHERE host = 'one' AND ts = 1;
INSERT INTO json2_last_non_null VALUES ('one', 1, '{"c":3}', '{"c":3}', NULL);

SELECT host, ts, j, j.a AS a, j.a.x AS ax, k, k.a AS ka, f
FROM json2_last_non_null ORDER BY host, ts;

ADMIN FLUSH_TABLE('json2_last_non_null');
ADMIN COMPACT_TABLE('json2_last_non_null');

SELECT host, ts, j, j.a AS a, j.a.x AS ax, k, k.a AS ka, f
FROM json2_last_non_null ORDER BY host, ts;

DELETE FROM json2_last_non_null WHERE j.a = '99';
SELECT COUNT(*) FROM json2_last_non_null WHERE host = 'two';

DROP TABLE json2_last_non_null;

-- A predicate on an added JSON2 field must retain older values of other fields.
CREATE TABLE json2_alter_backfill (
    host STRING,
    ts TIMESTAMP TIME INDEX,
    f DOUBLE,
    PRIMARY KEY (host)
) WITH ('merge_mode' = 'last_non_null');
INSERT INTO json2_alter_backfill VALUES ('one', 1, 10);
ADMIN FLUSH_TABLE('json2_alter_backfill');
ALTER TABLE json2_alter_backfill ADD COLUMN j JSON2;
INSERT INTO json2_alter_backfill VALUES ('one', 1, NULL, '{"a":1}');
SELECT host, ts, j, f FROM json2_alter_backfill WHERE j IS NOT NULL;
SELECT host, ts, j, f FROM json2_alter_backfill WHERE j.a = '1';
ADMIN FLUSH_TABLE('json2_alter_backfill');
SELECT host, ts, j, f FROM json2_alter_backfill WHERE j IS NOT NULL;
ADMIN COMPACT_TABLE('json2_alter_backfill');
-- SQLNESS ARG restart=true
SELECT host, ts, j, f FROM json2_alter_backfill WHERE j IS NOT NULL;
DROP TABLE json2_alter_backfill;
