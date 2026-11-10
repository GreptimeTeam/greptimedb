-- Per-column SST compression overrides via ALTER TABLE.
-- MODIFY COLUMN ... COMPRESSION WITH (...) is a merge-on-set delta over the
-- column's current options; MODIFY COLUMN ... UNSET COMPRESSION clears them.
CREATE TABLE alter_compression (
    ts TIMESTAMP TIME INDEX,
    host STRING,
    payload STRING,
    tokens BIGINT,
    j JSON2
);

INSERT INTO alter_compression (ts, host, payload, tokens) VALUES
    (1, 'a', 'first', 100),
    (2, 'b', 'second', 200);

ADMIN FLUSH_TABLE('alter_compression');

-- No override yet: the column is written with the SST defaults.
SHOW CREATE TABLE alter_compression;

ALTER TABLE alter_compression MODIFY COLUMN payload COMPRESSION WITH (type = 'zstd', level = '14');

SHOW CREATE TABLE alter_compression;

-- The delta merges into the current options: level is retained, page_size is added.
ALTER TABLE alter_compression MODIFY COLUMN payload COMPRESSION WITH (page_size = '16KiB');

SHOW CREATE TABLE alter_compression;

INSERT INTO alter_compression (ts, host, payload, tokens) VALUES
    (3, 'c', 'third', 300),
    (4, 'd', 'fourth', 400);

ADMIN FLUSH_TABLE('alter_compression');

SELECT ts, host, payload, tokens FROM alter_compression ORDER BY ts;

-- Data and options written before the restart survive it.
-- SQLNESS ARG restart=true
SHOW CREATE TABLE alter_compression;

SELECT ts, host, payload, tokens FROM alter_compression ORDER BY ts;

-- Encoding is a merge-on-set key as well.
ALTER TABLE alter_compression MODIFY COLUMN tokens COMPRESSION WITH (encoding = 'delta_binary_packed');

SHOW CREATE TABLE alter_compression;

-- UNSET drops the whole override, so the column falls back to the SST defaults.
ALTER TABLE alter_compression MODIFY COLUMN payload UNSET COMPRESSION;

ALTER TABLE alter_compression MODIFY COLUMN tokens UNSET COMPRESSION;

SHOW CREATE TABLE alter_compression;

-- After UNSET, a new delta starts from the SST defaults instead of the old options.
ALTER TABLE alter_compression MODIFY COLUMN payload COMPRESSION WITH (page_size = '32KiB');

SHOW CREATE TABLE alter_compression;

-- Invalid options are rejected and leave the column options untouched.
ALTER TABLE alter_compression MODIFY COLUMN payload COMPRESSION WITH (level = '0');
ALTER TABLE alter_compression MODIFY COLUMN payload COMPRESSION WITH (level = '23');
ALTER TABLE alter_compression MODIFY COLUMN payload COMPRESSION WITH (type = 'lz4');
ALTER TABLE alter_compression MODIFY COLUMN payload COMPRESSION WITH (page_size = 'large');
ALTER TABLE alter_compression MODIFY COLUMN payload COMPRESSION WITH (page_size = '0');
ALTER TABLE alter_compression MODIFY COLUMN payload COMPRESSION WITH (codec = 'zstd');
ALTER TABLE alter_compression MODIFY COLUMN payload COMPRESSION WITH ();
ALTER TABLE alter_compression MODIFY COLUMN missing COMPRESSION WITH (level = '3');
-- Nested columns are rejected: their values live in child columns.
ALTER TABLE alter_compression MODIFY COLUMN j COMPRESSION WITH (level = '3');
-- ADD COLUMN has no compression support.
ALTER TABLE alter_compression ADD COLUMN extra STRING COMPRESSION WITH (level = '3');

SHOW CREATE TABLE alter_compression;

-- Tag columns need flat SST format and dense primary key encoding: the
-- primary-key SST format packs tags into __primary_key, so they cannot carry
-- their own compression options.
CREATE TABLE alter_compression_tags (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE
) WITH ('sst_format' = 'primary_key');

INSERT INTO alter_compression_tags VALUES (1, 'host-a', 1), (2, 'host-b', 2);

ADMIN FLUSH_TABLE('alter_compression_tags');

ALTER TABLE alter_compression_tags MODIFY COLUMN host COMPRESSION WITH (level = '3');

-- A rejected alter leaves the table untouched.
SHOW CREATE TABLE alter_compression_tags;

CREATE TABLE alter_compression_tags_dense (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE
) WITH ('sst_format' = 'flat', 'primary_key_encoding' = 'dense');

INSERT INTO alter_compression_tags_dense VALUES (1, 'host-a', 1), (2, 'host-b', 2);

ADMIN FLUSH_TABLE('alter_compression_tags_dense');

ALTER TABLE alter_compression_tags_dense MODIFY COLUMN host COMPRESSION WITH (level = '3', page_size = '64KiB');

SHOW CREATE TABLE alter_compression_tags_dense;

INSERT INTO alter_compression_tags_dense VALUES (3, 'host-c', 3);

ADMIN FLUSH_TABLE('alter_compression_tags_dense');

SELECT * FROM alter_compression_tags_dense ORDER BY ts;

-- Compression is a physical-table concern for the metric engine.
CREATE TABLE alter_compression_phy (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE COMPRESSION WITH (level = '3')
) ENGINE = metric WITH (physical_metric_table = '');

ALTER TABLE alter_compression_phy MODIFY COLUMN val COMPRESSION WITH (level = '9');

SHOW CREATE TABLE alter_compression_phy;

CREATE TABLE alter_compression_logical (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE
) ENGINE = metric WITH (on_physical_table = 'alter_compression_phy');

INSERT INTO alter_compression_logical VALUES (1, 42);

ADMIN FLUSH_TABLE('alter_compression_phy');

ALTER TABLE alter_compression_logical MODIFY COLUMN val COMPRESSION WITH (level = '9');

SELECT * FROM alter_compression_logical;

DROP TABLE alter_compression_logical;

DROP TABLE alter_compression_phy;

DROP TABLE alter_compression_tags_dense;

DROP TABLE alter_compression_tags;

DROP TABLE alter_compression;
