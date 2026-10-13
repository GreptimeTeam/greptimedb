CREATE TABLE spans (
    ts TIMESTAMP TIME INDEX,
    trace_id STRING SKIPPING INDEX,
    prompt STRING COMPRESSION WITH (type = 'zstd', level = 3, page_size = '8MiB'),
    reply STRING COMPRESSION WITH (level = 9),
    tokens BIGINT COMPRESSION WITH (page_size = '64KiB'),
) WITH (append_mode = 'true');

SHOW CREATE TABLE spans;

INSERT INTO spans VALUES
    (1, 't1', 'fix the flaky test', 'running it', 120),
    (2, 't1', 'fix the flaky test\nrunning it', 'patched', 340),
    (3, 't2', 'another prompt', NULL, NULL);

ADMIN flush_table('spans');

SELECT ts, trace_id, prompt, reply, tokens FROM spans ORDER BY ts;

DROP TABLE spans;

CREATE TABLE bad (ts TIMESTAMP TIME INDEX, s STRING COMPRESSION WITH (level = 0));

CREATE TABLE bad (ts TIMESTAMP TIME INDEX, s STRING COMPRESSION WITH (type = 'lz4'));

CREATE TABLE bad (ts TIMESTAMP TIME INDEX, s STRING COMPRESSION WITH (page_size = 'large'));

CREATE TABLE bad (ts TIMESTAMP TIME INDEX, s STRING COMPRESSION WITH (codec = 'zstd'));

CREATE TABLE bad (ts TIMESTAMP TIME INDEX, j JSON2 COMPRESSION WITH (level = 3));

CREATE TABLE bad (ts TIMESTAMP TIME INDEX, s STRING COMPRESSION WITH (level = [3]));

CREATE TABLE bad (ts TIMESTAMP TIME INDEX, s STRING COMPRESSION WITH (page_size = NULL));

CREATE TABLE bad (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY COMPRESSION WITH (level = 3)
) WITH ('sst_format' = 'primary_key', 'memtable.type' = 'time_series');

CREATE TABLE bad (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY COMPRESSION WITH (level = 3)
) WITH ('primary_key_encoding' = 'sparse');

CREATE TABLE compressed_tags (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY COMPRESSION WITH (level = 3, page_size = '64KiB'),
    val DOUBLE
) WITH ('sst_format' = 'flat', 'primary_key_encoding' = 'dense');

SHOW CREATE TABLE compressed_tags;

INSERT INTO compressed_tags VALUES (1, 'host-a', 1), (2, 'host-b', 2);

ADMIN flush_table('compressed_tags');

SELECT * FROM compressed_tags ORDER BY ts;

DROP TABLE compressed_tags;

CREATE TABLE compression_phy (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE COMPRESSION WITH (level = 3)
) ENGINE = metric WITH (physical_metric_table = '');

CREATE TABLE compression_conflict (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE COMPRESSION WITH (level = 9)
) ENGINE = metric WITH (on_physical_table = 'compression_phy');

CREATE TABLE compression_same (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE COMPRESSION WITH (level = 3)
) ENGINE = metric WITH (on_physical_table = 'compression_phy');

CREATE TABLE compression_new_tag (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY COMPRESSION WITH (level = 3),
    val DOUBLE
) ENGINE = metric WITH (on_physical_table = 'compression_phy');

CREATE TABLE compression_logical (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE
) ENGINE = metric WITH (on_physical_table = 'compression_phy');

INSERT INTO compression_logical VALUES (1, 42);

ADMIN flush_table('compression_phy');

SELECT * FROM compression_logical;

DROP TABLE compression_logical;

DROP TABLE compression_phy;
