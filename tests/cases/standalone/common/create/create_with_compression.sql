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
