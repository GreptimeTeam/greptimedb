-- Only the SST written by the old binary.
SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg @@ 'world' ORDER BY ts;

SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg @@ 'error' ORDER BY ts;

SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg @@ '农业' ORDER BY ts;

SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg LIKE 'hello\_%' ORDER BY ts;

INSERT INTO t_fulltext_bloom_tokens VALUES
    (4, 'hello_world again'),
    (5, '农业 error');

ADMIN FLUSH_TABLE('t_fulltext_bloom_tokens');

-- Old and new SSTs.
SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg @@ 'world' ORDER BY ts;

SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg @@ 'error' ORDER BY ts;

SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg @@ '农业' ORDER BY ts;

SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg LIKE 'hello\_%' ORDER BY ts;

ADMIN COMPACT_TABLE('t_fulltext_bloom_tokens');

-- After compaction.
SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg @@ 'world' ORDER BY ts;

SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg @@ '农业' ORDER BY ts;

DROP TABLE t_fulltext_bloom_tokens;
