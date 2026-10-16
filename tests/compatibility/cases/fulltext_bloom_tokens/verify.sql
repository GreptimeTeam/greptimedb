-- Only the SST written by the old binary.
SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg @@ 'world' ORDER BY ts;

SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg @@ 'error' ORDER BY ts;

SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg @@ '农业' ORDER BY ts;

SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg LIKE 'hello\_%' ORDER BY ts;

INSERT INTO t_fulltext_bloom_tokens VALUES
    (4, 'hello_world again'),
    (5, '农业 error');

ADMIN FLUSH_TABLE('t_fulltext_bloom_tokens');

-- Old and new SSTs: one bloom blob of each version.
SELECT meta_json LIKE '%"bloom_version":2%' AS is_v2, count(*) AS ssts
FROM information_schema.ssts_index_meta
WHERE table_id = (SELECT table_id FROM information_schema.tables WHERE table_name = 't_fulltext_bloom_tokens')
  AND index_type = 'fulltext_bloom'
GROUP BY is_v2
ORDER BY is_v2;

SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg @@ 'world' ORDER BY ts;

SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg @@ 'error' ORDER BY ts;

SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg @@ '农业' ORDER BY ts;

SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg LIKE 'hello\_%' ORDER BY ts;

-- Rebuilds the old SST's index with the current bloom version.
ADMIN BUILD_INDEX('t_fulltext_bloom_tokens');

-- SQLNESS SLEEP 2s
SELECT meta_json LIKE '%"bloom_version":2%' AS is_v2, count(*) AS ssts
FROM information_schema.ssts_index_meta
WHERE table_id = (SELECT table_id FROM information_schema.tables WHERE table_name = 't_fulltext_bloom_tokens')
  AND index_type = 'fulltext_bloom'
GROUP BY is_v2
ORDER BY is_v2;

SELECT ts, msg FROM t_fulltext_bloom_tokens WHERE msg @@ 'world' ORDER BY ts;

DROP TABLE t_fulltext_bloom_tokens;

-- Two SSTs written by the old binary.
SELECT meta_json LIKE '%"bloom_version":2%' AS is_v2, count(*) AS ssts
FROM information_schema.ssts_index_meta
WHERE table_id = (SELECT table_id FROM information_schema.tables WHERE table_name = 't_fulltext_bloom_compact')
  AND index_type = 'fulltext_bloom'
GROUP BY is_v2
ORDER BY is_v2;

SELECT ts, msg FROM t_fulltext_bloom_compact WHERE msg @@ 'world' ORDER BY ts;

-- Merges both SSTs into one with a current bloom blob.
ADMIN COMPACT_TABLE('t_fulltext_bloom_compact', 'strict_window', 'window=3600');

SELECT meta_json LIKE '%"bloom_version":2%' AS is_v2, count(*) AS ssts
FROM information_schema.ssts_index_meta
WHERE table_id = (SELECT table_id FROM information_schema.tables WHERE table_name = 't_fulltext_bloom_compact')
  AND index_type = 'fulltext_bloom'
GROUP BY is_v2
ORDER BY is_v2;

SELECT ts, msg FROM t_fulltext_bloom_compact WHERE msg @@ 'world' ORDER BY ts;

SELECT ts, msg FROM t_fulltext_bloom_compact WHERE msg @@ '农业' ORDER BY ts;

DROP TABLE t_fulltext_bloom_compact;
