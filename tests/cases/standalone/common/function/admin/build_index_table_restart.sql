CREATE TABLE build_index_restart_test (
    ts TIMESTAMP TIME INDEX,
    msg TEXT,
);

INSERT INTO build_index_restart_test VALUES 
(1,"The quick brown fox jumps over the lazy dog"), 
(2,"The quick brown fox jumps over the lazy cat"), 
(3,"The quick brown fox jumps over the lazy mouse"), 
(4,"The quick brown fox jumps over the lazy rabbit"), 
(5,"The quick brown fox jumps over the lazy turtle");

ADMIN FLUSH_TABLE('build_index_restart_test');

-- SQLNESS SLEEP 1s
-- No fulltext index yet
SHOW INDEX FROM build_index_restart_test;

-- No physical fulltext index metadata yet.
-- ssts_index_meta.index_type uses physical backend names (fulltext_bloom/fulltext_tantivy),
-- unlike SHOW INDEX's greptime-fulltext-index-* display value.
-- One flushed SST with one fulltext column should produce exactly one fulltext metadata entry.
SELECT COUNT(*) AS fulltext_index_meta_count
FROM information_schema.ssts_index_meta
WHERE table_id = (
    SELECT table_id
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'build_index_restart_test'
)
AND index_type LIKE 'fulltext%';

ALTER TABLE build_index_restart_test MODIFY COLUMN msg SET FULLTEXT INDEX;

ADMIN BUILD_INDEX('build_index_restart_test');

-- SQLNESS SLEEP 1s
-- Fulltext index built, verify via fulltext query
SELECT msg FROM build_index_restart_test WHERE MATCHES(msg, 'fox') ORDER BY ts;

-- Verify index metadata
SHOW INDEX FROM build_index_restart_test;

-- Verify physical fulltext index metadata before restart
SELECT COUNT(*) AS fulltext_index_meta_count
FROM information_schema.ssts_index_meta
WHERE table_id = (
    SELECT table_id
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'build_index_restart_test'
)
AND index_type LIKE 'fulltext%';

-- SQLNESS ARG restart=true
-- After restart, verify fulltext query still works
SELECT msg FROM build_index_restart_test WHERE MATCHES(msg, 'fox') ORDER BY ts;

-- Verify index metadata persists
SHOW INDEX FROM build_index_restart_test;

-- Verify physical fulltext index metadata persists
SELECT COUNT(*) AS fulltext_index_meta_count
FROM information_schema.ssts_index_meta
WHERE table_id = (
    SELECT table_id
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'build_index_restart_test'
)
AND index_type LIKE 'fulltext%';

-- Run ADMIN BUILD_INDEX again (idempotency)
ADMIN BUILD_INDEX('build_index_restart_test');

-- SQLNESS SLEEP 1s
-- Fulltext query still works after idempotent rebuild
SELECT msg FROM build_index_restart_test WHERE MATCHES(msg, 'fox') ORDER BY ts;

-- Verify index metadata still present
SHOW INDEX FROM build_index_restart_test;

-- Verify idempotent rebuild did not duplicate physical fulltext index metadata
SELECT COUNT(*) AS fulltext_index_meta_count
FROM information_schema.ssts_index_meta
WHERE table_id = (
    SELECT table_id
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'build_index_restart_test'
)
AND index_type LIKE 'fulltext%';

DROP TABLE build_index_restart_test;
