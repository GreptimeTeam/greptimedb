CREATE TABLE skip_wal_test (
    host STRING,
    cpu_util DOUBLE,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    TIME INDEX(ts)
);

-- Test setting skip_wal to true
ALTER TABLE skip_wal_test SET 'skip_wal'='true';

SHOW CREATE TABLE skip_wal_test;

-- Test setting skip_wal to false
ALTER TABLE skip_wal_test SET 'skip_wal'='false';

SHOW CREATE TABLE skip_wal_test;

-- Test unsetting skip_wal (should set to false)
ALTER TABLE skip_wal_test UNSET 'skip_wal';

SHOW CREATE TABLE skip_wal_test;

-- Test setting skip_wal to true again
ALTER TABLE skip_wal_test SET 'skip_wal'='true';

SHOW CREATE TABLE skip_wal_test;

-- SQLNESS ARG restart=true
-- Verify persistence after restart
SHOW CREATE TABLE skip_wal_test;

DROP TABLE skip_wal_test;

