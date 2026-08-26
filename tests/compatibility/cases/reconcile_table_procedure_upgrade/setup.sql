CREATE TABLE reconcile_upgrade_target (
    host STRING PRIMARY KEY,
    value INT,
    ts TIMESTAMP TIME INDEX
);

INSERT INTO reconcile_upgrade_target VALUES
    ('a', 1, '2026-01-01 00:00:00'),
    ('z', 2, '2026-01-01 00:01:00');

-- The compatibility runner captures the old binary's persisted procedure step
-- and clones it under a fresh ID for the current binary to recover.
ADMIN RECONCILE_TABLE('reconcile_upgrade_target');
