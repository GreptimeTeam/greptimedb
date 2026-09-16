CREATE TABLE system_metrics (
    host STRING,
    idc STRING,
    cpu_util DOUBLE,
    memory_util DOUBLE,
    disk_util DOUBLE,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY(host, idc),
    TIME INDEX(ts)
) WITH (skip_wal = "true");

INSERT INTO system_metrics
VALUES
    ("host1", "idc_a", 11.8, 10.3, 10.3, 1667446797450),
    ("host2", "idc_a", 80.0, 70.3, 90.0, 1667446797450),
    ("host1", "idc_b", 50.0, 66.7, 40.6, 1667446797450);

-- SQLNESS ARG restart=true
SELECT * FROM system_metrics;

INSERT INTO system_metrics
VALUES
    ("host1", "idc_a", 11.8, 10.3, 10.3, 1667446797450),
    ("host2", "idc_a", 80.0, 70.3, 90.0, 1667446797450),
    ("host1", "idc_b", 50.0, 66.7, 40.6, 1667446797450);

ADMIN flush_table('system_metrics');

-- SQLNESS ARG restart=true
SELECT * FROM system_metrics;

-- A table created without a real WAL provider cannot enable WAL later.
ALTER TABLE system_metrics SET 'skip_wal' = 'false';

DROP TABLE system_metrics;

CREATE TABLE alter_skip_wal (
    host STRING,
    val DOUBLE,
    ts TIMESTAMP,
    PRIMARY KEY(host),
    TIME INDEX(ts)
);

INSERT INTO alter_skip_wal VALUES ('host1', 1, 1000);

ALTER TABLE alter_skip_wal SET 'skip_wal' = 'true';

SHOW CREATE TABLE alter_skip_wal;

-- This row is intentionally not written to WAL.
INSERT INTO alter_skip_wal VALUES ('host2', 2, 2000);

ALTER TABLE alter_skip_wal SET 'skip_wal' = 'false';

SHOW CREATE TABLE alter_skip_wal;

-- Repeating the transition is idempotent.
ALTER TABLE alter_skip_wal SET 'skip_wal' = 'false';

ALTER TABLE alter_skip_wal UNSET 'skip_wal';

-- This row uses the restored WAL provider.
INSERT INTO alter_skip_wal VALUES ('host3', 3, 3000);

SELECT * FROM alter_skip_wal ORDER BY ts;

-- Re-enabling WAL does not retroactively write the skipped row to WAL.
-- SQLNESS ARG restart=true
SHOW CREATE TABLE alter_skip_wal;

SELECT * FROM alter_skip_wal ORDER BY ts;

DROP TABLE alter_skip_wal;
