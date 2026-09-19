CREATE TABLE phy (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE
) ENGINE = metric WITH ('physical_metric_table' = '', 'ttl' = '1s');

CREATE TABLE lg (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE
) ENGINE = metric WITH ('on_physical_table' = 'phy');

INSERT INTO lg VALUES (now(), 1);
ADMIN flush_table('phy');
ADMIN compact_table('phy');
SELECT count(*) FROM lg;
