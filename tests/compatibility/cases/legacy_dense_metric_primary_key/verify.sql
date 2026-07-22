SHOW CREATE TABLE legacy_dense_phy;

DESC TABLE legacy_dense_phy;

SELECT host, ts, val FROM legacy_dense_logical ORDER BY ts;

INSERT INTO legacy_dense_logical (ts, val, host) VALUES ('2026-01-01 00:02:00', 3.0, 'new-c'), ('2026-01-01 00:03:00', 4.0, 'new-d');

ADMIN FLUSH_TABLE('legacy_dense_phy');

SELECT host, ts, val FROM legacy_dense_logical ORDER BY ts;

-- SQLNESS ARG restart=true
SELECT host, ts, val FROM legacy_dense_logical ORDER BY ts;

DESC TABLE legacy_dense_phy;
