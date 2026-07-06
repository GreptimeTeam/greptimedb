CREATE TABLE ssts_limit_old_manifest_table (
  a INT PRIMARY KEY,
  b STRING,
  ts TIMESTAMP TIME INDEX
)
PARTITION ON COLUMNS (a) (
  a < 1000,
  a >= 1000 AND a < 2000,
  a >= 2000
);

INSERT INTO ssts_limit_old_manifest_table VALUES
  (500, 'a', '2026-07-06 00:00:00+0000'),
  (1500, 'b', '2026-07-06 00:01:00+0000'),
  (2500, 'c', '2026-07-06 00:02:00+0000');

ADMIN FLUSH_TABLE('ssts_limit_old_manifest_table');
