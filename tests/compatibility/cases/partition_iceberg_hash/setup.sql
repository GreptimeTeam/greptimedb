CREATE TABLE iceberg_hash_routes (
  host STRING,
  tenant BIGINT UNSIGNED,
  ts TIMESTAMP TIME INDEX,
  v INT,
  PRIMARY KEY (host, tenant)
)
PARTITION ON COLUMNS (host, tenant) (
  iceberg_hash(host) < 0 AND tenant < 100,
  iceberg_hash(host) < 0 AND tenant >= 100,
  iceberg_hash(host) >= 0 AND iceberg_hash(tenant) < 0,
  iceberg_hash(host) >= 0 AND iceberg_hash(tenant) >= 0
);

INSERT INTO iceberg_hash_routes VALUES
  ('a', 1, 1, 1),
  ('b', 100, 2, 2),
  ('server42', 18446744073709551615, 3, 3),
  ('中🙂', 0, 4, 4),
  (NULL, 7, 5, 5);

ADMIN FLUSH_TABLE('iceberg_hash_routes');
