CREATE TABLE hash_routes (
  host STRING,
  tenant BIGINT UNSIGNED,
  ts TIMESTAMP TIME INDEX,
  v INT,
  PRIMARY KEY (host, tenant)
)
PARTITION ON COLUMNS (host, tenant) (
  iceberg_hash(host) < 0 AND tenant < 100,
  iceberg_hash(host) < 0 AND tenant >= 100,
  ICEBERG_HASH(host) >= 0 AND iceberg_hash(tenant) < 0,
  0 <= iceberg_hash(host) AND iceberg_hash(tenant) >= 0
);

SHOW CREATE TABLE hash_routes;

SELECT partition_name, partition_description FROM information_schema.partitions WHERE table_name = 'hash_routes' ORDER BY partition_name;

INSERT INTO hash_routes VALUES
  ('a', 1, 1, 1),
  ('b', 100, 2, 2),
  ('server42', 18446744073709551615, 3, 3),
  ('中🙂', 0, 4, 4),
  ('', 7, 5, 5),
  (NULL, 7, 6, 6),
  ('a', NULL, 7, 7);

SELECT * FROM hash_routes ORDER BY ts;

-- A filter on the hashed column must not prune the region holding the row.
SELECT * FROM hash_routes WHERE host = 'server42';

SELECT * FROM hash_routes WHERE host IS NULL;

DELETE FROM hash_routes WHERE host = 'a';

SELECT * FROM hash_routes ORDER BY ts;

DROP TABLE hash_routes;

CREATE TABLE hash_float (f DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY (f))
PARTITION ON COLUMNS (f) (
  iceberg_hash(f) < 0,
  iceberg_hash(f) >= 0
);

CREATE TABLE hash_two_args (a STRING, b STRING, ts TIMESTAMP TIME INDEX, PRIMARY KEY (a, b))
PARTITION ON COLUMNS (a, b) (
  iceberg_hash(a, b) < 0,
  iceberg_hash(a, b) >= 0
);

CREATE TABLE hash_gap (a STRING, ts TIMESTAMP TIME INDEX, PRIMARY KEY (a))
PARTITION ON COLUMNS (a) (
  iceberg_hash(a) < 0,
  iceberg_hash(a) > 0
);

CREATE TABLE quoted_hash (
  "host-name" STRING,
  uid BIGINT,
  ts TIMESTAMP TIME INDEX,
  PRIMARY KEY ("host-name")
)
PARTITION ON COLUMNS ("host-name", uid) (
  iceberg_hash("host-name") < 0,
  iceberg_hash("host-name") >= 0 AND iceberg_hash(uid) < 0,
  iceberg_hash("host-name") >= 0 AND iceberg_hash(uid) >= 0
);

SHOW CREATE TABLE quoted_hash;

-- A partition column keeps its type, even when nullable and outside the primary key.
ALTER TABLE quoted_hash MODIFY COLUMN uid DOUBLE;

ALTER TABLE quoted_hash MODIFY COLUMN uid STRING;

DROP TABLE quoted_hash;
