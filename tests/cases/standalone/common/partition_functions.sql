CREATE TABLE prefix_partition (host STRING PRIMARY KEY, ts TIMESTAMP TIME INDEX, v INT)
PARTITION ON COLUMNS (host) (
  substring(host, 1, 1) < 'm',
  substring(host, 1, 1) >= 'm'
);

SHOW CREATE TABLE prefix_partition;

INSERT INTO prefix_partition VALUES ('abc', 1, 1), ('xyz', 2, 2), ('中🙂', 3, 3), (NULL, 4, 4), ('', 5, 5);

SELECT * FROM prefix_partition ORDER BY ts;
SELECT * FROM prefix_partition WHERE host = 'abc';
SELECT * FROM prefix_partition WHERE host >= 'm' ORDER BY ts;
SELECT * FROM prefix_partition WHERE host IS NULL;
ADMIN FLUSH_TABLE('prefix_partition');
SELECT * FROM prefix_partition ORDER BY ts;

DELETE FROM prefix_partition WHERE host = 'abc';
SELECT * FROM prefix_partition ORDER BY ts;

DROP TABLE prefix_partition;

CREATE TABLE hash_partition (host STRING, idc STRING, ts TIMESTAMP TIME INDEX, v INT, PRIMARY KEY(host, idc))
PARTITION ON COLUMNS (host, idc) (
  hash(host, idc) < '8',
  hash(host, idc) >= '8'
);

SHOW CREATE TABLE hash_partition;

INSERT INTO hash_partition VALUES ('a', 'bc', 1, 1), ('ab', 'c', 2, 2), ('中', '🙂', 3, 3), (NULL, 'dc', 4, 4), ('', '', 5, 5);
SELECT * FROM hash_partition ORDER BY ts;
SELECT * FROM hash_partition WHERE host = 'a';
SELECT * FROM hash_partition WHERE host IS NULL;
ADMIN FLUSH_TABLE('hash_partition');
SELECT * FROM hash_partition ORDER BY ts;
DELETE FROM hash_partition WHERE host = 'ab';
SELECT * FROM hash_partition ORDER BY ts;
DROP TABLE hash_partition;

CREATE TABLE nested_partition (host STRING PRIMARY KEY, ts TIMESTAMP TIME INDEX)
PARTITION ON COLUMNS (host) (
  substring(hash(substring(host, 1)), 1, 1) < '8',
  substring(hash(substring(host, 1)), 1, 1) >= '8'
);
INSERT INTO nested_partition VALUES ('abc', 1), ('xyz', 2), (NULL, 3);
SELECT * FROM nested_partition ORDER BY ts;
SHOW CREATE TABLE nested_partition;
DROP TABLE nested_partition;

CREATE TABLE invalid_function (host STRING, ts TIMESTAMP TIME INDEX)
PARTITION ON COLUMNS (host) (hash(host) < '8', hash(host) > '8');

CREATE TABLE invalid_function (host STRING, ts TIMESTAMP TIME INDEX)
PARTITION ON COLUMNS (host) (substring(host, 1, -1) < 'm', substring(host, 1, -1) >= 'm');

CREATE TABLE invalid_function (host STRING, ts TIMESTAMP TIME INDEX)
PARTITION ON COLUMNS (host) (hash(ts) < '8', hash(ts) >= '8');

CREATE TABLE invalid_function (host STRING, ts TIMESTAMP TIME INDEX)
PARTITION ON COLUMNS (host) (hash(host) < '9', hash(host) >= '8');

CREATE TABLE dynamic_substring (host STRING, length BIGINT, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host, length))
PARTITION ON COLUMNS (host, length) (
  substring(host, 1, length) < NULL OR substring(host, 1, length) >= NULL
);

INSERT INTO dynamic_substring VALUES ('abc', -1, 1);
INSERT INTO dynamic_substring VALUES ('abc', 2, 2);
SELECT * FROM dynamic_substring ORDER BY ts;
DROP TABLE dynamic_substring;

CREATE TABLE guarded_substring (host STRING, length BIGINT, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host, length))
PARTITION ON COLUMNS (host, length) (
  host < 'm' AND substring(host, 1, length) < 'm',
  host < 'm' AND substring(host, 1, length) >= 'm',
  host >= 'm'
);

INSERT INTO guarded_substring VALUES ('z', -1, 1);
CREATE TABLE substring_source (host STRING, length BIGINT, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host, length));
INSERT INTO substring_source VALUES ('a', 1, 2), ('z', -1, 3), (NULL, -1, 4);
INSERT INTO guarded_substring SELECT * FROM substring_source;
INSERT INTO guarded_substring VALUES ('a', -1, 5);
SELECT * FROM guarded_substring ORDER BY ts;
DROP TABLE substring_source;
DROP TABLE guarded_substring;

CREATE TABLE integer_hash_routes (tenant_id BIGINT, device_id BIGINT UNSIGNED, host STRING, ts TIMESTAMP TIME INDEX, v INT, PRIMARY KEY(tenant_id, device_id, host))
PARTITION ON COLUMNS (tenant_id, device_id, host) (
  hash(tenant_id, device_id, host) < '8',
  hash(tenant_id, device_id, host) >= '8'
);
INSERT INTO integer_hash_routes VALUES (42, 42, 'a', 1, 1), (-1, 18446744073709551615, 'b', 2, 2), (-9223372036854775808, 0, '中', 3, 3), (NULL, 42, 'a', 4, 4), (42, NULL, 'a', 5, 5), (42, 42, NULL, 6, 6);
ADMIN FLUSH_TABLE('integer_hash_routes');

SHOW CREATE TABLE integer_hash_routes;
SELECT * FROM integer_hash_routes ORDER BY ts;
INSERT INTO integer_hash_routes VALUES (42, 42, 'a', 1, 10), (-1, 18446744073709551615, 'b', 2, 20), (-9223372036854775808, 0, '中', 3, 30), (NULL, 42, 'a', 4, 40), (42, NULL, 'a', 5, 50), (42, 42, NULL, 6, 60);
SELECT * FROM integer_hash_routes ORDER BY ts;
DELETE FROM integer_hash_routes WHERE tenant_id = -1;
SELECT * FROM integer_hash_routes ORDER BY ts;
DROP TABLE integer_hash_routes;
