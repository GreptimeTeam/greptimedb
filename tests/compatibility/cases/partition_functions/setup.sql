CREATE TABLE function_routes (host STRING PRIMARY KEY, ts TIMESTAMP TIME INDEX, v INT)
PARTITION ON COLUMNS (host) (
  hash(substring(host, 1, 2)) < '8',
  hash(substring(host, 1, 2)) >= '8'
);
INSERT INTO function_routes VALUES ('abc', 1, 1), ('xyz', 2, 2), ('中🙂', 3, 3), (NULL, 4, 4);
ADMIN FLUSH_TABLE('function_routes');

CREATE TABLE integer_hash_routes (tenant_id BIGINT, device_id BIGINT UNSIGNED, host STRING, ts TIMESTAMP TIME INDEX, v INT, PRIMARY KEY(tenant_id, device_id, host))
PARTITION ON COLUMNS (tenant_id, device_id, host) (
  hash(tenant_id, device_id, host) < '8',
  hash(tenant_id, device_id, host) >= '8'
);
INSERT INTO integer_hash_routes VALUES (42, 42, 'a', 1, 1), (-1, 18446744073709551615, 'b', 2, 2), (-9223372036854775808, 0, '中', 3, 3), (NULL, 42, 'a', 4, 4), (42, NULL, 'a', 5, 5), (42, 42, NULL, 6, 6);
ADMIN FLUSH_TABLE('integer_hash_routes');
