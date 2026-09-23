CREATE TABLE function_routes (host STRING PRIMARY KEY, ts TIMESTAMP TIME INDEX, v INT)
PARTITION ON COLUMNS (host) (
  hash(substring(host, 1, 2)) < '8',
  hash(substring(host, 1, 2)) >= '8'
);
INSERT INTO function_routes VALUES ('abc', 1, 1), ('xyz', 2, 2), ('中🙂', 3, 3), (NULL, 4, 4);
ADMIN FLUSH_TABLE('function_routes');
