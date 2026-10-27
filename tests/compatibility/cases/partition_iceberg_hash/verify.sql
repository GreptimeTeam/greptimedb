SHOW CREATE TABLE iceberg_hash_routes;

SELECT * FROM iceberg_hash_routes ORDER BY ts;

-- Overwrites must route to the regions holding the flushed rows.
INSERT INTO iceberg_hash_routes VALUES
  ('a', 1, 1, 10),
  ('b', 100, 2, 20),
  ('server42', 18446744073709551615, 3, 30),
  ('中🙂', 0, 4, 40),
  (NULL, 7, 5, 50);

SELECT * FROM iceberg_hash_routes ORDER BY ts;

SELECT * FROM iceberg_hash_routes WHERE host = 'server42';

DELETE FROM iceberg_hash_routes WHERE host = 'b';

SELECT * FROM iceberg_hash_routes ORDER BY ts;
