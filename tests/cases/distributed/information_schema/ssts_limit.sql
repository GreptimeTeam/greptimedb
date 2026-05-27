CREATE TABLE ssts_limit_case (
  a INT PRIMARY KEY INVERTED INDEX,
  b STRING SKIPPING INDEX,
  c STRING FULLTEXT INDEX,
  ts TIMESTAMP TIME INDEX,
)
PARTITION ON COLUMNS (a) (
  a < 1000,
  a >= 1000 AND a < 2000,
  a >= 2000
);

INSERT INTO ssts_limit_case VALUES
  (500, 'a', 'a', 1),
  (1500, 'b', 'b', 2),
  (2500, 'c', 'c', 3);

ADMIN FLUSH_TABLE('ssts_limit_case');

SELECT COUNT(*) FROM (SELECT region_id FROM information_schema.ssts_manifest LIMIT 1);

SELECT COUNT(*) FROM (SELECT index_file_path FROM information_schema.ssts_index_meta LIMIT 1);

SELECT COUNT(*) FROM (SELECT file_path FROM information_schema.ssts_storage LIMIT 1);

DROP TABLE ssts_limit_case;
