DESC TABLE information_schema.ssts_manifest;

DESC TABLE information_schema.ssts_storage;

DESC TABLE information_schema.ssts_index_meta;

CREATE TABLE sst_case (
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

INSERT INTO sst_case VALUES
  (500, 'a', 'a', 1),
  (1500, 'b', 'b', 2),
  (2500, 'c', 'c', 3);

ADMIN FLUSH_TABLE('sst_case');

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3,9}) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
-- SQLNESS REPLACE (/public/\d+) /public/<TABLE_ID>
SELECT * FROM information_schema.ssts_manifest order by file_path;

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3,9}) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+/index/<UUID>\.puffin) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>/index/<UUID>.puffin
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
-- SQLNESS REPLACE (/public/\d+) /public/<TABLE_ID>
SELECT * FROM information_schema.ssts_index_meta ORDER BY meta_json;

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
SELECT * FROM information_schema.ssts_storage order by file_path;

INSERT INTO sst_case VALUES
  (24, 'foo', 'foo', 100),
  (124, 'bar', 'bar', 200),
  (1024, 'baz', 'baz', 300);

ADMIN FLUSH_TABLE('sst_case');

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3,9}) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
-- SQLNESS REPLACE (/public/\d+) /public/<TABLE_ID>
SELECT * FROM information_schema.ssts_manifest order by file_path;

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3,9}) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+/index/<UUID>\.puffin) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>/index/<UUID>.puffin
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
-- SQLNESS REPLACE (/public/\d+) /public/<TABLE_ID>
SELECT * FROM information_schema.ssts_index_meta ORDER BY meta_json;

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
SELECT * FROM information_schema.ssts_storage order by file_path;

DROP TABLE sst_case;
