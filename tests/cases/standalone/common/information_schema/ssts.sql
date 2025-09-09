DESC TABLE information_schema.ssts_manifest;

DESC TABLE information_schema.ssts_storage;

CREATE TABLE sst_case (
  a INT PRIMARY KEY,
  b STRING SKIPPING INDEX,
  ts TIMESTAMP TIME INDEX,
)
PARTITION ON COLUMNS (a) (
  a < 1000,
  a >= 1000 AND a < 2000,
  a >= 2000
);

INSERT INTO sst_case VALUES
  (500, 'a', 1),
  (1500, 'b', 2),
  (2500, 'c', 3);

ADMIN FLUSH_TABLE('sst_case');

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3,9}) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
-- SQLNESS REPLACE (/public/\d+) /public/<TABLE_ID>
SELECT * FROM information_schema.ssts_manifest order by file_path;

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
SELECT * FROM information_schema.ssts_storage order by file_path;

INSERT INTO sst_case VALUES
  (24, 'foo', 100),
  (124, 'bar', 200),
  (1024, 'baz', 300);

ADMIN FLUSH_TABLE('sst_case');

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3,9}) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
-- SQLNESS REPLACE (/public/\d+) /public/<TABLE_ID>
SELECT * FROM information_schema.ssts_manifest order by file_path;

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
SELECT * FROM information_schema.ssts_storage order by file_path;

DROP TABLE sst_case;
