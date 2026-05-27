DESC TABLE information_schema.region_info;

CREATE TABLE region_info_case (
  a INT PRIMARY KEY,
  ts TIMESTAMP TIME INDEX,
)
WITH ("sst_format" = "flat");

INSERT INTO region_info_case VALUES (1, 1), (2, 2);

ADMIN FLUSH_TABLE('region_info_case');

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE (\{".*"\}) <JSON>
-- SQLNESS REPLACE (-{40,}) ----------------
-- SQLNESS REPLACE (region_options\s+\|) region_options |
SELECT region_id, state, role, writable, sequence, manifest_version, compaction_time_window, region_options, sst_format
FROM information_schema.region_info
WHERE region_id IN (
  SELECT region_id FROM information_schema.region_peers WHERE table_name = 'region_info_case'
)
ORDER BY region_id;

DROP TABLE region_info_case;
