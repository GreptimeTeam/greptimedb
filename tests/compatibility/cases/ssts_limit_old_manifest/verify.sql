SELECT COUNT(DISTINCT node_id) > 1 AS has_multi_datanodes
FROM information_schema.ssts_manifest;

SELECT COUNT(*) AS limited_rows
FROM (
  SELECT region_id
  FROM information_schema.ssts_manifest
  LIMIT 1
);

SELECT COUNT(*) AS filtered_limited_rows
FROM (
  SELECT region_id
  FROM information_schema.ssts_manifest
  WHERE region_id > 0
  LIMIT 1
);
