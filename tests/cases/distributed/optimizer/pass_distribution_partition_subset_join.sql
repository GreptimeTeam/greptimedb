CREATE TABLE pd_sub_l(
  ts TIMESTAMP TIME INDEX,
  device_id INTEGER,
  area STRING,
  lv INTEGER,
  PRIMARY KEY(device_id, area)
) PARTITION ON COLUMNS (device_id, area) (
  device_id < 2 AND area < 'm',
  device_id < 2 AND area >= 'm',
  device_id >= 2
) ENGINE=mito;

CREATE TABLE pd_sub_r(
  ts TIMESTAMP TIME INDEX,
  device_id INTEGER,
  area STRING,
  rv INTEGER,
  PRIMARY KEY(device_id, area)
) PARTITION ON COLUMNS (device_id, area) (
  device_id < 2 AND area < 'm',
  device_id < 2 AND area >= 'm',
  device_id >= 2
) ENGINE=mito;

INSERT INTO pd_sub_l VALUES
  (1, 1, 'a', 10),
  (2, 1, 'z', 20);

INSERT INTO pd_sub_r VALUES
  (1, 1, 'a', 100),
  (2, 1, 'z', 200);

ADMIN FLUSH_TABLE('pd_sub_l');
ADMIN FLUSH_TABLE('pd_sub_r');

-- Joining on only a subset of a multi-column partition key must still compare
-- rows across all matching partitions for that subset key.
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \d+\(\d+,\s+\d+\) REDACTED_REGION
-- SQLNESS REPLACE Hash\(\[device_id@0\],\s+\d+\) Hash([device_id@0],REDACTED_PARTITIONS)
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
-- SQLNESS REPLACE input_partitions=REDACTED\s+\| input_partitions=REDACTED|
EXPLAIN SELECT l.area, r.area
FROM pd_sub_l l JOIN pd_sub_r r ON l.device_id = r.device_id
ORDER BY l.area, r.area;

SELECT l.area, r.area
FROM pd_sub_l l JOIN pd_sub_r r ON l.device_id = r.device_id
ORDER BY l.area, r.area;

DROP TABLE pd_sub_l;
DROP TABLE pd_sub_r;
