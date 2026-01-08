-- Test repartition error cases

-- Setup: Create a physical table with partitions
CREATE TABLE repartition_test_table(
  device_id INT,
  area STRING,
  ts TIMESTAMP TIME INDEX,
  PRIMARY KEY(device_id)
) PARTITION ON COLUMNS (device_id) (
  device_id < 100,
  device_id >= 100 AND device_id < 200,
  device_id >= 200
);

-- Setup: Create a logical table (metric engine)
CREATE TABLE physical_metric_table(
  ts TIMESTAMP TIME INDEX,
  val DOUBLE
) ENGINE = metric WITH ("physical_metric_table" = "");

CREATE TABLE logical_metric_table(
  ts TIMESTAMP TIME INDEX,
  val DOUBLE,
  device_id STRING PRIMARY KEY
) ENGINE = metric WITH ("on_physical_table" = "physical_metric_table");

-- Test 0: Logical table cannot be repartitioned
ALTER TABLE logical_metric_table REPARTITION (
  device_id < '100'
) INTO (
  device_id < '50',
  device_id >= '50' AND device_id < '100'
);

-- Test 1: New partition rule contains non-partition column (ts is not a partition column)
ALTER TABLE repartition_test_table REPARTITION (
  device_id < 100
) INTO (
  device_id < 50,
  device_id >= 50 AND device_id < 100 AND ts < 1000
);

-- Test 2: From partition expr does not exist in existing partition exprs
-- device_id < 50 is not in the existing partitions (which are < 100, >= 100 AND < 200, >= 200)
ALTER TABLE repartition_test_table REPARTITION (
  device_id < 50
) INTO (
  device_id < 25,
  device_id >= 25 AND device_id < 50
);

-- Test 3: New partition rule is incomplete (cannot pass checker)
-- This creates a gap: device_id < 50 and device_id >= 100, missing [50, 100)
-- The existing partitions are: device_id < 100, device_id >= 100 AND device_id < 200, device_id >= 200
-- After removing device_id < 100 and adding device_id < 50 and device_id >= 100, we get:
-- device_id < 50, device_id >= 100 AND device_id < 200, device_id >= 200
-- This leaves a gap [50, 100)
ALTER TABLE repartition_test_table REPARTITION (
  device_id < 100
) INTO (
  device_id < 50,
  device_id >= 100
);

-- Test 4: New partition rule has overlapping partitions
-- This creates overlapping ranges: device_id < 100 and device_id >= 50 AND device_id < 150
-- After removing device_id < 100, we have: device_id >= 100 AND device_id < 200, device_id >= 200
-- Adding the new ones: device_id < 100, device_id >= 50 AND device_id < 150
-- This overlaps: [0, 100) and [50, 150) overlap in [50, 100)
ALTER TABLE repartition_test_table REPARTITION (
  device_id < 100
) INTO (
  device_id < 100,
  device_id >= 50 AND device_id < 150
);

-- Cleanup
DROP TABLE repartition_test_table;
DROP TABLE logical_metric_table;
DROP TABLE physical_metric_table;
