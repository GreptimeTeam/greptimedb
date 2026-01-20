CREATE TABLE alter_repartition_table(
  device_id INT,
  area STRING,
  ty STRING,
  ts TIMESTAMP TIME INDEX,
  PRIMARY KEY(device_id)
) PARTITION ON COLUMNS (device_id, area) (
  device_id < 100,
  device_id >= 100 AND device_id < 200,
  device_id >= 200
);

ALTER TABLE alter_repartition_table REPARTITION (
  device_id < 100
) INTO (
  device_id < 100 AND area < 'South',
  device_id < 100 AND area >= 'South'
);

SHOW CREATE TABLE alter_repartition_table;

ALTER TABLE alter_repartition_table MERGE PARTITION (
  device_id < 100 AND area < 'South',
  device_id < 100 AND area >= 'South'
);

SHOW CREATE TABLE alter_repartition_table;

-- FIXME(weny): Object store is not configured for the test environment,
-- so staging manifest may not be applied in some cases.

-- invalid: empty source clause
ALTER TABLE alter_repartition_table REPARTITION () INTO (
  device_id < 100
);

-- invalid: more than one INTO clause
ALTER TABLE alter_repartition_table REPARTITION (
  device_id < 100
) INTO (
  device_id < 50
), (
  device_id >= 50
) INTO (
  device_id >= 50
);

DROP TABLE alter_repartition_table;

CREATE TABLE alter_repartition_table_with_options(
  device_id INT,
  area STRING,
  ty STRING,
  ts TIMESTAMP TIME INDEX,
  PRIMARY KEY(device_id)
) PARTITION ON COLUMNS (device_id, area) (
  device_id < 100,
  device_id >= 100 AND device_id < 200,
  device_id >= 200
);

-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}) PROC_ID
ALTER TABLE alter_repartition_table_with_options REPARTITION (
  device_id < 100
) INTO (
  device_id < 100 AND area < 'South',
  device_id < 100 AND area >= 'South'
) WITH (
  TIMEOUT = '5m',
  WAIT = false
);

DROP TABLE alter_repartition_table_with_options;

-- Metric engine repartition test
CREATE TABLE metric_physical_table (
    ts TIMESTAMP TIME INDEX,
    host STRING,
    cpu DOUBLE,
    PRIMARY KEY(host)
)
PARTITION ON COLUMNS (host) (
    host < 'h1',
    host >= 'h1' AND host < 'h2',
    host >= 'h2'
)
ENGINE = metric
WITH (
    physical_metric_table = "true"
);

CREATE TABLE logical_table_v1 (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    cpu DOUBLE,
)
ENGINE = metric
WITH (
    on_physical_table = "metric_physical_table"
);

CREATE TABLE logical_table_v2 (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    cpu DOUBLE,
)
ENGINE = metric
WITH (
    on_physical_table = "metric_physical_table"
);

-- Split physical table partition
ALTER TABLE metric_physical_table SPLIT PARTITION (
    host < 'h1'
) INTO (
    host < 'h0',
    host >= 'h0' AND host < 'h1'
);

SHOW CREATE TABLE metric_physical_table;

-- Verify select * works and returns empty
SELECT * FROM metric_physical_table;

SELECT * FROM logical_table_v1;

SELECT * FROM logical_table_v2;

-- Merge physical table partition
ALTER TABLE metric_physical_table MERGE PARTITION (
    host < 'h0',
    host >= 'h0' AND host < 'h1'
);

SHOW CREATE TABLE metric_physical_table;

-- Verify select * works and returns empty
SELECT * FROM metric_physical_table;

SELECT * FROM logical_table_v1;

SELECT * FROM logical_table_v2;

DROP TABLE logical_table_v1;

DROP TABLE logical_table_v2;

DROP TABLE metric_physical_table;
