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
