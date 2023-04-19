CREATE TABLE system_metrics (
  id INT UNSIGNED NULL,
  host STRING NULL,
  cpu DOUBLE NULL,
  disk FLOAT NULL,
  ts TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  TIME INDEX (ts),
  PRIMARY KEY (id, host)
)
ENGINE=mito
WITH(
  regions = 3
);

SHOW CREATE TABLE system_metrics;

DROP TABLE system_metrics;
