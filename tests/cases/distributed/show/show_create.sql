CREATE TABLE system_metrics (
  id INT UNSIGNED,
  host STRING,
  cpu DOUBLE,
  disk FLOAT,
  n INT,
  ts TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  TIME INDEX (ts),
  PRIMARY KEY (id, host)
)
PARTITION BY RANGE COLUMNS (n) (
    PARTITION r0 VALUES LESS THAN (5),
    PARTITION r1 VALUES LESS THAN (9),
    PARTITION r2 VALUES LESS THAN (MAXVALUE),
)
ENGINE=mito
WITH(
  regions = 3
);

SHOW CREATE TABLE system_metrics;

DROP TABLE system_metrics;
