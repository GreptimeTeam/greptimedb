CREATE TABLE system_metrics (
  id INT UNSIGNED NULL,
  host STRING NULL,
  cpu DOUBLE NULL COMMENT 'cpu',
  disk FLOAT NULL,
  ts TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  TIME INDEX (ts),
  PRIMARY KEY (id, host)
)
ENGINE=mito
WITH(
  ttl = '7d',
  write_buffer_size = 1024
);

SHOW CREATE TABLE system_metrics;

DROP TABLE system_metrics;
