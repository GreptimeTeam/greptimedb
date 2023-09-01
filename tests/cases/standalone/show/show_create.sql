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

CREATE TABLE not_supported_table_options_keys (
  id INT UNSIGNED,
  host STRING,
  cpu DOUBLE,
  disk FLOAT,
  n INT COMMENT 'range key',
  ts TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  TIME INDEX (ts),
  PRIMARY KEY (id, host)
)
ENGINE=mito
WITH(
  foo = 123,
  ttl = '7d',
  write_buffer_size = 1024
);
