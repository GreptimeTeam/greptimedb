CREATE TABLE IF NOT EXISTS `test_table` (
  `bytes` BIGINT NULL,
  `http_version` STRING NULL,
  `ip` STRING NULL,
  `method` STRING NULL,
  `path` STRING NULL,
  `status` SMALLINT UNSIGNED NULL,
  `user` STRING NULL,
  `timestamp` TIMESTAMP(3) NOT NULL,
  TIME INDEX (`timestamp`),
  PRIMARY KEY (`user`, `path`, `status`)
)

ENGINE=mito
WITH(
  append_mode = 'true'
);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN ANALYZE SELECT count(*) FROM test_table;
