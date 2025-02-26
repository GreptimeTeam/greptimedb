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

INSERT INTO `test_table` (`bytes`, `http_version`, `ip`, `method`, `path`, `status`, `user`, `timestamp`)
VALUES (1024, 'HTTP/1.1', '192.168.1.1', 'GET', '/index.html', 200, 'user1', 1667446797450);

SELECT count(*) FROM test_table;

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE SELECT count(*) FROM test_table;

DROP TABLE test_table;
