-- GreptimeDB create table clause
-- structured test, use vector to pre-process log data into fields
CREATE TABLE IF NOT EXISTS `test_table` (
    `bytes` Int64 NULL,
    `http_version` STRING NULL,
    `ip` STRING NULL,
    `method` STRING NULL,
    `path` STRING NULL,
    `status` SMALLINT UNSIGNED NULL,
    `user` STRING NULL,
    `timestamp` TIMESTAMP(3) NOT NULL,
    PRIMARY KEY (`user`, `path`, `status`),
    TIME INDEX (`timestamp`)
)
ENGINE=mito
WITH(
    append_mode = 'true'
);

-- unstructured test, build fulltext index on message column
CREATE TABLE IF NOT EXISTS `test_table` (
    `message` STRING NULL FULLTEXT WITH(analyzer = 'English', case_sensitive = 'false'),
    `timestamp` TIMESTAMP(3) NOT NULL,
    TIME INDEX (`timestamp`)
)
ENGINE=mito
WITH(
    append_mode = 'true'
);

-- Clickhouse create table clause
-- structured test
CREATE TABLE IF NOT EXISTS test_table
(
    bytes UInt64 NOT NULL,
    http_version String NOT NULL,
    ip String NOT NULL,
    method String NOT NULL,
    path String NOT NULL,
    status UInt8 NOT NULL,
    user String NOT NULL,
    timestamp String NOT NULL,
)
ENGINE = MergeTree()
ORDER BY (user, path, status);

-- unstructured test
SET allow_experimental_full_text_index = true;
CREATE TABLE IF NOT EXISTS test_table
(
    message String,
    timestamp String,
    INDEX inv_idx(message) TYPE full_text(0) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY tuple();
