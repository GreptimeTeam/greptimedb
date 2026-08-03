-- Test compressed CSV import functionality
-- First, create and export data with different compression types
CREATE TABLE test_csv_export(
    `id` UINT32,
    `name` STRING,
    `value` DOUBLE,
    ts TIMESTAMP TIME INDEX
);

-- Insert test data
INSERT INTO test_csv_export(`id`, `name`, `value`, ts) VALUES 
    (1, 'Alice', 10.5, 1640995200000),
    (2, 'Bob', 20.3, 1640995260000),
    (3, 'Charlie', 30.7, 1640995320000),
    (4, 'David', 40.1, 1640995380000),
    (5, 'Eve', 50.9, 1640995440000);

-- Export with different compression types
COPY test_csv_export TO '${SQLNESS_HOME}/import/test_csv_uncompressed.csv' WITH (format='csv');
COPY test_csv_export TO '${SQLNESS_HOME}/import/test_csv_gzip.csv.gz' WITH (format='csv', compression_type='gzip');
COPY test_csv_export TO '${SQLNESS_HOME}/import/test_csv_zstd.csv.zst' WITH (format='csv', compression_type='zstd');
COPY test_csv_export TO '${SQLNESS_HOME}/import/test_csv_bzip2.csv.bz2' WITH (format='csv', compression_type='bzip2');
COPY test_csv_export TO '${SQLNESS_HOME}/import/test_csv_xz.csv.xz' WITH (format='csv', compression_type='xz');

-- Test importing uncompressed CSV
CREATE TABLE test_csv_import_uncompressed(
    `id` UINT32,
    `name` STRING,
    `value` DOUBLE,
    ts TIMESTAMP TIME INDEX
);

COPY test_csv_import_uncompressed FROM '${SQLNESS_HOME}/import/test_csv_uncompressed.csv' WITH (format='csv');

SELECT COUNT(*) as uncompressed_count FROM test_csv_import_uncompressed;

-- Test importing GZIP compressed CSV
CREATE TABLE test_csv_import_gzip(
    `id` UINT32,
    `name` STRING,
    `value` DOUBLE,
    ts TIMESTAMP TIME INDEX
);

COPY test_csv_import_gzip FROM '${SQLNESS_HOME}/import/test_csv_gzip.csv.gz' WITH (format='csv', compression_type='gzip');

SELECT COUNT(*) as gzip_count FROM test_csv_import_gzip;
SELECT `id`, `name`, `value` FROM test_csv_import_gzip WHERE `id` = 1;

-- Test importing ZSTD compressed CSV
CREATE TABLE test_csv_import_zstd(
    `id` UINT32,
    `name` STRING,
    `value` DOUBLE,
    ts TIMESTAMP TIME INDEX
);

COPY test_csv_import_zstd FROM '${SQLNESS_HOME}/import/test_csv_zstd.csv.zst' WITH (format='csv', compression_type='zstd');

SELECT COUNT(*) as zstd_count FROM test_csv_import_zstd;
SELECT `id`, `name`, `value` FROM test_csv_import_zstd WHERE `id` = 2;

-- Test importing BZIP2 compressed CSV
CREATE TABLE test_csv_import_bzip2(
    `id` UINT32,
    `name` STRING,
    `value` DOUBLE,
    ts TIMESTAMP TIME INDEX
);

COPY test_csv_import_bzip2 FROM '${SQLNESS_HOME}/import/test_csv_bzip2.csv.bz2' WITH (format='csv', compression_type='bzip2');

SELECT COUNT(*) as bzip2_count FROM test_csv_import_bzip2;
SELECT `id`, `name`, `value` FROM test_csv_import_bzip2 WHERE `id` = 3;

-- Test importing XZ compressed CSV
CREATE TABLE test_csv_import_xz(
    `id` UINT32,
    `name` STRING,
    `value` DOUBLE,
    ts TIMESTAMP TIME INDEX
);

COPY test_csv_import_xz FROM '${SQLNESS_HOME}/import/test_csv_xz.csv.xz' WITH (format='csv', compression_type='xz');

SELECT COUNT(*) as xz_count FROM test_csv_import_xz;
SELECT `id`, `name`, `value` FROM test_csv_import_xz WHERE `id` = 4;

-- Verify data integrity by comparing all imported tables
SELECT source, count FROM (
    SELECT 'uncompressed' as source, COUNT(*) as count, 1 as order_key FROM test_csv_import_uncompressed
    UNION ALL
    SELECT 'gzip', COUNT(*) as count, 2 as order_key FROM test_csv_import_gzip
    UNION ALL
    SELECT 'zstd', COUNT(*) as count, 3 as order_key FROM test_csv_import_zstd
    UNION ALL
    SELECT 'bzip2', COUNT(*) as count, 4 as order_key FROM test_csv_import_bzip2
    UNION ALL
    SELECT 'xz', COUNT(*) as count, 5 as order_key FROM test_csv_import_xz
) AS subquery
ORDER BY order_key;

-- Clean up
DROP TABLE test_csv_export;
DROP TABLE test_csv_import_uncompressed;
DROP TABLE test_csv_import_gzip;
DROP TABLE test_csv_import_zstd;
DROP TABLE test_csv_import_bzip2;
DROP TABLE test_csv_import_xz;
