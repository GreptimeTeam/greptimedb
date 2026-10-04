-- Test compressed CSV export functionality
CREATE TABLE test_csv_compression(
    `id` UINT32,
    `name` STRING,
    `value` DOUBLE,
    ts TIMESTAMP TIME INDEX
);

-- Insert test data
INSERT INTO test_csv_compression(`id`, `name`, `value`, ts) VALUES 
    (1, 'Alice', 10.5, 1640995200000),
    (2, 'Bob', 20.3, 1640995260000),
    (3, 'Charlie', 30.7, 1640995320000),
    (4, 'David', 40.1, 1640995380000),
    (5, 'Eve', 50.9, 1640995440000);

-- Test uncompressed CSV export
COPY test_csv_compression TO '${SQLNESS_HOME}/export/test_csv_uncompressed.csv' WITH (format='csv');

-- Test GZIP compressed CSV export
COPY test_csv_compression TO '${SQLNESS_HOME}/export/test_csv_gzip.csv.gz' WITH (format='csv', compression_type='gzip');

-- Test ZSTD compressed CSV export
COPY test_csv_compression TO '${SQLNESS_HOME}/export/test_csv_zstd.csv.zst' WITH (format='csv', compression_type='zstd');

-- Test BZIP2 compressed CSV export
COPY test_csv_compression TO '${SQLNESS_HOME}/export/test_csv_bzip2.csv.bz2' WITH (format='csv', compression_type='bzip2');

-- Test XZ compressed CSV export
COPY test_csv_compression TO '${SQLNESS_HOME}/export/test_csv_xz.csv.xz' WITH (format='csv', compression_type='xz');

-- Test compressed CSV with custom delimiter b'\t' (ASCII 9)
COPY test_csv_compression TO '${SQLNESS_HOME}/export/test_csv_tab_separated.csv.gz' WITH (format='csv', compression_type='gzip', delimiter='9');

-- Test compressed CSV with timestamp format
COPY test_csv_compression TO '${SQLNESS_HOME}/export/test_csv_timestamp_format.csv.zst' WITH (format='csv', compression_type='zstd', timestamp_format='%Y-%m-%d %H:%M:%S');

-- Test compressed CSV with date format
COPY test_csv_compression TO '${SQLNESS_HOME}/export/test_csv_date_format.csv.gz' WITH (format='csv', compression_type='gzip', date_format='%Y/%m/%d');

-- Clean up
DROP TABLE test_csv_compression;
