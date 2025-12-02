-- Test compressed JSON export functionality
CREATE TABLE test_json_compression(
    `id` UINT32,
    `name` STRING,
    `value` DOUBLE,
    ts TIMESTAMP TIME INDEX
);

-- Insert test data
INSERT INTO test_json_compression(`id`, `name`, `value`, ts) VALUES 
    (1, 'Alice', 10.5, 1640995200000),
    (2, 'Bob', 20.3, 1640995260000),
    (3, 'Charlie', 30.7, 1640995320000),
    (4, 'David', 40.1, 1640995380000),
    (5, 'Eve', 50.9, 1640995440000);

-- Test uncompressed JSON export
COPY test_json_compression TO '${SQLNESS_HOME}/export/test_json_uncompressed.json' WITH (format='json');

-- Test GZIP compressed JSON export
COPY test_json_compression TO '${SQLNESS_HOME}/export/test_json_gzip.json.gz' WITH (format='json', compression_type='gzip');

-- Test ZSTD compressed JSON export
COPY test_json_compression TO '${SQLNESS_HOME}/export/test_json_zstd.json.zst' WITH (format='json', compression_type='zstd');

-- Test BZIP2 compressed JSON export
COPY test_json_compression TO '${SQLNESS_HOME}/export/test_json_bzip2.json.bz2' WITH (format='json', compression_type='bzip2');

-- Test XZ compressed JSON export
COPY test_json_compression TO '${SQLNESS_HOME}/export/test_json_xz.json.xz' WITH (format='json', compression_type='xz');

-- Test compressed JSON with schema inference limit
COPY test_json_compression TO '${SQLNESS_HOME}/export/test_json_schema_limit.json.gz' WITH (format='json', compression_type='gzip', schema_infer_max_record=100);

-- Clean up
DROP TABLE test_json_compression;
