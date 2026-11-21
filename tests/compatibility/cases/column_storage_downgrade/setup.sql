CREATE TABLE external_blob (
    ts TIMESTAMP TIME INDEX,
    payload STRING STORAGE WITH (threshold = '1KiB')
);
INSERT INTO external_blob VALUES (1, 'short');
INSERT INTO external_blob SELECT 2::TIMESTAMP, repeat('x', 4096);
ADMIN FLUSH_TABLE('external_blob');
