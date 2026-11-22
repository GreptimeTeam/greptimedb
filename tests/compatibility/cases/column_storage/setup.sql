CREATE TABLE legacy_blob (ts TIMESTAMP TIME INDEX, payload BINARY);
INSERT INTO legacy_blob VALUES (1, 'before upgrade');
ADMIN FLUSH_TABLE('legacy_blob');
