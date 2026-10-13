CREATE TABLE legacy_payload (ts TIMESTAMP TIME INDEX, payload STRING);
INSERT INTO legacy_payload VALUES (1, 'before upgrade');
ADMIN FLUSH_TABLE('legacy_payload');
