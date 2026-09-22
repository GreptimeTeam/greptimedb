CREATE TABLE legacy_build_index (ts TIMESTAMP TIME INDEX, msg STRING);
INSERT INTO legacy_build_index VALUES (1, 'hello world'), (2, 'goodbye world');
ADMIN FLUSH_TABLE('legacy_build_index');
