SELECT ts, CAST(payload AS STRING) FROM legacy_blob ORDER BY ts;

CREATE TABLE stored_blob (
    ts TIMESTAMP TIME INDEX,
    payload BINARY STORAGE WITH (threshold = '1KiB')
);
INSERT INTO stored_blob SELECT * FROM legacy_blob;
ADMIN FLUSH_TABLE('stored_blob');

-- SQLNESS ARG restart=true
SHOW CREATE TABLE stored_blob;

INSERT INTO stored_blob VALUES (2, 'after restart');
ADMIN FLUSH_TABLE('stored_blob');
SELECT ts, CAST(payload AS STRING) FROM stored_blob ORDER BY ts;
