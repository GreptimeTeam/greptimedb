SELECT ts, CAST(payload AS STRING) FROM legacy_blob ORDER BY ts;

CREATE TABLE stored_blob (
    ts TIMESTAMP TIME INDEX,
    payload BINARY STORAGE WITH (threshold = '1KiB')
);
INSERT INTO stored_blob SELECT * FROM legacy_blob;
-- Above the threshold, so the value goes to the blob sidecar.
INSERT INTO stored_blob SELECT 3::TIMESTAMP, arrow_cast(repeat('y', 4096), 'Binary');
ADMIN FLUSH_TABLE('stored_blob');

-- SQLNESS ARG restart=true
SHOW CREATE TABLE stored_blob;

INSERT INTO stored_blob VALUES (2, 'after restart');
ADMIN FLUSH_TABLE('stored_blob');
SELECT ts, CAST(payload AS STRING) FROM stored_blob WHERE ts <> 3 ORDER BY ts;
SELECT ts, length(CAST(payload AS STRING)) FROM stored_blob WHERE ts = 3;
