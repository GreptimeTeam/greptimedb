SELECT * FROM legacy_payload ORDER BY ts;

CREATE TABLE compressed_payload (
    ts TIMESTAMP TIME INDEX,
    payload STRING COMPRESSION WITH (level = 3, page_size = '64KiB')
);
INSERT INTO compressed_payload SELECT * FROM legacy_payload;
ADMIN FLUSH_TABLE('compressed_payload');

-- SQLNESS ARG restart=true
SHOW CREATE TABLE compressed_payload;

INSERT INTO compressed_payload VALUES (2, 'after restart');
ADMIN FLUSH_TABLE('compressed_payload');
SELECT * FROM compressed_payload ORDER BY ts;
