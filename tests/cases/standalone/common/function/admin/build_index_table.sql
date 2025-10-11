CREATE TABLE test (
    ts TIMESTAMP TIME INDEX,
    msg TEXT,
);

INSERT INTO test VALUES 
(1,"The quick brown fox jumps over the lazy dog"), 
(2,"The quick brown fox jumps over the lazy cat"), 
(3,"The quick brown fox jumps over the lazy mouse"), 
(4,"The quick brown fox jumps over the lazy rabbit"), 
(5,"The quick brown fox jumps over the lazy turtle");

SELECT * FROM test;

ADMIN FLUSH_TABLE('test');

-- No fulltext index yet
SELECT index_size FROM INFORMATION_SCHEMA.REGION_STATISTICS;

ALTER TABLE test MODIFY COLUMN msg SET FULLTEXT INDEX;

ADMIN BUILD_INDEX('test');

-- SQLNESS SLEEP 1s

-- Fulltext index built
SELECT index_size FROM INFORMATION_SCHEMA.REGION_STATISTICS;