CREATE TABLE `test` (
  `message` STRING,
  `time` TIMESTAMP TIME INDEX,
) WITH (
  append_mode = 'true'
);

SHOW CREATE TABLE test;

-- Write/read after altering column fulltext options
INSERT INTO test VALUES ('hello', '2020-01-01 00:00:00'), 
('world', '2020-01-01 00:00:01'), 
('hello world', '2020-01-02 00:00:00'), 
('world hello', '2020-01-02 00:00:01');

SELECT * FROM test WHERE MATCHES(message, 'hello') ORDER BY message;

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT INDEX WITH(analyzer = 'Chinese', case_sensitive = 'true');

SELECT * FROM test WHERE MATCHES(message, 'hello') ORDER BY message;

INSERT INTO test VALUES ('hello NiKo', '2020-01-03 00:00:00'), 
('NiKo hello', '2020-01-03 00:00:01'), 
('hello hello', '2020-01-04 00:00:00'), 
('NiKo, NiKo', '2020-01-04 00:00:01');

SELECT * FROM test WHERE MATCHES(message, 'hello') ORDER BY message;

-- SQLNESS ARG restart=true
SHOW CREATE TABLE test;

SHOW INDEX FROM test;

ALTER TABLE test MODIFY COLUMN message UNSET FULLTEXT INDEX;

SHOW CREATE TABLE test;

SHOW INDEX FROM test;

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT INDEX WITH(analyzer = 'Chinese', case_sensitive = 'true');

SHOW CREATE TABLE test;

SHOW INDEX FROM test;

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT INDEX WITH(analyzer = 'Chinese', case_sensitive = 'false');

ALTER TABLE test MODIFY COLUMN message UNSET FULLTEXT INDEX;

SHOW CREATE TABLE test;

SHOW INDEX FROM test;

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT INDEX WITH(analyzer = 'Chinese', case_sensitive = 'true', backend = 'bloom');

SHOW CREATE TABLE test;

SHOW INDEX FROM test;

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT INDEX WITH(analyzer = 'Chinese', case_sensitive = 'true', backend = 'tantivy');

SHOW CREATE TABLE test;

SHOW INDEX FROM test;

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT INDEX WITH(analyzer = 'Chinglish', case_sensitive = 'false');

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT INDEX WITH(analyzer = 'Chinese', case_sensitive = 'no');

ALTER TABLE test MODIFY COLUMN time SET FULLTEXT INDEX WITH(analyzer = 'Chinese', case_sensitive = 'false');

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT INDEX WITH(analyzer = 'English', case_sensitive = 'true');

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT INDEX WITH(backend = 'xor');

DROP TABLE test;
