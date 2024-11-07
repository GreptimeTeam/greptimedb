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

SELECT * FROM test WHERE MATCHES(message, 'hello');

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT WITH(analyzer = 'Chinese', case_sensitive = 'true');

SELECT * FROM test WHERE MATCHES(message, 'hello');

-- SQLNESS ARG restart=true
SHOW CREATE TABLE test;

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT WITH(enable = 'false');

SHOW CREATE TABLE test;

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT WITH(analyzer = 'English', case_sensitive = 'true');

SHOW CREATE TABLE test;

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT WITH(analyzer = 'Chinese', case_sensitive = 'false');

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT WITH(enable = 'false');

SHOW CREATE TABLE test;

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT WITH(analyzer = 'Chinglish', case_sensitive = 'false');

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT WITH(analyzer = 'Chinese', case_sensitive = 'no');

ALTER TABLE test MODIFY COLUMN time SET FULLTEXT WITH(analyzer = 'Chinese', case_sensitive = 'false');

DROP TABLE test;
