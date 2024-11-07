CREATE TABLE `test` (
  `message` STRING,
  `time` TIMESTAMP TIME INDEX,
) WITH (
  append_mode = 'true'
);

SHOW CREATE TABLE test;

ALTER TABLE test MODIFY COLUMN message SET FULLTEXT WITH(analyzer = 'Chinese', case_sensitive = 'true');

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