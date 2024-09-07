CREATE TABLE `test` (
  `message` STRING FULLTEXT,
  `time` TIMESTAMP TIME INDEX,
) WITH (
  append_mode = 'true'
);

SHOW CREATE TABLE test;

ALTER TABLE test SET COLUMN message WITH(analyzer = 'Chinese', case_sensitive = 'true');

SHOW CREATE TABLE test;

ALTER TABLE test SET COLUMN message WITH(analyzer = 'Chinese', case_sensitive = 'false');

SHOW CREATE TABLE test;

ALTER TABLE test SET COLUMN message WITH(analyzer = 'English', case_sensitive = 'true');

SHOW CREATE TABLE test;

ALTER TABLE test SET COLUMN message WITH(analyzer = 'English', case_sensitive = 'false');

SHOW CREATE TABLE test;

ALTER TABLE test SET COLUMN message WITH(analyzer = 'English', case_sensitive = 'yes');

SHOW CREATE TABLE test;

ALTER TABLE test SET COLUMN message WITH(analyzer = 'Chinglish', case_sensitive = 'false');

SHOW CREATE TABLE test;

ALTER TABLE test SET COLUMN time WITH(analyzer = 'Chinese', case_sensitive = 'false');

SHOW CREATE TABLE test;

ALTER TABLE test SET COLUMN time WITH(analyzer = 'Chinese', case_sensitive = 'no');

SHOW CREATE TABLE test;

DROP TABLE test;
