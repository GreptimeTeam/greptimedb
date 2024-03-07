CREATE TABLE integers (i BIGINT);

CREATE TABLE integers (i INT TIME INDEX);

CREATE TABLE integers (i TIMESTAMP TIME INDEX NULL);

CREATE TABLE integers (i TIMESTAMP TIME INDEX, j BIGINT, TIME INDEX(j));

CREATE TABLE integers (i TIMESTAMP TIME INDEX, j BIGINT, TIME INDEX(i, j));

CREATE TABLE integers (i TIMESTAMP TIME INDEX);

CREATE TABLE times (i TIMESTAMP TIME INDEX DEFAULT CURRENT_TIMESTAMP());

CREATE TABLE IF NOT EXISTS integers (i TIMESTAMP TIME INDEX);

CREATE TABLE test1 (i INTEGER, j INTEGER);

CREATE TABLE test1 (i INTEGER, j TIMESTAMP TIME INDEX NOT NULL);

CREATE TABLE test2 (i INTEGER, j TIMESTAMP TIME INDEX NULL);

CREATE TABLE test2 (i INTEGER, j TIMESTAMP TIME INDEX);

CREATE TABLE test2 (i INTEGER, j TIMESTAMP TIME INDEX);

CREATE TABLE 'N.~' (i TIMESTAMP TIME INDEX);

DESC TABLE integers;

DESC TABLE test1;

DESC TABLE test2;

DROP TABLE integers;

DROP TABLE times;

DROP TABLE test1;

DROP TABLE test2;

CREATE TABLE test_pk ("timestamp" TIMESTAMP TIME INDEX, host STRING PRIMARY KEY, "value" DOUBLE);

DESC TABLE test_pk;

DROP TABLE test_pk;

CREATE TABLE test_multiple_pk_definitions ("timestamp" TIMESTAMP TIME INDEX, host STRING PRIMARY KEY, "value" DOUBLE, PRIMARY KEY(host));

CREATE TABLE test_multiple_pk_definitions ("timestamp" TIMESTAMP TIME INDEX, host STRING PRIMARY KEY, "value" DOUBLE, PRIMARY KEY(host), PRIMARY KEY(host));

CREATE TABLE test_multiple_inline_pk_definitions ("timestamp" TIMESTAMP TIME INDEX, host STRING PRIMARY KEY, "value" DOUBLE PRIMARY KEY);

CREATE TABLE neg_default_value(i INT DEFAULT -1024, ts TIMESTAMP TIME INDEX);

desc TABLE neg_default_value;

DROP TABLE neg_default_value;

CREATE TABLE test_like_1 (PK STRING PRIMARY KEY, i INTEGER DEFAULT 7, j TIMESTAMP TIME INDEX);

CREATE TABLE test_like_2 LIKE test_like_1;

CREATE TABLE test_like_2 LIKE test_like_1;

DESC TABLE test_like_1;

DESC TABLE test_like_2;

DROP TABLE test_like_1;

DROP TABLE test_like_2;
