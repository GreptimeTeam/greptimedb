CREATE TABLE integers (i BIGINT);

CREATE TABLE integers (i INT TIME INDEX);

CREATE TABLE integers (i BIGINT TIME INDEX NULL);

CREATE TABLE integers (i BIGINT TIME INDEX, j BIGINT, TIME INDEX(j));

CREATE TABLE integers (i BIGINT TIME INDEX, j BIGINT, TIME INDEX(i, j));

CREATE TABLE integers (i BIGINT TIME INDEX);

CREATE TABLE times (i TIMESTAMP TIME INDEX DEFAULT CURRENT_TIMESTAMP);

CREATE TABLE IF NOT EXISTS integers (i BIGINT TIME INDEX);

CREATE TABLE test1 (i INTEGER, j INTEGER);

CREATE TABLE test1 (i INTEGER, j BIGINT TIME INDEX NOT NULL);

CREATE TABLE test2 (i INTEGER, j BIGINT TIME INDEX NULL);

CREATE TABLE test2 (i INTEGER, j BIGINT TIME INDEX);

DESC TABLE integers;

DESC TABLE test1;

DESC TABLE test2;

DROP TABLE integers;

DROP TABLE times;

DROP TABLE test1;

DROP TABLE test2;

CREATE TABLE test_pk (timestamp BIGINT TIME INDEX, host STRING PRIMARY KEY, value DOUBLE);

DESC TABLE test_pk;

DROP TABLE test_pk;

CREATE TABLE test_multiple_pk_definitions (timestamp BIGINT TIME INDEX, host STRING PRIMARY KEY, value DOUBLE, PRIMARY KEY(host));

CREATE TABLE test_multiple_pk_definitions (timestamp BIGINT TIME INDEX, host STRING PRIMARY KEY, value DOUBLE, PRIMARY KEY(host), PRIMARY KEY(host));

CREATE TABLE test_multiple_inline_pk_definitions (timestamp BIGINT TIME INDEX, host STRING PRIMARY KEY, value DOUBLE PRIMARY KEY);
