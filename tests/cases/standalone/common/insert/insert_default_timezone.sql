--- insert timestamp with default values aware of session timezone test ---

CREATE TABLE test1 (i INTEGER, j TIMESTAMP default '2024-01-30 00:01:01' TIME INDEX, PRIMARY KEY(i));

INSERT INTO test1 VALUES (1, DEFAULT), (2, DEFAULT), (3, '2024-01-31 00:01:01'), (4, '2025-02-01 00:01:01');

SELECT * FROM test1;

SET time_zone = 'Asia/Shanghai';

CREATE TABLE test2 (i INTEGER, j TIMESTAMP default '2024-01-30 00:01:01' TIME INDEX, PRIMARY KEY(i));

INSERT INTO test2 VALUES (1, DEFAULT), (2, DEFAULT), (3, '2024-01-31 00:01:01'), (4, '2025-02-01 00:01:01');

SELECT * FROM test2;

SELECT * FROM test1;

SET time_zone = 'UTC';

DROP TABLE test1;

DROP TABLE test2;
