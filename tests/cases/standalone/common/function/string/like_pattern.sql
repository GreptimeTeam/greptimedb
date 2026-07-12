-- String LIKE pattern matching tests

-- Basic LIKE patterns
SELECT 'hello world' LIKE 'hello%';

SELECT 'hello world' LIKE '%world';

SELECT 'hello world' LIKE '%llo%';

SELECT 'hello world' LIKE 'hello_world';

SELECT 'hello world' LIKE 'hello world';

-- LIKE with NOT
SELECT 'hello world' NOT LIKE 'goodbye%';

SELECT 'hello world' NOT LIKE 'hello%';

-- Case sensitivity
SELECT 'Hello World' LIKE 'hello%';

SELECT 'Hello World' ILIKE 'hello%';

SELECT 'Hello World' ILIKE 'HELLO%';

-- Test with table data
CREATE TABLE like_test("name" VARCHAR, email VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO like_test VALUES
    ('John Doe', 'john@example.com', 1000),
    ('Jane Smith', 'jane@gmail.com', 2000),
    ('Bob Wilson', 'bob@yahoo.com', 3000),
    ('Alice Johnson', 'alice@company.org', 4000),
    ('Charlie Brown', 'charlie@test.net', 5000);

-- Pattern matching on names
SELECT "name" FROM like_test WHERE "name" LIKE 'J%' ORDER BY ts;

SELECT "name" FROM like_test WHERE "name" LIKE '%son' ORDER BY ts;

-- Contains space
SELECT "name" FROM like_test WHERE "name" LIKE '% %' ORDER BY ts;

-- Pattern matching on emails
SELECT "name", email FROM like_test WHERE email LIKE '%@gmail.com' ORDER BY ts;

SELECT "name", email FROM like_test WHERE email LIKE '%.com' ORDER BY ts;

SELECT "name", email FROM like_test WHERE email LIKE '%@%.org' ORDER BY ts;

-- Underscore wildcard
SELECT "name" FROM like_test WHERE "name" LIKE 'Jo__ ___' ORDER BY ts;

SELECT email FROM like_test WHERE email LIKE '____@%' ORDER BY ts;

-- Multiple wildcards
-- Contains 'o'
SELECT "name" FROM like_test WHERE "name" LIKE '%o%' ORDER BY ts;

-- 'a' before and after @
SELECT email FROM like_test WHERE email LIKE '%a%@%a%' ORDER BY ts;

-- Escaping special characters
CREATE TABLE escape_test("text" VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO escape_test VALUES
    ('100% complete', 1000),
    ('test_file.txt', 2000),
    ('50% done', 3000),
    ('backup_2023.sql', 4000);

-- Need to escape % and _
-- Contains %
SELECT "text" FROM escape_test WHERE "text" LIKE '%\%%' ORDER BY ts;

-- Contains _
SELECT "text" FROM escape_test WHERE "text" LIKE '%\_%' ORDER BY ts;

-- Unicode pattern matching
CREATE TABLE unicode_like(s VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO unicode_like VALUES
    ('Hello 世界', 1000),
    ('🚀 rocket', 2000),
    ('café shop', 3000);

SELECT s FROM unicode_like WHERE s LIKE '%世界' ORDER BY ts;

SELECT s FROM unicode_like WHERE s LIKE '🚀%' ORDER BY ts;

SELECT s FROM unicode_like WHERE s LIKE '%é%' ORDER BY ts;

DROP TABLE like_test;

DROP TABLE escape_test;

DROP TABLE unicode_like;
