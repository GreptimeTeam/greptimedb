-- String position/search function tests

-- POSITION function
SELECT POSITION('world' IN 'hello world');

SELECT POSITION('xyz' IN 'hello world');

SELECT POSITION('' IN 'hello world');

SELECT POSITION('world' IN '');

-- STRPOS function (same as POSITION)
SELECT STRPOS('hello world', 'world');

SELECT STRPOS('hello world', 'xyz');

SELECT STRPOS('hello world', 'hello');

SELECT STRPOS('hello world', 'o');

-- INSTR function
SELECT INSTR('hello world', 'world');

SELECT INSTR('hello world', 'o');

SELECT INSTR('hello world', 'xyz');

-- Case sensitive search
SELECT POSITION('WORLD' IN 'hello world');

SELECT POSITION('World' IN 'hello world');

-- LEFT and RIGHT functions
SELECT LEFT('hello world', 5);

SELECT RIGHT('hello world', 5);

-- More than string length
SELECT LEFT('hello', 10);

-- More than string length
SELECT RIGHT('hello', 10);

-- Test with NULL values
SELECT POSITION('world' IN NULL);

SELECT POSITION(NULL IN 'hello world');

SELECT LEFT(NULL, 5);

SELECT RIGHT('hello', NULL);

-- Test with table data
CREATE TABLE position_test(s VARCHAR, "search" VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO position_test VALUES
    ('hello world', 'world', 1000),
    ('hello world', 'hello', 2000),
    ('hello world', 'xyz', 3000),
    ('programming', 'gram', 4000),
    ('database', 'base', 5000);

SELECT s, "search", POSITION("search" IN s) AS a, STRPOS(s, "search") AS b FROM position_test ORDER BY ts;

-- Test LEFT and RIGHT with table data
SELECT s, LEFT(s, 5), RIGHT(s, 5) FROM position_test ORDER BY ts;

-- Unicode position tests
SELECT POSITION('世' IN 'hello世界');

SELECT POSITION('界' IN 'hello世界');

SELECT STRPOS('café shop', 'é');

SELECT LEFT('中文测试', 2);

SELECT RIGHT('中文测试', 2);

-- Multiple occurrences (finds first one)
SELECT POSITION('o' IN 'hello world');

SELECT STRPOS('hello world', 'l');

DROP TABLE position_test;
