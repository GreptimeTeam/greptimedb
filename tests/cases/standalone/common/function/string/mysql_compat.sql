-- MySQL-compatible string function tests

-- LOCATE function tests
SELECT LOCATE('world', 'hello world');

SELECT LOCATE('xyz', 'hello world');

SELECT LOCATE('o', 'hello world');

SELECT LOCATE('o', 'hello world', 5);

SELECT LOCATE('o', 'hello world', 6);

SELECT LOCATE('', 'hello');

SELECT LOCATE('世', 'hello世界');

SELECT LOCATE(NULL, 'hello');

SELECT LOCATE('o', NULL);

-- ELT function tests
SELECT ELT(1, 'a', 'b', 'c');

SELECT ELT(2, 'a', 'b', 'c');

SELECT ELT(3, 'a', 'b', 'c');

SELECT ELT(0, 'a', 'b', 'c');

SELECT ELT(4, 'a', 'b', 'c');

SELECT ELT(NULL, 'a', 'b', 'c');

-- FIELD function tests
SELECT FIELD('b', 'a', 'b', 'c');

SELECT FIELD('d', 'a', 'b', 'c');

SELECT FIELD('a', 'a', 'b', 'c');

SELECT FIELD('A', 'a', 'b', 'c');

SELECT FIELD(NULL, 'a', 'b', 'c');

-- FIELD coercion matrix: strings compare as strings; mixed values compare as DOUBLE.
SELECT FIELD(2, '02', 3);
SELECT FIELD('2', '02', '3');
SELECT FIELD(2, 2, 3);
SELECT FIELD(NULL, 1, 2);
SELECT FIELD(2, NULL, 2);
SELECT FIELD(12, '12x', 3);
SELECT FIELD(0, 'x', 1);
-- Pure integers retain exact comparison above the f64 precision boundary.
SELECT FIELD(9007199254740993, 9007199254740992, 9007199254740993);
-- A floating-point argument intentionally promotes the comparison to DOUBLE.
SELECT FIELD(9007199254740993, 9007199254740992e0);

-- INSERT function tests
SELECT INSERT('Quadratic', 3, 4, 'What');

SELECT INSERT('Quadratic', 3, 100, 'What');

SELECT INSERT('Quadratic', 0, 4, 'What');

SELECT INSERT('hello', 1, 0, 'X');

SELECT INSERT('hello世界', 6, 1, 'の');

SELECT INSERT(NULL, 1, 1, 'X');

-- SPACE function tests
SELECT SPACE(5);

SELECT SPACE(0);

SELECT SPACE(-1);

SELECT CONCAT('a', SPACE(3), 'b');

SELECT SPACE(NULL);

-- FORMAT function tests
SELECT FORMAT(1234567.891, 2);

SELECT FORMAT(1234567.891, 0);

SELECT FORMAT(1234.5, 4);

SELECT FORMAT(-1234567.891, 2);

SELECT FORMAT(0.5, 2);

SELECT FORMAT(123, 2);

SELECT FORMAT(NULL, 2);

-- Combined test with table
CREATE TABLE string_test(idx INT, val VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO string_test VALUES
(1, 'hello world', 1),
(2, 'foo bar baz', 2),
(3, 'hello世界', 3);

SELECT idx, val, LOCATE('o', val) as loc FROM string_test ORDER BY idx;

SELECT idx, val, INSERT(val, 1, 5, 'hi') as inserted FROM string_test ORDER BY idx;

DROP TABLE string_test;
