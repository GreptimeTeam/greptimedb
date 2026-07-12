-- Regular expression function tests

-- REGEXP_MATCHES function
SELECT regexp_like('hello123world', '\d+');

SELECT regexp_like('no numbers here', '\d+');

SELECT regexp_like('email@example.com', '[a-zA-Z0-9]+@[a-zA-Z0-9]+\.[a-zA-Z]+');

-- REGEXP_REPLACE function
SELECT REGEXP_REPLACE('hello123world', '\d+', 'XXX');

SELECT REGEXP_REPLACE('phone: 123-456-7890', '\d{3}-\d{3}-\d{4}', 'XXX-XXX-XXXX');

SELECT REGEXP_REPLACE('  extra   spaces  ', '\s+', ' ');

-- REGEXP_EXTRACT function
SELECT REGEXP_EXTRACT('version 1.2.3', '\d+\.\d+\.\d+');

SELECT REGEXP_EXTRACT('no match here', '\d+\.\d+\.\d+');

-- Test with ~ operator (regex match)
SELECT 'hello123' ~ '\d+';

SELECT 'hello world' ~ '\d+';

SELECT 'email@example.com' ~ '[a-zA-Z0-9]+@[a-zA-Z0-9]+\.[a-zA-Z]+';

-- Test with table data
CREATE TABLE regex_test("text" VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO regex_test VALUES
    ('Phone: 123-456-7890', 1000),
    ('Email: user@domain.com', 2000),
    ('Version 2.1.0', 3000),
    ('No pattern here', 4000);

SELECT "text", REGEXP_EXTRACT("text", '\d{3}-\d{3}-\d{4}') as phone FROM regex_test ORDER BY ts;

SELECT "text", REGEXP_EXTRACT("text", '[a-zA-Z0-9]+@[a-zA-Z0-9]+\.[a-zA-Z]+') as email FROM regex_test ORDER BY ts;

SELECT "text", REGEXP_EXTRACT("text", '\d+\.\d+\.\d+') as version FROM regex_test ORDER BY ts;

DROP TABLE regex_test;
