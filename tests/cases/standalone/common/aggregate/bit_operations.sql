-- Migrated from DuckDB test: test/sql/aggregate/aggregates/test_bit_*.test
-- Test bitwise aggregate operations

-- Test BIT_AND
CREATE TABLE bit_test(i INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO bit_test VALUES
    (7, 1000),   -- 111
    (3, 2000),   -- 011
    (5, 3000),   -- 101
    (NULL, 4000);

-- Should be 1 (001)
SELECT BIT_AND(i) FROM bit_test;

-- Test BIT_OR
-- Should be 7 (111)
SELECT BIT_OR(i) FROM bit_test;

-- Test BIT_XOR
-- Should be 1 (111 XOR 011 XOR 101)
SELECT BIT_XOR(i) FROM bit_test;

-- Test with groups
INSERT INTO bit_test VALUES (8, 5000), (12, 6000), (4, 7000);

-- Create separate table for group testing
CREATE TABLE bit_groups(grp INTEGER, i INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO bit_groups VALUES
    (1, 7, 1000), (1, 3, 2000), (1, 5, 3000),
    (2, 8, 4000), (2, 12, 5000), (2, 4, 6000);

SELECT grp, BIT_AND(i), BIT_OR(i), BIT_XOR(i) FROM bit_groups GROUP BY grp ORDER BY grp;

-- Test edge cases
-- NULL
SELECT BIT_AND(i) FROM bit_test WHERE i > 100;

SELECT BIT_OR(i) FROM bit_test WHERE i > 100;

SELECT BIT_XOR(i) FROM bit_test WHERE i > 100;

DROP TABLE bit_test;

DROP TABLE bit_groups;
