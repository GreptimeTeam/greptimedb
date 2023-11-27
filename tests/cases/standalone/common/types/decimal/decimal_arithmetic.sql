-- Port from https://github.com/duckdb/duckdb/blob/main/test/sql/types/decimal/decimal_arithmetic.test

-- negate

SELECT -('0.1'::DECIMAL), -('-0.1'::DECIMAL);

-- unary +

SELECT +('0.1'::DECIMAL), +('-0.1'::DECIMAL);

-- addition

SELECT '0.1'::DECIMAL + '0.1'::DECIMAL;

-- addition with non-decimal

SELECT '0.1'::DECIMAL + 1::INTEGER;

SELECT '0.5'::DECIMAL(4,4) + '0.5'::DECIMAL(4,4);

-- addition between different decimal types

SELECT '0.5'::DECIMAL(1,1) + '100.0'::DECIMAL(3,0);

-- test decimals and integers with big decimals

SELECT ('0.5'::DECIMAL(1,1) + 10000)::VARCHAR,
        ('0.54321'::DECIMAL(5,5) + 10000)::VARCHAR,
        ('0.5432154321'::DECIMAL(10,10) + 10000)::VARCHAR,
        ('0.543215432154321'::DECIMAL(15,15) + 10000::DECIMAL(20,15))::VARCHAR,
        ('0.54321543215432154321'::DECIMAL(20,20) + 10000)::VARCHAR,
        ('0.5432154321543215432154321'::DECIMAL(25,25) + 10000)::VARCHAR;

-- out of range

SELECT ('0.54321543215432154321543215432154321'::DECIMAL(35,35) + 10000)::VARCHAR;

-- different types

SELECT '0.5'::DECIMAL(1,1) + 1::TINYINT,
        '0.5'::DECIMAL(1,1) + 2::SMALLINT,
        '0.5'::DECIMAL(1,1) + 3::INTEGER,
        '0.5'::DECIMAL(1,1) + 4::BIGINT;

-- negative numbers

SELECT '0.5'::DECIMAL(1,1) + -1::TINYINT,
        '0.5'::DECIMAL(1,1) + -2::SMALLINT,
        '0.5'::DECIMAL(1,1) + -3::INTEGER,
        '0.5'::DECIMAL(1,1) + -4::BIGINT;

-- subtract

SELECT '0.5'::DECIMAL(1,1) - 1::TINYINT,
        '0.5'::DECIMAL(1,1) - 2::SMALLINT,
        '0.5'::DECIMAL(1,1) - 3::INTEGER,
        '0.5'::DECIMAL(1,1) - 4::BIGINT;

-- negative numbers

SELECT '0.5'::DECIMAL(1,1) - -1::TINYINT,
        '0.5'::DECIMAL(1,1) - -2::SMALLINT,
        '0.5'::DECIMAL(1,1) - -3::INTEGER,
        '0.5'::DECIMAL(1,1) - -4::BIGINT;


-- now with a table

CREATE TABLE decimals(d DECIMAL(3, 2), ts timestamp time index);

INSERT INTO decimals VALUES ('0.1',1000), ('0.2',1000);

SELECT * FROM decimals;

SELECT d + 10000 FROM decimals;

SELECT d + '0.1'::DECIMAL, d + 10000 FROM decimals;

DROP TABLE decimals;

-- multiplication

SELECT '0.1'::DECIMAL * '10.0'::DECIMAL;

SELECT arrow_typeof('0.1'::DECIMAL(2,1) * '10.0'::DECIMAL(3,1));

SELECT '0.1'::DECIMAL * '0.1'::DECIMAL;

-- multiplication with non-decimal

SELECT '0.1'::DECIMAL * 10::INTEGER;

SELECT '5.0'::DECIMAL(4,3) * '5.0'::DECIMAL(4,3);

-- negative multiplication

SELECT '-5.0'::DECIMAL(4,3) * '5.0'::DECIMAL(4,3);

-- no precision is lost

SELECT ('18.25'::DECIMAL(4,2) * '17.25'::DECIMAL(4,2))::VARCHAR;

-- different types

SELECT '0.001'::DECIMAL * 100::TINYINT,
        '0.001'::DECIMAL * 10000::SMALLINT,
        '0.001'::DECIMAL * 1000000::INTEGER,
        '0.001'::DECIMAL * 100000000::BIGINT;

-- multiplication could not be performed exactly: throw error

SELECT '0.000000000000000000000000000001'::DECIMAL(38,30) * '0.000000000000000000000000000001'::DECIMAL(38,30);

-- test addition, subtraction and multiplication with various scales and precisions

SELECT 2.0 + 1.0 as col1,
       2.0000 + 1.0000 as col2,
       2.000000000000 + 1.000000000000 as col3,
       2.00000000000000000000 + 1.00000000000000000000 as col4;

SELECT 2.0 - 1.0 as col1,
       2.0000 - 1.0000 as col2,
       2.000000000000 - 1.000000000000 as col3,
       2.00000000000000000000 - 1.00000000000000000000 as col4;

SELECT 2.0 * 1.0 as col1,
       2.0000 * 1.0000 as col2;
