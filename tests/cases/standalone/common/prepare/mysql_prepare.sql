-- invalid prepare, from
-- https://github.com/duckdb/duckdb/blob/00a605270719941ca0412ad5d0a14b1bdfbf9eb5/test/sql/prepared/invalid_prepare.test
-- SQLNESS PROTOCOL MYSQL
SELECT ?;

-- SQLNESS PROTOCOL MYSQL
PREPARE stmt FROM 'SELECT ?::int;';

-- SQLNESS PROTOCOL MYSQL
EXECUTE stmt USING 1;

-- SQLNESS PROTOCOL MYSQL
EXECUTE stmt USING 'a';

-- SQLNESS PROTOCOL MYSQL
DEALLOCATE stmt;

-- SQLNESS PROTOCOL MYSQL
PREPARE stmt FROM 'SELECT ?::int WHERE 1=0;';

-- SQLNESS PROTOCOL MYSQL
EXECUTE stmt USING 1;

-- SQLNESS PROTOCOL MYSQL
EXECUTE stmt USING 'a';

-- SQLNESS PROTOCOL MYSQL
DEALLOCATE stmt;

-- parameter variants, from:
-- https://github.com/duckdb/duckdb/blob/2360dd00f193b5d0850f9379d0c3794eb2084f36/test/sql/prepared/parameter_variants.test
-- SQLNESS PROTOCOL MYSQL
PREPARE stmt FROM 'SELECT CAST(? AS INTEGER), CAST(? AS STRING);';

-- SQLNESS PROTOCOL MYSQL
EXECUTE stmt USING 1, 'hello';

-- SQLNESS PROTOCOL MYSQL
DEALLOCATE stmt;
