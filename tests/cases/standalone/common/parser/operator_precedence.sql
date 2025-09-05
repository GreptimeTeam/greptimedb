-- Existing operator precedence tests
SELECT 2*3+1;

SELECT 1+2*3;

SELECT 2^2 + 1;

SELECT 1+2^2;

SELECT 2*4 / 2;

SELECT 2*(4 / 2);

SELECT 16/2*4;

SELECT (16/2)*4;

SELECT 2*3*2;

SELECT 2^3*2;

SELECT 2*3^2;

-- Migrated from DuckDB test: test/sql/parser/division_operator_precedence.test
-- Additional division operator precedence tests (only standard division)

-- Division operator precedence
SELECT 6 * 1 / 2;

-- Addition and division precedence
SELECT 6 + 1 / 2;

-- More division precedence tests
SELECT 10 / 2 * 3;

SELECT 10 / (2 * 3);
