-- FLOAT type
-- any arithmetic on a nan value will result in a nan value

SELECT 'nan'::FLOAT + 1;

SELECT 'nan'::FLOAT + 'inf'::FLOAT;

SELECT 'nan'::FLOAT - 1;

SELECT 'nan'::FLOAT - 'inf'::FLOAT;

SELECT 'nan'::FLOAT * 1;

SELECT 'nan'::FLOAT * 'inf'::FLOAT;

SELECT 'nan'::FLOAT / 1;

SELECT 'nan'::FLOAT / 'inf'::FLOAT;

SELECT 'nan'::FLOAT % 1;

SELECT 'nan'::FLOAT % 'inf'::FLOAT;

SELECT -('nan'::FLOAT);

-- DOUBLE type
-- any arithmetic on a nan value will result in a nan value

SELECT 'nan'::DOUBLE + 1;

SELECT 'nan'::DOUBLE + 'inf'::DOUBLE;

SELECT 'nan'::DOUBLE - 1;

SELECT 'nan'::DOUBLE - 'inf'::DOUBLE;

SELECT 'nan'::DOUBLE * 1;

SELECT 'nan'::DOUBLE * 'inf'::DOUBLE;

SELECT 'nan'::DOUBLE / 1;

SELECT 'nan'::DOUBLE / 'inf'::DOUBLE;

SELECT 'nan'::DOUBLE % 1;

SELECT 'nan'::DOUBLE % 'inf'::DOUBLE;

SELECT -('nan'::DOUBLE);

-- FLOAT type
-- any arithmetic on a nan value will result in a nan value

SELECT 'inf'::FLOAT + 1;

-- inf + inf = inf
SELECT 'inf'::FLOAT + 'inf'::FLOAT;

SELECT 'inf'::FLOAT - 1;

-- inf - inf = nan
SELECT 'inf'::FLOAT - 'inf'::FLOAT;

SELECT 'inf'::FLOAT * 1;

-- inf * inf = inf
SELECT 'inf'::FLOAT * 'inf'::FLOAT;

-- inf * -inf = -inf
SELECT 'inf'::FLOAT * '-inf'::FLOAT;

SELECT 'inf'::FLOAT / 1;

SELECT 1 / 'inf'::FLOAT;

SELECT 'inf'::FLOAT % 1;

SELECT 1 % 'inf'::FLOAT;


-- DOUBLE type
-- any arithmetic on a nan value will result in a nan value

SELECT 'inf'::DOUBLE + 1;

-- inf + inf = inf
SELECT 'inf'::DOUBLE + 'inf'::DOUBLE;

SELECT 'inf'::DOUBLE - 1;

-- inf - inf = nan
SELECT 'inf'::DOUBLE - 'inf'::DOUBLE;

SELECT 'inf'::DOUBLE * 1;

-- inf * inf = inf
SELECT 'inf'::DOUBLE * 'inf'::DOUBLE;

-- inf * -inf = -inf
SELECT 'inf'::DOUBLE * '-inf'::DOUBLE;

SELECT 'inf'::DOUBLE / 1;

SELECT 1 / 'inf'::DOUBLE;

SELECT 'inf'::DOUBLE % 1;

SELECT 1 % 'inf'::DOUBLE;
