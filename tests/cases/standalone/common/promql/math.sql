CREATE TABLE angles (
  ts timestamp(3) time index,
  unit STRING PRIMARY KEY,
  val DOUBLE,
);

INSERT INTO TABLE angles VALUES
    (0,     'radians', 0),
    (0,     'degrees', 0),
    (5000,  'radians', 1.5708),  -- π/2
    (5000,  'degrees', 90),
    (10000, 'radians', 3.1416),  -- π
    (10000, 'degrees', 180),
    (15000, 'radians', 4.7124),  -- 3π/2
    (15000, 'degrees', 270),
    (20000, 'radians', 6.2832),  -- 2π
    (20000, 'degrees', 360),
    (25000, 'positive', 42.5),
    (25000, 'negative', -17.3),
    (30000, 'zero', 0),
    (30000, 'small_pos', 0.001),
    (35000, 'small_neg', -0.001);

-- FIXME: test pi() function (returns constant π ≈ 3.141592653589793)

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '5s') pi();

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '5s') pi() + 1;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '5s') angles{unit="radians"} * pi();

-- Test rad() function (converts degrees to radians)

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '5s') rad(angles{unit="degrees"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '5s') rad(angles);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '5s') scalar(rad(angles{unit="degrees"}));

-- Test deg() function (converts radians to degrees)

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '5s') deg(angles{unit="radians"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '5s') deg(angles);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '5s') scalar(deg(angles{unit="radians"}));

-- Test conversions with pi()

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '5s') rad(deg(pi()));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '5s') deg(rad(pi()));

-- Test with arithmetic operations

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '5s') deg(angles{unit="radians"}) + rad(angles{unit="degrees"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '5s') deg(pi() * 2);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '5s') rad(180 * angles{unit="radians"});

-- Test sgn() function (returns sign of values: -1, 0, or 1)

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 35, '5s') sgn(angles{unit="positive"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 35, '5s') sgn(angles{unit="negative"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 35, '5s') sgn(angles{unit="zero"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 35, '5s') sgn(angles{unit="small_pos"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 35, '5s') sgn(angles{unit="small_neg"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (25, 35, '5s') sgn(angles);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (25, 35, '5s') scalar(sgn(angles{unit="positive"}));

-- FIXME: test sgn with arithmetic operations
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (25, 35, '5s') sgn(angles - 42.5);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 35, '5s') sgn(angles{unit="radians"} - pi());

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (25, 35, '5s') sgn(angles) * angles;

Drop table angles;
