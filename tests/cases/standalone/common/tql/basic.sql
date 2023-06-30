CREATE TABLE test(i DOUBLE, j TIMESTAMP TIME INDEX, k STRING PRIMARY KEY);

-- insert two points at 1ms and one point at 2ms
INSERT INTO test VALUES (1, 1, "a"), (1, 1, "b"), (2, 2, "a");

-- SQLNESS SORT_RESULT 2 1
-- evaluate at 0s, 5s and 10s. No point at 0s.
TQL EVAL (0, 10, '5s') test;

-- the point at 1ms will be shadowed by the point at 2ms
TQL EVAL (0, 10, '5s') test{k="a"};

DROP TABLE test;
