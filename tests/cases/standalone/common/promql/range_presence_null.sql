-- NULL fields are missing samples, not zero-valued samples.
CREATE TABLE range_presence_null (
    ts TIMESTAMP(3) TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE,
);

INSERT INTO range_presence_null VALUES
    (0, 'a', 1.0),
    (1000, 'a', NULL),
    (2000, 'a', NULL),
    (3000, 'a', 4.0),
    (0, 'b', NULL),
    (1000, 'b', NULL),
    (2000, 'b', NULL),
    (3000, 'b', NULL);

-- At t=2 the trailing NULLs must not hide 1; at t=3 count must be 2.
-- At t=7 the left-open window is empty. Valid results must disappear.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (2, 7, '1s') count_over_time(range_presence_null{host="a"}[4s]);
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (2, 7, '1s') last_over_time(range_presence_null{host="a"}[4s]);
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (2, 7, '1s') present_over_time(range_presence_null{host="a"}[4s]);
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (2, 7, '1s') absent_over_time(range_presence_null{host="a"}[4s]);

-- All-NULL windows have no samples: only absent_over_time returns 1.
TQL EVAL (3, 3, '1s') count_over_time(range_presence_null{host="b"}[4s]);
TQL EVAL (3, 3, '1s') last_over_time(range_presence_null{host="b"}[4s]);
TQL EVAL (3, 3, '1s') present_over_time(range_presence_null{host="b"}[4s]);
TQL EVAL (3, 3, '1s') absent_over_time(range_presence_null{host="b"}[4s]);

DROP TABLE range_presence_null;
