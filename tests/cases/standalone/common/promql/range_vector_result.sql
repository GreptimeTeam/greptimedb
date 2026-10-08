-- An instant query of a range-vector expression returns the selected samples
-- with their own timestamps. Expected results are from Prometheus 3.

create table range_vector_result (
    ts timestamp time index,
    val double,
    host string primary key
);

insert into range_vector_result values
    (0, 1.0, 'a'),
    (10000, 2.0, 'a'),
    (20000, 3.0, 'a'),
    (30000, 4.0, 'a'),
    (0, 10.0, 'b'),
    (20000, 30.0, 'b');

-- SQLNESS SORT_RESULT 3 1
tql eval (30, 30, '1s') range_vector_result[20s];

-- SQLNESS SORT_RESULT 3 1
tql eval (30, 30, '1s') range_vector_result[20s] offset 10s;

-- SQLNESS SORT_RESULT 3 1
tql eval (30, 30, '1s') range_vector_result[15s] @ 20;

-- SQLNESS SORT_RESULT 3 1
tql eval (30, 30, '1s') range_vector_result[30s:10s];

-- SQLNESS SORT_RESULT 3 1
tql eval (30, 30, '1s') range_vector_result[35s:10s] offset 5s;

tql eval (35, 35, '1s') range_vector_result[4s:10s];

tql eval (0, 30, '10s') range_vector_result[20s];

-- SQLNESS SORT_RESULT 3 1
tql eval (30, 30, '1s') range_vector_result[20s] as v;

-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
tql explain (30, 30, '1s') range_vector_result[20s];

tql eval (30, 30, '0') range_vector_result[30s:];

drop table range_vector_result;
