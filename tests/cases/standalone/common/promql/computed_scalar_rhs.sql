-- A vector combined with a computed scalar on the right keeps the vector's
-- labels, like a scalar on the left does. Expected results are from Prometheus 3.

create table computed_scalar_lhs (
    ts timestamp time index,
    val double,
    host string primary key
);

create table computed_scalar_rhs (
    ts timestamp time index,
    val double,
    host string primary key
);

insert into computed_scalar_lhs values
    (0, 1.0, 'a'),
    (0, 2.0, 'b');

insert into computed_scalar_rhs values
    (0, 10.0, 'x');

-- SQLNESS SORT_RESULT 3 1
tql eval (60, 60, '1s') computed_scalar_lhs + time();

-- SQLNESS SORT_RESULT 3 1
tql eval (60, 60, '1s') time() + computed_scalar_lhs;

-- SQLNESS SORT_RESULT 3 1
tql eval (60, 60, '1s') computed_scalar_lhs * scalar(computed_scalar_rhs);

-- SQLNESS SORT_RESULT 3 1
tql eval (60, 60, '1s') computed_scalar_lhs < bool (time() * 2);

-- SQLNESS SORT_RESULT 3 1
tql eval (60, 60, '1s') (time() >= computed_scalar_lhs) ^ scalar(computed_scalar_rhs);

-- SQLNESS SORT_RESULT 3 1
tql eval (60, 60, '1s') scalar(computed_scalar_rhs) * (time() >= computed_scalar_lhs);

drop table computed_scalar_lhs;

drop table computed_scalar_rhs;
