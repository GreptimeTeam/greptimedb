-- NaN and rounding follow Prometheus (IEEE 754) rather than SQL ordering.
-- Expected results are from Prometheus 3.

create table float_semantics (
    ts timestamp time index,
    val double,
    host string primary key
);

insert into float_semantics values
    (0, 1.0, 'a'),
    (0, 2.0, 'b'),
    (0, 'NaN'::double, 'c'),
    (0, -2.5, 'd');

tql eval (0, 0, '1s') max(float_semantics);

tql eval (0, 0, '1s') min(float_semantics);

tql eval (0, 0, '1s') topk(1, float_semantics);

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 0, '1s') float_semantics <= NaN;

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 0, '1s') float_semantics != bool NaN;

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 0, '1s') round(float_semantics);

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 0, '1s') sqrt(float_semantics);

drop table float_semantics;
