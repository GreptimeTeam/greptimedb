-- Regression test for JSON integer precision loss beyond u64::MAX.
-- Integers outside i64/u64 range must be rejected with an explicit
-- error instead of being silently coerced to f64.
-- See PR: fix(datatypes): reject JSON numbers beyond u64::MAX

create table t_json_precision (
    ts timestamp time index,
    j  json
);

-- Within u64 range: exact round-trip
insert into t_json_precision values (1, '{"n": 18446744073709551615}');

-- i64::MIN also accepted exactly
insert into t_json_precision values (4, '{"n": -9223372036854775808}');

-- Floats and exponents are unaffected
insert into t_json_precision values (5, '{"n": 1.5}');

-- Beyond u64::MAX: rejected with an explicit error
insert into t_json_precision values (2, '{"n": 18446744073709551616}');

-- 30-digit integer: rejected
insert into t_json_precision values (3, '{"n": 123456789012345678901234567890}');

-- Negative beyond i64::MIN: rejected
insert into t_json_precision values (6, '{"n": -9223372036854775809}');

select
    ts,
    json_get(j, '$.n') as n
from t_json_precision
order by ts;

drop table t_json_precision;