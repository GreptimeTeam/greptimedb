-- Reproduction for JSON integer precision loss beyond u64::MAX.
-- See issue: TBD (link once published)
--
-- Case 1 (control): integer within u64 range is stored exactly.
-- Case 2 (bug): u64::MAX + 1 is silently coerced to f64.
-- Case 3 (bug): 30-digit integer is severely distorted.

create table t_json_precision (
    ts timestamp time index,
    j  json
);

-- Within u64 range: exact round-trip
insert into t_json_precision values (1, '{"n": 18446744073709551615}');

-- Beyond u64::MAX: currently accepted with Affected Rows: 1 and
-- silently stored as a f64 approximation (precision lost forever).
insert into t_json_precision values (2, '{"n": 18446744073709551616}');
insert into t_json_precision values (3, '{"n": 123456789012345678901234567890}');

select
    ts,
    json_get(j, '$.n') as n
from t_json_precision
order by ts;

drop table t_json_precision;