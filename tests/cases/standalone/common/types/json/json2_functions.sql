create table json2_function_table (
    ts timestamp time index,
    j json2,
    exponent double
) with (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

insert into json2_function_table values
    (1, '{"metrics":{"value":1,"enabled":true},"label":"one","tags":["a"]}', 2),
    (2, '{"metrics":{"value":-2.5,"enabled":false},"label":2,"tags":{"source":"b"}}', 2);

admin flush_table('json2_function_table');

insert into json2_function_table values
    (3, '{"metrics":{"value":"three","enabled":"yes"},"label":true,"tags":[1,2]}', 2),
    (4, '{"metrics":{"value":true},"label":{"name":"four"},"tags":"four"}', 2),
    (5, '{"metrics":{"value":{"nested":5}},"label":["five"],"tags":null}', 2),
    (6, '{"metrics":{"value":[6]},"label":null}', 2);

admin flush_table('json2_function_table');

admin compact_table('json2_function_table', 'swcs', '86400');

insert into json2_function_table values
    (7, '{"metrics":{"value":null},"label":"seven","extra":7}', 2),
    (8, '{"metrics":"opaque","label":8,"extra":false}', 2);

admin flush_table('json2_function_table');

insert into json2_function_table values
    (9, '{"other":9,"label":{"name":"nine"}}', 2),
    (10, '{"metrics":{"value":10},"label":"ten","extra":[10]}', 2);

-- Arrow casts Boolean values to Float64 as true = 1.0 and false = 0.0.
select abs(j.metrics.value) as abs_value from json2_function_table order by ts;

select power(j.label, 2) * 2 as scaled_label from json2_function_table order by ts;

select power(j.metrics.value, exponent) as powered_value from json2_function_table order by ts;

select coalesce(j.metrics.value, exponent) as value_or_exponent from json2_function_table order by ts;

select coalesce(
    j.metrics.value,
    j.label::double
) as mixed_value from json2_function_table order by ts;

select ts, j.label as label, signum(j.metrics.value) as value_sign
from json2_function_table
order by ts;

select ts, j.label as label
from json2_function_table
where iszero(j.metrics.value) = false
order by ts;

select count(j.metrics.value) as value_count from json2_function_table;

select sum(j.metrics.value) as value_sum from json2_function_table;

select lag(j.metrics.value) over (order by ts) as previous_value
from json2_function_table
order by ts;

drop table json2_function_table;
