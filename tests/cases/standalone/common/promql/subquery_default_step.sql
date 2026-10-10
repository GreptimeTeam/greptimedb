-- A subquery without a step uses the evaluation interval (one minute by
-- default in Prometheus), not the query step. Expected results are from Prometheus 3.

create table subquery_default_step (
    ts timestamp time index,
    val double,
    host string primary key
);

insert into subquery_default_step values
    (0, 1.0, 'a'),
    (60000, 1.0, 'a'),
    (120000, 1.0, 'a'),
    (180000, 1.0, 'a'),
    (240000, 1.0, 'a'),
    (300000, 1.0, 'a');

tql eval (300, 300, '1s') count_over_time(subquery_default_step[5m:]);

tql eval (300, 300, '1s') count_over_time(subquery_default_step[5m:30s]);

-- The outer range query keeps its own step around the subquery's default step.
tql eval (300, 360, '15s') count_over_time(subquery_default_step[5m:]);

drop table subquery_default_step;
