-- Reproduce https://github.com/GreptimeTeam/greptimedb/issues/8144
-- Binary comparison/arithmetic applied to a histogram_quantile() result.

create table http_request_duration_seconds_bucket (
    ts timestamp time index,
    le string,
    pod string,
    val double,
    primary key (pod, le),
);

insert into http_request_duration_seconds_bucket values
    (2900000, "0.01", "pod-a", 10),
    (2900000, "0.05", "pod-a", 20),
    (2900000, "0.1", "pod-a", 30),
    (2900000, "+Inf", "pod-a", 40),
    (3000000, "0.01", "pod-a", 20),
    (3000000, "0.05", "pod-a", 50),
    (3000000, "0.1", "pod-a", 80),
    (3000000, "+Inf", "pod-a", 100),
    (2900000, "0.01", "pod-b", 5),
    (2900000, "0.05", "pod-b", 8),
    (2900000, "0.1", "pod-b", 12),
    (2900000, "+Inf", "pod-b", 15),
    (3000000, "0.01", "pod-b", 10),
    (3000000, "0.05", "pod-b", 25),
    (3000000, "0.1", "pod-b", 45),
    (3000000, "+Inf", "pod-b", 60);

-- histogram_quantile alone
-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') histogram_quantile(0.5, sum by (le, pod) (rate(http_request_duration_seconds_bucket[5m])));

-- comparison filter
-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') histogram_quantile(0.5, sum by (le, pod) (rate(http_request_duration_seconds_bucket[5m]))) >= 0.02;

-- arithmetic
-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') histogram_quantile(0.5, sum by (le, pod) (rate(http_request_duration_seconds_bucket[5m]))) + 0;

-- bool modifier
-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') histogram_quantile(0.5, sum by (le, pod) (rate(http_request_duration_seconds_bucket[5m]))) >= bool 0.02;

-- subquery
-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') count_over_time((histogram_quantile(0.5, sum by (le, pod) (rate(http_request_duration_seconds_bucket[5m]))) >= 0.02)[10m:1m]);

drop table http_request_duration_seconds_bucket;
