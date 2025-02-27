create table metric_total (
    ts timestamp time index,
    val double,
);

insert into metric_total values
    (0, 1),
    (10000, 2);

tql eval (10, 10, '1s') sum_over_time(metric_total[50s:10s]);

tql eval (10, 10, '1s') sum_over_time(metric_total[50s:5s]);

tql eval (300, 300, '1s') sum_over_time(metric_total[50s:10s]);

tql eval (359, 359, '1s') sum_over_time(metric_total[60s:10s]);

tql eval (10, 10, '1s') rate(metric_total[20s:10s]);

tql eval (20, 20, '1s') rate(metric_total[20s:5s]);

drop table metric_total;
