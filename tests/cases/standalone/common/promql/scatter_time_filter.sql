-- More than 128 evaluation points must survive the expression-depth guard.
-- Sparse samples exercise excluded lower bounds, included upper bounds and gaps.
CREATE TABLE scatter_time_filter (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE
);

INSERT INTO scatter_time_filter VALUES
    (3600000, 1), (3600001, 2), (7199999, 3), (7200000, 4),
    (7200001, 5), (10800000, 6), (2592000000, 7),
    (2592000001, 8);

TQL EVAL (0, 2593800, '2h') sum(sum_over_time(scatter_time_filter[1h]));
TQL EVAL (0, 2593800, '2h') sum(increase(scatter_time_filter[1h]));
TQL EVAL (0, 2593800, '2h') sum(sum_over_time(scatter_time_filter[2h]));
TQL EVAL (0, 2593800, '2h') sum(sum_over_time(scatter_time_filter[3h]));

DROP TABLE scatter_time_filter;
