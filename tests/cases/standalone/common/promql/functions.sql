CREATE TABLE
    prom_series (
        ts TIMESTAMP TIME INDEX,
        val DOUBLE,
        host STRING PRIMARY KEY
    );

INSERT INTO
    prom_series (ts, val, host)
VALUES
    (0, 0.0, 'p'),
    (300, 10.0, 'p'),
    (600, 20.0, 'p'),
    (900, 30.0, 'p'),
    (1200, 40.0, 'p'),
    (1500, 0.0, 'p'),
    (1800, 10.0, 'p'),
    (2100, 20.0, 'p'),
    (2400, 30.0, 'p'),
    (2700, 40.0, 'p'),
    (3000, 50.0, 'p');

-- predict_linear
-- SQLNESS SORT_RESULT 3 1
tql eval (3, 3, '1s') predict_linear(prom_series[3s], 0);

-- SQLNESS SORT_RESULT 3 1
tql eval (3, 3, '1s') predict_linear(prom_series[3s], 3);

-- SQLNESS SORT_RESULT 3 1
tql eval (3, 3, '1s') predict_linear(prom_series[3s], 40 + 2);

-- double_exponential_smoothing
-- SQLNESS SORT_RESULT 3 1
tql eval (10, 10, '1s') double_exponential_smoothing(prom_series[10s], 0.4 + 0.1, 0.1);

-- holt_winters (backward compatibility)
-- SQLNESS SORT_RESULT 3 1
tql eval (10, 10, '1s') holt_winters(prom_series[10s], 0.4 + 0.1, 0.1);

DROP TABLE prom_series;

CREATE TABLE
    prom_series_q (
        ts TIMESTAMP TIME INDEX,
        val DOUBLE,
        host STRING PRIMARY KEY
    );

INSERT INTO
    prom_series_q (ts, val, host)
VALUES
    (1000, 123.45, 'q'),
    (2000, 234.567, 'q'),
    (3000, 345.678, 'q'),
    (4000, 456.789, 'q');

-- quantile_over_time
-- SQLNESS SORT_RESULT 3 1
tql eval (4, 4, '1s') quantile_over_time(0.2 + 0.05, prom_series_q[4s]);

-- SQLNESS SORT_RESULT 3 1
tql eval (4, 4, '1s') quantile_over_time(0.4 + 0.1, prom_series_q[4s]);

-- round
-- SQLNESS SORT_RESULT 3 1
tql eval (3, 3, '1s') round(prom_series_q);

-- SQLNESS SORT_RESULT 3 1
tql eval (1, 4, '1s') round(prom_series_q, 0.01);

-- SQLNESS SORT_RESULT 3 1
tql eval (1, 4, '1s') round(prom_series_q, 0.05 + 0.05);

-- SQLNESS SORT_RESULT 3 1
tql eval (1, 4, '1s') round(prom_series_q, 10.0);

-- SQLNESS SORT_RESULT 3 1
tql eval (1, 4, '1s') round(prom_series_q, 100.0 + 3.0);

-- SQLNESS SORT_RESULT 3 1
tql eval (1, 4, '1s') round(prom_series_q, - 3.0 + 13.0);

DROP TABLE prom_series_q;
