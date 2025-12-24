-- Referenced from https://github.com/prometheus/prometheus/blob/a48d348811619ba4e8eae9c3eaae4857749a1578/promql/promqltest/testdata/functions.test#L172-L185

create table calculate_rate_offset_total (
    ts timestamp time index,
    val double,
    x string primary key
);

insert into calculate_rate_offset_total values
    (0, 0.0,    'a'),
    (300000, 10.0, 'a'),
    (600000, 20.0, 'a'),
    (900000, 30.0, 'a'),
    (1200000, 40.0, 'a'),
    (1500000, 50.0, 'a'),
    (1800000, 60.0, 'a'),
    (2100000, 70.0, 'a'),
    (2400000, 80.0, 'a'),
    (2700000, 90.0, 'a'),
    (3000000, 100.0, 'a'),
    (0, 0.0,    'b'),
    (300000, 20.0, 'b'),
    (600000, 40.0, 'b'),
    (900000, 60.0, 'b'),
    (1200000, 80.0, 'b'),
    (1500000, 100.0, 'b'),
    (1800000, 120.0, 'b'),
    (2100000, 140.0, 'b'),
    (2400000, 160.0, 'b'),
    (2700000, 180.0, 'b'),
    (3000000, 200.0, 'b');

-- SQLNESS SORT_RESULT 3 1
tql eval (1500, 1500, '1s') calculate_rate_offset_total;

-- SQLNESS SORT_RESULT 3 1
tql eval (1500, 1500, '1s') calculate_rate_offset_total offset 10m;

-- SQLNESS SORT_RESULT 3 1
tql eval (1500, 1500, '1s') calculate_rate_offset_total offset -10m;

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 0, '1s') calculate_rate_offset_total offset 10m;

-- SQLNESS SORT_RESULT 3 1
tql eval (0, 0, '1s') calculate_rate_offset_total offset -10m;

-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') calculate_rate_offset_total offset 10m;

-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') calculate_rate_offset_total offset -10m;

-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') rate(calculate_rate_window_total[10m]);

-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') rate(calculate_rate_offset_total[10m] offset 5m);

drop table calculate_rate_offset_total;
