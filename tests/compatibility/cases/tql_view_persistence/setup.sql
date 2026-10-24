-- Fixed-epoch samples: at the 1000s evaluation point, 600.001s is inside the explicit
-- 10m lookback but already outside the default 5m one, and 1000.000s is the point itself.
CREATE TABLE tql_view_source (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    series STRING PRIMARY KEY
);

INSERT INTO tql_view_source VALUES
    (600001, 41, 'beyond_default_lookback'),
    (1000000, 12, 'at_eval');

ADMIN FLUSH_TABLE('tql_view_source');

CREATE VIEW tql_view_instant AS TQL EVAL (1000, 1000, '1s', '10m') tql_view_source;

CREATE VIEW tql_view_range AS TQL EVAL (1000, 1000, '1s', '10m') max_over_time(tql_view_source[2s]);
