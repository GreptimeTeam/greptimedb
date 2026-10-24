-- Instant evaluation at 1000s honours the 10m lookback stored in the persisted view
-- plan: 41 is 400s+ old but still selected, 12 sits exactly on the grid point.
SELECT ts, series, val FROM tql_view_instant ORDER BY series;

SHOW CREATE VIEW tql_view_instant;

-- The range view binds its own 2s window, so only the sample at the evaluation point
-- survives; the column names come from the plan persisted by the old binary.
SELECT * FROM tql_view_range;
