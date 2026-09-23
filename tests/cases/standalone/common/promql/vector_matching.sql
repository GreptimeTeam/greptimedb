CREATE TABLE counter_metric (
  host STRING NULL,
  device STRING NULL,
  ts TIMESTAMP(3) TIME INDEX,
  greptime_value DOUBLE,
  PRIMARY KEY(host, device)
);

CREATE TABLE gauge_metric (
  host STRING NULL,
  device STRING NULL,
  ts TIMESTAMP(3) TIME INDEX,
  greptime_value DOUBLE,
  PRIMARY KEY(host, device)
);

INSERT INTO counter_metric VALUES
  ('host1', 'eth0', 0, 10), ('host1', 'eth1', 0, 20), ('host2', 'eth0', 0, 30);

INSERT INTO gauge_metric VALUES
  ('host1', 'eth0', 0, 2), ('host1', 'eth1', 0, 4), ('host2', 'eth0', 0, 5);

-- Matching on the whole tag set keeps it.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') counter_metric / gauge_metric;

-- `on(...)` reduces the result to the matching labels.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') counter_metric{device="eth0"} / on(host) gauge_metric{device="eth0"};

-- `ignoring(...)` drops the ignored labels, here from operands that don't even share them.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') counter_metric{device="eth0"} / ignoring(device) gauge_metric{device="eth1"};

-- `on()` matches everything against everything, so both sides must hold a single series.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') counter_metric{host="host1",device="eth0"} / on() gauge_metric{host="host1",device="eth0"};

-- A filtering comparison keeps the left sample values and the derived labels.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') counter_metric{device="eth0"} > on(host) gauge_metric{device="eth0"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') counter_metric{device="eth0"} > bool on(host) gauge_metric{device="eth0"};

-- `host1` carries two devices on both sides: the match group repeats on the one side.
TQL EVAL (0, 0, '5s') counter_metric / on(host) gauge_metric;

-- The one side is unique here, the many side is not, and no group modifier makes it explicit.
TQL EVAL (0, 0, '5s') counter_metric / on(host) gauge_metric{device="eth0"};

-- `group_left` takes the result labels from the many side.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') counter_metric / on(host) group_left gauge_metric{device="eth0"};

-- `group_right` swaps the sides.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') counter_metric{device="eth0"} / on(host) group_right gauge_metric;

-- `group_right` makes the left operand the one side, which `host1` breaks.
TQL EVAL (0, 0, '5s') counter_metric / on(host) group_right gauge_metric{device="eth0"};

-- `group_left(device)` copies `device` from the one side, so both `host1` rows end up with the
-- same labels.
TQL EVAL (0, 0, '5s') counter_metric / on(host) group_left(device) gauge_metric{device="eth0"};

-- An included label the one side doesn't carry is dropped from the result.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') counter_metric{device="eth0"} / on(host) group_left(device) sum by(host)(gauge_metric);

DROP TABLE counter_metric;

DROP TABLE gauge_metric;

-- A label can carry the name the cardinality check generates for its row count.
CREATE TABLE collide_left (
  host STRING NULL,
  __promql_match_group_count STRING NULL,
  ts TIMESTAMP(3) TIME INDEX,
  greptime_value DOUBLE,
  PRIMARY KEY(host, __promql_match_group_count)
);

CREATE TABLE collide_right (
  host STRING NULL,
  __promql_match_group_count STRING NULL,
  ts TIMESTAMP(3) TIME INDEX,
  greptime_value DOUBLE,
  PRIMARY KEY(host, __promql_match_group_count)
);

INSERT INTO collide_left VALUES ('host1', 'a', 0, 10);

INSERT INTO collide_right VALUES ('host1', 'b', 0, 2);

TQL EVAL (0, 0, '5s') collide_left / on(host) collide_right;

DROP TABLE collide_left;

DROP TABLE collide_right;

-- Partitioning on a column outside the match keys puts one match group in several regions. The
-- cardinality check counts the merged data, so it must still see the duplicate.
CREATE TABLE spread_left (
  host STRING NULL,
  device STRING NULL,
  ts TIMESTAMP(3) TIME INDEX,
  greptime_value DOUBLE,
  PRIMARY KEY(host, device)
)
PARTITION ON COLUMNS (device) (
  device < 'eth1',
  device >= 'eth1'
);

CREATE TABLE spread_right (
  host STRING NULL,
  ts TIMESTAMP(3) TIME INDEX,
  greptime_value DOUBLE,
  PRIMARY KEY(host)
);

INSERT INTO spread_left VALUES ('host1', 'eth0', 0, 10), ('host1', 'eth1', 0, 20);

INSERT INTO spread_right VALUES ('host1', 0, 2);

TQL EVAL (0, 0, '5s') spread_left / on(host) spread_right;

TQL EVAL (0, 0, '5s') spread_right / on(host) group_left spread_left;

DROP TABLE spread_left;

DROP TABLE spread_right;

-- An outer aggregate groups on the labels the operand has left, not on the ones the scan
-- underneath could still produce.
CREATE TABLE outer_agg_a (
  host STRING NULL,
  device STRING NULL,
  ts TIMESTAMP(3) TIME INDEX,
  greptime_value DOUBLE,
  PRIMARY KEY(host, device)
);

CREATE TABLE outer_agg_b (
  host STRING NULL,
  ts TIMESTAMP(3) TIME INDEX,
  greptime_value DOUBLE,
  PRIMARY KEY(host)
);

INSERT INTO outer_agg_a VALUES ('h1', 'd1', 0, 10), ('h2', 'd2', 0, 20);

INSERT INTO outer_agg_b VALUES ('h1', 0, 2), ('h2', 0, 4);

-- `on(host)` leaves only `host`, so `without(host)` groups everything together.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') sum without(host) (outer_agg_a / on(host) outer_agg_b);

-- `device` is not a label of the operand any more, so it groups everything together too.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') sum by(device) (outer_agg_a / on(host) outer_agg_b);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') sum by(host) (outer_agg_a / on(host) outer_agg_b);

-- A label the operand still has stays groupable, whether or not the scan projected it.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') sum without(device) (outer_agg_a);

DROP TABLE outer_agg_a;

DROP TABLE outer_agg_b;

-- Same over the metric engine, where the scan prunes tag columns in favour of `__tsid`.
CREATE TABLE outer_agg_physical (
  ts TIMESTAMP(3) TIME INDEX,
  greptime_value DOUBLE,
) ENGINE = metric WITH ("physical_metric_table" = "");

CREATE TABLE outer_agg_metric_a (
  host STRING NULL,
  device STRING NULL,
  ts TIMESTAMP(3) NOT NULL,
  greptime_value DOUBLE NULL,
  TIME INDEX (ts),
  PRIMARY KEY(host, device),
)
ENGINE = metric
WITH(
  on_physical_table = 'outer_agg_physical'
);

CREATE TABLE outer_agg_metric_b (
  host STRING NULL,
  ts TIMESTAMP(3) NOT NULL,
  greptime_value DOUBLE NULL,
  TIME INDEX (ts),
  PRIMARY KEY(host),
)
ENGINE = metric
WITH(
  on_physical_table = 'outer_agg_physical'
);

INSERT INTO outer_agg_metric_a (host, device, ts, greptime_value) VALUES
  ('h1', 'd1', 0, 10), ('h2', 'd2', 0, 20);

INSERT INTO outer_agg_metric_b (host, ts, greptime_value) VALUES
  ('h1', 0, 2), ('h2', 0, 4);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') sum without(host) (outer_agg_metric_a / on(host) outer_agg_metric_b);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') sum by(device) (outer_agg_metric_a / on(host) outer_agg_metric_b);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') sum by(host) (outer_agg_metric_a / on(host) outer_agg_metric_b);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') sum without(device) (outer_agg_metric_a);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '5s') sum by(host) (outer_agg_metric_a);

DROP TABLE outer_agg_metric_a;

DROP TABLE outer_agg_metric_b;

DROP TABLE outer_agg_physical;
