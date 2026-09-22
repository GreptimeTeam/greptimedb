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
