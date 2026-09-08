-- Correctness coverage for the instant-selector last-row optimization.
-- Each TQL EVAL below has distinct values so its chosen physical sample is visible.

-- The default instant lookback is (T - 300s, T]: exclude its lower bound,
-- include a sample just inside it and at T, and exclude a future sample.
CREATE TABLE instant_last_lookback (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    series STRING PRIMARY KEY
) ENGINE=mito;

INSERT INTO instant_last_lookback VALUES
    (700000, 10, 'lower_excluded'),
    (700001, 11, 'just_inside'),
    (1000000, 12, 'upper_included'),
    (1000000, 14, 'future_has_prior'),
    (1000001, 13, 'future_has_prior');
ADMIN FLUSH_TABLE('instant_last_lookback');

-- Expected pairs: just_inside=11, upper_included=12, future_has_prior=14.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1000, 1000, '1s') instant_last_lookback;

-- An empty table must remain an empty instant vector.
CREATE TABLE instant_last_empty (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    series STRING PRIMARY KEY
) ENGINE=mito;

-- Expected: no rows.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1000, 1000, '1s') instant_last_empty;

DROP TABLE instant_last_empty;
DROP TABLE instant_last_lookback;

-- Positive offset reads earlier data; negative offset reads later data.  Repeat
-- the earlier evaluation after a later one to catch a cached instant window.
CREATE TABLE instant_last_offset (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    series STRING PRIMARY KEY
) ENGINE=mito;

INSERT INTO instant_last_offset VALUES
    (940000, 21, 'offset'),
    (1000000, 22, 'offset'),
    (1060000, 23, 'offset');
ADMIN FLUSH_TABLE('instant_last_offset');

-- Expected value: 22.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1000, 1000, '1s') instant_last_offset;

-- Expected value: 21 (evaluation time shifted back 60s).
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1000, 1000, '1s') instant_last_offset offset 60s;

-- Expected value: 23 (evaluation time shifted forward 60s).
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1000, 1000, '1s') instant_last_offset offset -60s;

-- Expected value: 23.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1060, 1060, '1s') instant_last_offset;

-- Expected value: 22 again, not the later evaluation's value.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1000, 1000, '1s') instant_last_offset;

DROP TABLE instant_last_offset;

-- Put t1 and t2 in one SST. Deleting newest t2 must reveal t1 whether the
-- Delete is still in the memtable or has been flushed to a newer SST.
CREATE TABLE instant_last_delete (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    series STRING PRIMARY KEY
) ENGINE=mito;

INSERT INTO instant_last_delete VALUES
    (900000, 31, 'delete'),
    (1000000, 32, 'delete');
ADMIN FLUSH_TABLE('instant_last_delete');

DELETE FROM instant_last_delete WHERE series = 'delete' AND ts = 1000000;

-- Expected value: 31; the newest point is a memtable Delete.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1000, 1000, '1s') instant_last_delete;

ADMIN FLUSH_TABLE('instant_last_delete');

-- Expected value: 31; the Delete is now in an SST.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1000, 1000, '1s') instant_last_delete;

-- A same-timestamp memtable overwrite of t1 must win over the old SST value.
INSERT INTO instant_last_delete VALUES (900000, 33, 'delete');

-- Expected value: 33.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1000, 1000, '1s') instant_last_delete;

-- A newer memtable point must win, and retain that identity after its flush.
INSERT INTO instant_last_delete VALUES (1010000, 34, 'delete');

-- Expected value: 34.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1010, 1010, '1s') instant_last_delete;

ADMIN FLUSH_TABLE('instant_last_delete');

-- Expected value: 33 after flush; it overwrites t1 in an older SST.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1000, 1000, '1s') instant_last_delete;

-- Expected value: 34 after flush.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1010, 1010, '1s') instant_last_delete;

DROP TABLE instant_last_delete;

-- Ordinary IEEE NaN is a valid PromQL sample and must not be suppressed.
CREATE TABLE instant_last_nan (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    series STRING PRIMARY KEY
) ENGINE=mito;

INSERT INTO instant_last_nan VALUES (1000000, 'NaN'::DOUBLE, 'ordinary_nan');
ADMIN FLUSH_TABLE('instant_last_nan');

-- Expected value: NaN, with the ordinary_nan series present.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1000, 1000, '1s') instant_last_nan;

DROP TABLE instant_last_nan;

-- PromQL normalizes to milliseconds; these distinct raw timestamps collide at
-- 1s. LastRow must preserve the unhinted path's selected samples: 42 and 52.
CREATE TABLE instant_last_sec (
    ts TIMESTAMP(0) TIME INDEX,
    val DOUBLE,
    series STRING PRIMARY KEY
) ENGINE=mito;

INSERT INTO instant_last_sec VALUES
    (0, 61, 'sec'),
    (1, 62, 'sec'),
    (2, 63, 'sec');
ADMIN FLUSH_TABLE('instant_last_sec');

-- Expected value: 62 at the native seconds boundary.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') instant_last_sec;

CREATE TABLE instant_last_micro (
    ts TIMESTAMP(6) TIME INDEX,
    val DOUBLE,
    series STRING PRIMARY KEY
) ENGINE=mito;

INSERT INTO instant_last_micro VALUES
    (999999, 41, 'micro'),
    (1000000, 42, 'micro'),
    (1000001, 43, 'micro');
ADMIN FLUSH_TABLE('instant_last_micro');

-- Expected value: 42, matching the unhinted millisecond-normalized path.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') instant_last_micro;

CREATE TABLE instant_last_nano (
    ts TIMESTAMP(9) TIME INDEX,
    val DOUBLE,
    series STRING PRIMARY KEY
) ENGINE=mito;

INSERT INTO instant_last_nano VALUES
    (999999999, 51, 'nano'),
    (1000000000, 52, 'nano'),
    (1000000001, 53, 'nano');
ADMIN FLUSH_TABLE('instant_last_nano');

-- Expected value: 52, matching the unhinted millisecond-normalized path.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') instant_last_nano;

DROP TABLE instant_last_sec;
DROP TABLE instant_last_micro;
DROP TABLE instant_last_nano;

-- A field selector chooses the val column; it does not filter val. PromQL must
-- apply the comparison after selecting the latest eligible sample, rather than
-- falling back to an older sample that satisfies the comparison.
CREATE TABLE instant_last_field_filter (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    host STRING,
    instance STRING,
    PRIMARY KEY (host, instance)
) ENGINE=mito;

-- Keep the older matching sample in an SST and the latest non-matching sample
-- in the memtable. Both timestamps are eligible at 1s.
INSERT INTO instant_last_field_filter VALUES (900, 10, 'host-a', 'instance-a');
ADMIN FLUSH_TABLE('instant_last_field_filter');
INSERT INTO instant_last_field_filter VALUES (1000, 1, 'host-a', 'instance-a');

-- Expected: no rows. The selected latest value is 1, so it must not fall back
-- to the older value 10 merely because that value is greater than 5.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 1, '1s') instant_last_field_filter{__field__="val"} > 5;

-- A value matcher filters the scan before selecting a sample: the older 10
-- remains eligible, unlike the post-selection comparison above.
TQL EVAL (1, 1, '1s') instant_last_field_filter{val="10.0"};

-- SQL filters rows before aggregation, so the older matching value remains.
-- Expected value: 10.
SELECT last_value(val ORDER BY ts) FROM instant_last_field_filter WHERE val > 5;

DROP TABLE instant_last_field_filter;

-- Range evaluation is a control: range functions need their complete windows,
-- not a single last row.  At 10s and 20s, [11s] contains two samples.
CREATE TABLE instant_last_range_control (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    series STRING PRIMARY KEY
) ENGINE=mito;

INSERT INTO instant_last_range_control VALUES
    (0, 0, 'range'),
    (10000, 10, 'range'),
    (20000, 20, 'range');
ADMIN FLUSH_TABLE('instant_last_range_control');

-- Expected instant values at 0s, 10s, and 20s: 0, 10, and 20.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '10s') instant_last_range_control;

-- Expected last values at 0s, 10s, and 20s: 0, 10, and 20.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '10s') last_over_time(instant_last_range_control[11s]);

-- At 10s, rate is 10/11 because counter-zero extrapolation stops at 0s;
-- at 20s it is 1. The 0s window has only one sample.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 20, '10s') rate(instant_last_range_control[11s]);

-- Expected rate at 20s: 1 from both points in the [11s] window.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (20, 20, '1s') rate(instant_last_range_control[11s]);

DROP TABLE instant_last_range_control;
