CREATE TABLE rate_fusion (
    ts TIMESTAMP(3) TIME INDEX,
    val DOUBLE,
    dc STRING,
    instance STRING,
    PRIMARY KEY (dc, instance),
) WITH ('append_mode' = 'true');

INSERT INTO rate_fusion VALUES
    (0, 0.0, 'shared', 'a'),
    (60000, 60.0, 'shared', 'a'),
    (120000, 120.0, 'shared', 'a'),
    (180000, 180.0, 'shared', 'a'),
    (240000, 240.0, 'shared', 'a'),
    (300000, 300.0, 'shared', 'a'),
    (0, 0.0, 'shared', 'b'),
    (60000, 30.0, 'shared', 'b'),
    (120000, 60.0, 'shared', 'b'),
    (180000, 90.0, 'shared', 'b'),
    (240000, 120.0, 'shared', 'b'),
    (300000, 150.0, 'shared', 'b'),
    (0, 10.0, 'other', 'c'),
    (60000, 70.0, 'other', 'c'),
    (120000, NULL, 'other', 'c'),
    (180000, 100.0, 'other', 'c'),
    (240000, 5.0, 'other', 'c'),
    (300000, 65.0, 'other', 'c'),
    (0, NULL, 'other', 'd'),
    (60000, NULL, 'other', 'd'),
    (120000, NULL, 'other', 'd'),
    (180000, NULL, 'other', 'd'),
    (240000, 42.0, 'other', 'd'),
    (300000, NULL, 'other', 'd');

-- Two nonzero linear rates share one dc; the reset counter and sparse/all-NULL series
-- exercise reset handling, missing windows, and label aggregation in the other dc.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (120, 300, '60s') sum by (dc) (rate(rate_fusion[2m]));

-- Instant form of the same grouped aggregation at the final range step.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (300, 300, '60s') sum by (dc) (rate(rate_fusion[2m]));

-- Offset and a different aggregator must not take the narrow fused path.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (120, 300, '60s') sum by (dc) (rate(rate_fusion[2m] offset 1m));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (120, 300, '60s') min by (dc) (rate(rate_fusion[2m]));

DROP TABLE rate_fusion;
