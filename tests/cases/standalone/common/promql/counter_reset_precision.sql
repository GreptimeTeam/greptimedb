CREATE TABLE counter_reset_precision (
    ts TIMESTAMP TIME INDEX,
    greptime_value DOUBLE,
    series STRING PRIMARY KEY
);

INSERT INTO counter_reset_precision VALUES
    (0,      10000000000000000.0, 'a'),
    (30000,  1.0,                 'a'),
    (60000,  0.0,                 'a'),
    (90000,  1.0,                 'a'),
    (120000, 2.0,                 'a');

-- Two adjacent windows over the same samples: (-30s, 90s] and (0s, 120s]. The second
-- one drops the 1e16 reset and keeps the 1 -> 0 one, so its correction is 1.0. Carrying
-- the first window's correction forward loses that, since 1e16 + 1.0 rounds to 1e16.
TQL EVAL (90, 120, '30s') increase(counter_reset_precision[2m]);

TQL EVAL (90, 120, '30s') rate(counter_reset_precision[2m]);

DROP TABLE counter_reset_precision;
