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
    (120000, 2.0,                 'a'),
    (150000, 3.0,                 'a'),
    (180000, 4.0,                 'a');

-- The counter resets twice, at 30s from 1e16 and at 60s from 1.0, and 1e16 + 1.0 is 1e16 in
-- f64. Windows are two minutes wide and step by one sample, so every row here depends on the
-- 1.0 reset surviving.
--
-- Summing the two corrections on their own drops the 1.0 and then cancels against the first
-- sample, so the 90s window reports no increase. Carrying that sum into the next window and
-- subtracting the 1e16 that left it reports half the increase, and the windows after that
-- subtract a reset the sum never held, so the correction turns negative and stays there for
-- the rest of the batch.
TQL EVAL (90, 180, '30s') increase(counter_reset_precision[2m]);

DROP TABLE counter_reset_precision;
