-- The last two 'sub_ms' samples are 400ns apart and share one millisecond, which
-- irate and idelta cannot resolve, so they return no point instead of inf.
-- The 'one_ms' samples are 1ms apart and still produce a rate.
CREATE TABLE sub_ms_interval (
    job STRING,
    `value` DOUBLE,
    ts TIMESTAMP(9) TIME INDEX,
    PRIMARY KEY(job)
);

INSERT INTO sub_ms_interval VALUES
    ('sub_ms', 999, 999999900),
    ('sub_ms', 10, 999999100),
    ('sub_ms', 13, 999999500),
    ('one_ms', 10, 998000000),
    ('one_ms', 13, 999000000);

SELECT * FROM sub_ms_interval ORDER BY job, ts;

TQL EVAL (1, 1, '1s') irate(sub_ms_interval[5s]);

TQL EVAL (1, 1, '1s') idelta(sub_ms_interval[5s]);

ADMIN FLUSH_TABLE('sub_ms_interval');

TQL EVAL (1, 1, '1s') irate(sub_ms_interval[5s]);

TQL EVAL (1, 1, '1s') idelta(sub_ms_interval[5s]);

DROP TABLE sub_ms_interval;
