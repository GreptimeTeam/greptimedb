-- description: Timestamp display precision over the MySQL protocol should
-- respect the column's storage unit (issue #8227):
--   - TIMESTAMP(3) must not pad to 6 fractional digits
--   - TIMESTAMP(9) must not truncate to 6 fractional digits

-- SQLNESS PROTOCOL MYSQL
CREATE TABLE ts_precision_display (
    ts_s  TIMESTAMP(0),
    ts_ms TIMESTAMP(3),
    ts_us TIMESTAMP(6),
    ts_ns TIMESTAMP(9),
    v     INT,
    TIME INDEX (ts_ms),
    PRIMARY KEY (v)
);

-- SQLNESS PROTOCOL MYSQL
INSERT INTO ts_precision_display VALUES (
    '2026-06-02 03:50:00',
    '2026-06-02 03:50:00.195',
    '2026-06-02 03:50:00.195123',
    '2026-06-02 03:50:00.195123456',
    1
);

-- subsecond digits render in groups of 3: values representable in ms show
-- 3 digits regardless of storage unit (.100, not .100000)
-- SQLNESS PROTOCOL MYSQL
INSERT INTO ts_precision_display VALUES (
    '2026-06-02 03:50:01',
    '2026-06-02 03:50:01.100',
    '2026-06-02 03:50:01.100000',
    '2026-06-02 03:50:01.100000000',
    2
);

-- zero subseconds render with no fractional part for every precision
-- SQLNESS PROTOCOL MYSQL
INSERT INTO ts_precision_display VALUES (
    '2026-06-02 03:50:02',
    '2026-06-02 03:50:02',
    '2026-06-02 03:50:02',
    '2026-06-02 03:50:02',
    3
);

-- SQLNESS PROTOCOL MYSQL
SELECT * FROM ts_precision_display ORDER BY v;

-- SQLNESS PROTOCOL MYSQL
DROP TABLE ts_precision_display;
