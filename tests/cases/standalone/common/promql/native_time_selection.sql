-- Regression coverage for instant and range selection on native microsecond and
-- nanosecond time indexes.

CREATE TABLE native_time_us (
    ts TIMESTAMP(6) TIME INDEX,
    series STRING PRIMARY KEY,
    val DOUBLE,
);

INSERT INTO native_time_us VALUES
    (1000001, 'future', 101),
    (1000000, 'exact', 201),
    (1000001, 'exact', 202),
    (-299000000, 'lowerbound', 301),
    (-298999999, 'lowerplus', 302),
    (1000000, 'positive_lowerbound', 701),
    (1000001, 'positive_lowerplus', 702),
    (1000001, 'multi', 401),
    (-1000000, 'offset', 501),
    (0, 'offset', 502),
    (1000000, 'offset', 503),
    (999999, 'past', 602),
    (999001, 'past', 601),
    (0, 'window', 1),
    (1, 'window', 2),
    (2, 'window', 5),
    (1000000, 'window', 3),
    (1000001, 'window', 4);

-- Future-only selection is empty before flushing, exercising the memtable path.
TQL EVAL (1, 1, '1s', '300s') native_time_us{series="future"};

ADMIN FLUSH_TABLE('native_time_us');

-- At 1s, selection keeps an exact native timestamp.
TQL EVAL (1, 1, '1s', '300s') native_time_us{series="exact"};
TQL EVAL (1, 1, '1s', '300s') timestamp(native_time_us{series="future"});
TQL EVAL (1, 1, '1s', '300s') timestamp(native_time_us{series="exact"});

-- Instant lookback bounds are exclusive: these return only 302 and 702.
TQL EVAL (1, 1, '1s', '300s') native_time_us{series=~"lower.*"};
TQL EVAL (301, 301, '1s', '300s') native_time_us{series=~"positive_lower.*"};

-- The sub-millisecond point belongs only to the 2s evaluation step.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 2, '1s', '300s') native_time_us{series="multi"};

-- The latest native timestamp below 1s is retained even when inserts are unordered.
TQL EVAL (1, 1, '1s', '300s') native_time_us{series="past"};

-- Offsets select native timestamps, including stored negative time.
TQL EVAL (0, 0, '1s', '300s') native_time_us{series="offset"};
TQL EVAL (0, 0, '1s', '300s') native_time_us{series="offset"} offset 1s;
TQL EVAL (0, 0, '1s', '300s') native_time_us{series="offset"} offset -1s;

-- [1s] at 1s excludes 0 and 1s+tick, retaining 0+tick, 0+2ticks, and 1s.
TQL EVAL (1, 1, '1s', '300s') count_over_time(native_time_us{series="window"}[1s]);
TQL EVAL (1, 1, '1s', '300s') sum_over_time(native_time_us{series="window"}[1s]);
TQL EVAL (1, 1, '1s', '300s') last_over_time(native_time_us{series="window"}[1s]);

-- The inner selector consumes native time; the subquery consumes ms evaluations.
TQL EVAL (1, 1, '1s') last_over_time((native_time_us{series="exact"})[1s:1s]);

DROP TABLE native_time_us;

CREATE TABLE native_time_ns (
    ts TIMESTAMP(9) TIME INDEX,
    series STRING PRIMARY KEY,
    val DOUBLE,
);

INSERT INTO native_time_ns VALUES
    (1000000001, 'future', 101),
    (1000000000, 'exact', 201),
    (1000000001, 'exact', 202),
    (-299000000000, 'lowerbound', 301),
    (-298999999999, 'lowerplus', 302),
    (1000000000, 'positive_lowerbound', 701),
    (1000000001, 'positive_lowerplus', 702),
    (1000000001, 'multi', 401),
    (-1000000000, 'offset', 501),
    (0, 'offset', 502),
    (1000000000, 'offset', 503),
    (999999000, 'past', 602),
    (999001000, 'past', 601),
    (0, 'window', 1),
    (1, 'window', 2),
    (2, 'window', 5),
    (1000000000, 'window', 3),
    (1000000001, 'window', 4);

-- Future-only selection is empty before flushing, exercising the memtable path.
TQL EVAL (1, 1, '1s', '300s') native_time_ns{series="future"};

ADMIN FLUSH_TABLE('native_time_ns');

-- At 1s, selection keeps an exact native timestamp.
TQL EVAL (1, 1, '1s', '300s') native_time_ns{series="exact"};
TQL EVAL (1, 1, '1s', '300s') timestamp(native_time_ns{series="future"});
TQL EVAL (1, 1, '1s', '300s') timestamp(native_time_ns{series="exact"});

-- Instant lookback bounds are exclusive: these return only 302 and 702.
TQL EVAL (1, 1, '1s', '300s') native_time_ns{series=~"lower.*"};
TQL EVAL (301, 301, '1s', '300s') native_time_ns{series=~"positive_lower.*"};

-- The sub-millisecond point belongs only to the 2s evaluation step.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (1, 2, '1s', '300s') native_time_ns{series="multi"};

-- The latest native timestamp below 1s is retained even when inserts are unordered.
TQL EVAL (1, 1, '1s', '300s') native_time_ns{series="past"};

-- Offsets select native timestamps, including stored negative time.
TQL EVAL (0, 0, '1s', '300s') native_time_ns{series="offset"};
TQL EVAL (0, 0, '1s', '300s') native_time_ns{series="offset"} offset 1s;
TQL EVAL (0, 0, '1s', '300s') native_time_ns{series="offset"} offset -1s;

-- [1s] at 1s excludes 0 and 1s+tick, retaining 0+tick, 0+2ticks, and 1s.
TQL EVAL (1, 1, '1s', '300s') count_over_time(native_time_ns{series="window"}[1s]);
TQL EVAL (1, 1, '1s', '300s') sum_over_time(native_time_ns{series="window"}[1s]);
TQL EVAL (1, 1, '1s', '300s') last_over_time(native_time_ns{series="window"}[1s]);

-- The inner selector consumes native time; the subquery consumes ms evaluations.
TQL EVAL (1, 1, '1s') last_over_time((native_time_ns{series="exact"})[1s:1s]);

DROP TABLE native_time_ns;

-- Second precision is promoted before applying fractional-second offsets.
CREATE TABLE native_time_sec (ts TIMESTAMP(0) TIME INDEX, val DOUBLE);
INSERT INTO native_time_sec VALUES (0, 10), (1, 11), (2, 12);
TQL EVAL (1, 1, '1s', '1s') native_time_sec offset 500ms;
TQL EVAL (1, 1, '1s', '1s') native_time_sec offset -500ms;
DROP TABLE native_time_sec;
