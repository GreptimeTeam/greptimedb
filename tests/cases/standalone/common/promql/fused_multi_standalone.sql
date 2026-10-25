-- SQL semantic inputs accompany the direct integration test for native routing and multi-partition execution.
-- One insert and one flush keep the fixture small; this runner does not prove scan partitioning.
-- Four series, one sample per second. Instant selectors below evaluate at 4s, so their last values
-- are h1/left 5, h1/right 50, h2/left 10, h2/right 100.
CREATE TABLE fused_multi_standalone (
    host STRING,
    kind STRING,
    ts TIMESTAMP(3) TIME INDEX,
    val DOUBLE,
    PRIMARY KEY (host, kind),
);

INSERT INTO fused_multi_standalone VALUES
    ('h1', 'left', 0, 1), ('h1', 'right', 0, 10), ('h2', 'left', 0, 2), ('h2', 'right', 0, 20),
    ('h1', 'left', 1000, 2), ('h1', 'right', 1000, 20), ('h2', 'left', 1000, 4), ('h2', 'right', 1000, 40),
    ('h1', 'left', 2000, 3), ('h1', 'right', 2000, 30), ('h2', 'left', 2000, 6), ('h2', 'right', 2000, 60),
    ('h1', 'left', 3000, 4), ('h1', 'right', 3000, 40), ('h2', 'left', 3000, 8), ('h2', 'right', 3000, 80),
    ('h1', 'left', 4000, 5), ('h1', 'right', 4000, 50), ('h2', 'left', 4000, 10), ('h2', 'right', 4000, 100);
ADMIN FLUSH_TABLE('fused_multi_standalone');

-- Both occurrences are the same full-key selector, combined by default matching addition.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (4, 4, '1s') fused_multi_standalone{host!="",kind!=""} + fused_multi_standalone{host!="",kind!=""};

-- Subtraction is order sensitive: each series must be minus its own last value.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (4, 4, '1s') fused_multi_standalone{host!="",kind!=""} - (fused_multi_standalone{host!="",kind!=""} * 2);

-- A matcher-bearing range selector with @ and offset exercises scan bounds as well as labels.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (4, 4, '1s') rate(fused_multi_standalone{host="h1",kind="left"}[3s] @ 3 offset 1s);

-- The same two selectors, added with an explicit full-key join.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (4, 4, '1s') fused_multi_standalone{host!="",kind!=""} + on(host, kind) fused_multi_standalone{host!="",kind!=""};

-- Equality with bool keeps all four series and reports 1 for each.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (4, 4, '1s') fused_multi_standalone{host!="",kind!=""} == bool fused_multi_standalone{host!="",kind!=""};

-- Different matchers cannot share one fused scan; on(host) keeps host: h1 55 and h2 110.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (4, 4, '1s') fused_multi_standalone{kind="left"} + on(host) fused_multi_standalone{kind="right"};

-- group_right keeps the right-side kind: h1/left 10 and h1/right 55.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (4, 4, '1s') fused_multi_standalone{kind="left",host="h1"} + on(host) group_right fused_multi_standalone{host="h1"};

-- Raw-table anchor: @ 3 with offset 1s ends the 3s window at 2s, although evaluation is at 4s.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (4, 4, '1s') rate(fused_multi_standalone[3s] @ 3 offset 1s);

-- Incomplete matching identity via on() stays on fallback, even for one series.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (4, 4, '1s') fused_multi_standalone{host="h1",kind="left"} - on() (fused_multi_standalone{host="h1",kind="left"} * 2);

-- Aggregate and subquery forms stay on the legacy fallback source path.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (4, 4, '1s') sum(rate(fused_multi_standalone[5s]));
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (4, 4, '1s') rate(fused_multi_standalone[5s:1s]);

DROP TABLE fused_multi_standalone;
