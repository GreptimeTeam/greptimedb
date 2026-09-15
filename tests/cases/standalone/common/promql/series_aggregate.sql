CREATE TABLE series_aggregate (
    ts TIMESTAMP(3) TIME INDEX,
    "region" STRING,
    host STRING,
    val DOUBLE,
    PRIMARY KEY ("region", host),
);

INSERT INTO series_aggregate VALUES
    (0, 'west', 'a', 0), (1000, 'west', 'a', 1),
    (2000, 'west', 'a', 2), (4000, 'west', 'a', 4),
    (0, 'west', 'b', 10), (1000, 'west', 'b', 12),
    (2000, 'west', 'b', 1), (4000, 'west', 'b', 5),
    (0, 'east', 'c', 0), (1000, 'east', 'c', 3),
    (2000, 'east', 'c', 6), (10000, 'east', 'c', 30),
    (0, NULL, 'd', 0), (1000, NULL, 'd', 4),
    (2000, NULL, 'd', 8), (4000, NULL, 'd', 16),
    (0, 'empty', 'e', NULL), (1000, 'empty', 'e', NULL);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '1s') avg by (region) (rate(series_aggregate[4s]));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '1s') sum by (region) (rate(series_aggregate[4s]));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '1s') sum(rate(series_aggregate[4s]));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '1s') avg by (region) (rate(series_aggregate[4s] offset 1s));

-- Entirely absent groups must stay absent after the aggregate.
TQL EVAL (20, 25, '1s') avg by (region) (rate(series_aggregate[4s]));

DROP TABLE series_aggregate;
