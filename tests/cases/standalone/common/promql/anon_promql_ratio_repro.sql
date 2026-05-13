CREATE TABLE phy (
    t TIMESTAMP TIME INDEX,
    v DOUBLE
) ENGINE=metric WITH ("physical_metric_table" = "");

CREATE TABLE metric_a (
    l1 STRING NULL,
    l2 STRING NULL,
    l3 STRING NULL,
    l4 STRING NULL,
    l5 STRING NULL,
    t TIMESTAMP NOT NULL,
    v DOUBLE NULL,
    TIME INDEX (t),
    PRIMARY KEY (l1, l2, l3, l4, l5)
) ENGINE=metric WITH (on_physical_table = 'phy');

CREATE TABLE metric_b (
    l6 STRING NULL,
    l1 STRING NULL,
    l2 STRING NULL,
    l3 STRING NULL,
    l4 STRING NULL,
    t TIMESTAMP NOT NULL,
    v DOUBLE NULL,
    TIME INDEX (t),
    PRIMARY KEY (l6, l1, l2, l3, l4)
) ENGINE=metric WITH (on_physical_table = 'phy');

INSERT INTO metric_a (l1, l2, l3, l4, l5, t, v) VALUES
    ('v1', 'v2', 'v3', 'v4a', 'v5a', 1, 0),
    ('v1', 'v2', 'v3', 'v4a', 'v5a', 180000, 120),
    ('v1', 'v2', 'v3', 'v4a', 'v5a', 360000, 240),
    ('v1', 'v2', 'v3', 'v4a', 'v5b', 1, 0),
    ('v1', 'v2', 'v3', 'v4a', 'v5b', 180000, 30),
    ('v1', 'v2', 'v3', 'v4a', 'v5b', 360000, 60),
    ('v1', 'v2', 'v3-b', 'v4b', 'v5c', 1, 0),
    ('v1', 'v2', 'v3-b', 'v4b', 'v5c', 180000, 60),
    ('v1', 'v2', 'v3-b', 'v4b', 'v5c', 360000, 120);

INSERT INTO metric_b (l6, l1, l2, l3, l4, t, v) VALUES
    ('v6', 'v1', 'v2', 'v3', 'v4a', 1, 1),
    ('v6', 'v1', 'v2', 'v3', 'v4a', 180000, 1),
    ('v6', 'v1', 'v2', 'v3', 'v4a', 360000, 1),
    ('v6', 'v1', 'v2', 'v3-b', 'v4b', 1, 2),
    ('v6', 'v1', 'v2', 'v3-b', 'v4b', 180000, 2),
    ('v6', 'v1', 'v2', 'v3-b', 'v4b', 360000, 2);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (180, 360, '180s') count(((rate(metric_a{l1="v1",l2="v2",l3=~"v3(|-a|-b)"}[3m]) / on(l3,l4) group_left metric_b{l6="v6",l1="v1",l2="v2",l3=~"v3(|-a|-b)"}) > 0.50));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (180, 360, '180s') count(rate(metric_a{l1="v1",l2="v2",l3=~"v3(|-a|-b)"}[3m]));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (180, 360, '180s') count(rate(metric_a{l1="v1",l2="v2",l3=~"v3(|-a|-b)"}[3m])) / 2;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (180, 360, '180s') (count(((rate(metric_a{l1="v1",l2="v2",l3=~"v3(|-a|-b)"}[3m]) / on(l3,l4) group_left metric_b{l6="v6",l1="v1",l2="v2",l3=~"v3(|-a|-b)"}) > 0.50)) / count(rate(metric_a{l1="v1",l2="v2",l3=~"v3(|-a|-b)"}[3m]))) * 100;

DROP TABLE metric_a;
DROP TABLE metric_b;
DROP TABLE phy;
