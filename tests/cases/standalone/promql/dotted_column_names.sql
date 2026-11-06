-- https://github.com/GreptimeTeam/greptimedb/issues/9390
-- Tag, field and time index names containing dots or upper case letters must
-- be resolved as plain column names, not as `relation.column`. `Host` and `host`
-- hold different values, so reading one for the other changes the output.
CREATE TABLE "otel.m" (
    "ts.time" TIMESTAMP(3) TIME INDEX,
    "service.name" STRING,
    "Host" STRING,
    host STRING,
    "v.val" DOUBLE,
    PRIMARY KEY ("service.name", "Host", host)
) PARTITION ON COLUMNS ("service.name") (
    "service.name" < 'b',
    "service.name" >= 'b'
);

INSERT INTO "otel.m" VALUES
    (0,     'a', 'h1', 'x', 1),
    (0,     'a', 'h2', 'x', 2),
    (0,     'b', 'h1', 'y', 3),
    (5000,  'a', 'h1', 'x', 4),
    (5000,  'a', 'h2', 'x', 5),
    (5000,  'b', 'h1', 'y', 6),
    (10000, 'a', 'h1', 'x', 8),
    (10000, 'a', 'h2', 'x', 9),
    (10000, 'b', 'h1', 'y', 8);

CREATE TABLE "otel.h" (
    "ts.time" TIMESTAMP(3) TIME INDEX,
    "service.name" STRING,
    le STRING,
    "v.val" DOUBLE,
    PRIMARY KEY ("service.name", le)
);

INSERT INTO "otel.h" VALUES
    (0,     'a', '0.1',  1),
    (0,     'a', '1',    3),
    (0,     'a', '+Inf', 4),
    (10000, 'a', '0.1',  2),
    (10000, 'a', '1',    6),
    (10000, 'a', '+Inf', 10);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') {"otel.m"} and {"otel.m", "Host"="h1"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') {"otel.m"} and on("service.name") {"otel.m", "service.name"="b"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') {"otel.m"} and on(host) {"otel.m", host="y"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') {"otel.m"} unless {"otel.m", "Host"="h1"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') {"otel.m"} unless on("service.name") {"otel.m", "service.name"="b"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') {"otel.m"} or {"otel.m", "Host"="h1"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') count_values("v.name", {"otel.m"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') count_values by ("service.name") ("v.name", {"otel.m"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') topk(1, {"otel.m"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') bottomk by ("service.name") (1, {"otel.m"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') sum by ("service.name") ({"otel.m"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') sum by ("Host") ({"otel.m"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') sum by (host) ({"otel.m"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') sum without ("Host") ({"otel.m"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') -{"otel.m"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') 2 * {"otel.m"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') {"otel.m"} - 1;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') timestamp({"otel.m"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') minute({"otel.m"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (5, 10, '5s') sum by ("service.name") (increase({"otel.m"}[10s]));

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') scalar(sum({"otel.m"})) * {"otel.m", "Host"="h2"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') absent({"otel.m", "service.name"="c"});

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 10, '5s') histogram_quantile(0.5, {"otel.h"});

-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
TQL EXPLAIN (0, 10, '5s') {"otel.m"} and {"otel.m", "Host"="h1"};

-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
TQL EXPLAIN (0, 10, '5s') topk(1, {"otel.m"});

-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
TQL EXPLAIN (5, 10, '5s') sum by ("service.name") (increase({"otel.m"}[10s]));

-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
TQL EXPLAIN (0, 10, '5s') histogram_quantile(0.5, {"otel.h"});

DROP TABLE "otel.m";

DROP TABLE "otel.h";
