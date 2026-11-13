CREATE TABLE host (
  ts timestamp(3) time index,
  host STRING PRIMARY KEY,
  val BIGINT,
);

INSERT INTO TABLE host VALUES
    (0,     'host1', 0),
    (5000,  'host1', null),
    (10000, 'host1', 1),
    (15000, 'host1', null),
    (20000, 'host1', 2),
    (0,     'host2', 3),
    (5000,  'host2', null),
    (10000, 'host2', 4),
    (15000, 'host2', null),
    (20000, 'host2', 5);

-- Test long UNION ALL chains, which plan to deeply nested unions

-- SQLNESS TEMPLATE
SELECT count(*) FROM ({% for i in range(256) %}{% if not loop.first %} UNION ALL {% endif %}SELECT 1 AS a{% endfor %});

-- Only the first branch returns rows. 256 branches that all scan `host` can
-- take over 30s in a debug build, which records a slow query and creates the
-- `greptime_private.slow_queries` table.
-- SQLNESS TEMPLATE
SELECT ts, host, min(val) RANGE '5s' FROM (SELECT ts, host, val FROM host{% for i in range(255) %} UNION ALL SELECT ts, host, val FROM host WHERE false{% endfor %}) ALIGN '5s' BY (host) ORDER BY host, ts;

DROP TABLE host;
