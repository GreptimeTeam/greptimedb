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

-- SQLNESS TEMPLATE
SELECT ts, host, min(val) RANGE '5s' FROM ({% for i in range(256) %}{% if not loop.first %} UNION ALL {% endif %}SELECT ts, host, val FROM host{% endfor %}) ALIGN '5s' BY (host) ORDER BY host, ts;

DROP TABLE host;
