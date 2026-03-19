CREATE TABLE last_row_selector_cache_filter (
  host STRING,
  cpu DOUBLE,
  ts TIMESTAMP TIME INDEX,
  PRIMARY KEY (host)
) WITH ('sst_format' = 'flat');

INSERT INTO last_row_selector_cache_filter VALUES
  ('a', 1.0, 1000),
  ('a', 2.0, 2000),
  ('b', 3.0, 1000),
  ('b', 4.0, 2000);

ADMIN FLUSH_TABLE('last_row_selector_cache_filter');

SELECT host, last_value(cpu ORDER BY ts) AS last_cpu
FROM last_row_selector_cache_filter
WHERE host = 'a'
GROUP BY host
ORDER BY host;

SELECT host, last_value(cpu ORDER BY ts) AS last_cpu
FROM last_row_selector_cache_filter
GROUP BY host
ORDER BY host;

DROP TABLE last_row_selector_cache_filter;
