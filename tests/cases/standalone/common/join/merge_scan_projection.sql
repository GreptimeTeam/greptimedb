CREATE TABLE projection_spans (
  trace_id STRING,
  span_id STRING,
  parent_span_id STRING,
  service_name STRING,
  payload STRING,
  ts TIMESTAMP TIME INDEX,
  PRIMARY KEY (service_name)
)
PARTITION ON COLUMNS (service_name) (
  service_name < 'storage', service_name >= 'storage'
)
WITH (append_mode = 'true');

INSERT INTO projection_spans VALUES
  ('trace-a', 'root', NULL, 'front', 'root payload', 0),
  ('trace-a', 'child', 'root', 'storage', 'child payload', 1),
  ('trace-a', 'same', 'root', 'front', 'same service', 2),
  ('trace-b', 'child', 'root', 'storage', 'unmatched trace', 3);

ADMIN FLUSH_TABLE('projection_spans');

-- Each remote scan supplies the three join columns without a local projection.
-- SQLNESS REPLACE (peers.*) peers=REDACTED
EXPLAIN SELECT p.service_name AS parent_service,
  c.service_name AS child_service, COUNT(*) AS call_count
FROM projection_spans c JOIN projection_spans p
  ON c.trace_id = p.trace_id AND c.parent_span_id = p.span_id
WHERE c.service_name <> p.service_name
GROUP BY p.service_name, c.service_name
ORDER BY p.service_name, c.service_name;

SELECT p.service_name AS parent_service,
  c.service_name AS child_service, COUNT(*) AS call_count
FROM projection_spans c JOIN projection_spans p
  ON c.trace_id = p.trace_id AND c.parent_span_id = p.span_id
WHERE c.service_name <> p.service_name
GROUP BY p.service_name, c.service_name
ORDER BY p.service_name, c.service_name;

DROP TABLE projection_spans;
