SELECT greptime_timestamp, greptime_metric, job, zone, greptime_value, exemplar_labels
FROM metrics_exemplars ORDER BY greptime_timestamp, greptime_metric;
SELECT COUNT(*) FROM metrics_exemplars WHERE greptime_metric = 'latency' AND job = 'api';
SELECT COUNT(*) FROM metrics_exemplars WHERE greptime_metric IS NULL AND job IS NULL AND zone IS NULL;
ADMIN FLUSH_TABLE('metrics_exemplars');
ADMIN COMPACT_TABLE('metrics_exemplars', 'swcs', '86400');
SELECT COUNT(*) FROM metrics_exemplars;
SELECT COUNT(*) FROM metrics_exemplars WHERE zone = 'east' AND exemplar_labels.span_id::STRING = 'def';
