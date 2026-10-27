SELECT greptime_timestamp, greptime_metric, job, zone, greptime_value, exemplar_labels
FROM metrics_exemplars ORDER BY greptime_timestamp, greptime_metric;
SELECT COUNT(*) FROM metrics_exemplars WHERE greptime_metric = 'latency' AND job = 'api';
SELECT COUNT(*) FROM metrics_exemplars WHERE greptime_metric IS NULL AND job IS NULL AND zone IS NULL;
ADMIN FLUSH_TABLE('metrics_exemplars');
ADMIN COMPACT_TABLE('metrics_exemplars', 'swcs', '86400');
SELECT COUNT(*) FROM metrics_exemplars;
SELECT COUNT(*) FROM metrics_exemplars WHERE zone = 'east' AND exemplar_labels.span_id::STRING = 'def';

SELECT * FROM exemplar_source_ids ORDER BY __tsid, greptime_timestamp;
ADMIN FLUSH_TABLE('exemplar_source_ids');
ADMIN COMPACT_TABLE('exemplar_source_ids', 'swcs', '86400');
SELECT COUNT(*) FROM exemplar_source_ids WHERE __table_id = 42 AND __tsid = 18446744073709551615 AND job IS NULL;
SELECT COUNT(*) FROM exemplar_source_ids WHERE __table_id = 43 AND __tsid = 18446744073709551615;
