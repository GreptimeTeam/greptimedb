SELECT * FROM exemplar_source_ids ORDER BY __tsid, greptime_timestamp;
ADMIN FLUSH_TABLE('exemplar_source_ids');
ADMIN COMPACT_TABLE('exemplar_source_ids', 'swcs', '86400');
SELECT COUNT(*) FROM exemplar_source_ids WHERE __table_id = 42 AND __tsid = 18446744073709551615 AND job IS NULL;
SELECT COUNT(*) FROM exemplar_source_ids WHERE __table_id = 43 AND __tsid = 18446744073709551615;
