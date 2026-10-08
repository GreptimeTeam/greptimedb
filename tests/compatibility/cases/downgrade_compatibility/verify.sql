SELECT ts, host, val FROM t_downgrade_compatibility ORDER BY ts, host;

SELECT ts, host, val FROM t_preserve_sequence_downgrade ORDER BY ts, host;

SELECT ts, host, f, d
FROM t_sst_float_field_encoding_downgrade
ORDER BY ts, host;

-- Older binaries don't recognize the bloom blob written by the current one and must
-- scan without pruning instead of probing it with their own tokens.
SELECT ts, msg FROM t_fulltext_bloom_v2_downgrade WHERE msg @@ 'hello_world' ORDER BY ts;

SELECT ts, msg FROM t_fulltext_bloom_v2_downgrade WHERE msg @@ '错误error日志' ORDER BY ts;
