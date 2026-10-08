SELECT ts, host, val FROM t_downgrade_compatibility ORDER BY ts, host;

SELECT ts, host, val FROM t_preserve_sequence_downgrade ORDER BY ts, host;

SELECT ts, host, f, d
FROM t_sst_float_field_encoding_downgrade
ORDER BY ts, host;
