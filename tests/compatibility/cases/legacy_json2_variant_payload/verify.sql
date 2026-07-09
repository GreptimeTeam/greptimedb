SELECT ts, j.a.b AS a_b, j.c AS c
FROM t_legacy_json2_variant_payload
ORDER BY ts;

SELECT ts, j.d AS d
FROM t_legacy_json2_variant_payload
ORDER BY ts;

SHOW CREATE TABLE t_legacy_json2_variant_payload;
