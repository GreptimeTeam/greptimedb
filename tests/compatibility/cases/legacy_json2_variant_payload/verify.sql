SELECT ts, j.a.b AS a_b, j.c AS c
FROM t_legacy_json2_variant_payload
ORDER BY ts;

SELECT
  ts,
  j.d.e AS d_e
FROM t_legacy_json2_variant_payload_object_d
ORDER BY ts;

-- JSON2 projection rendering is a user-visible compatibility contract.
SELECT ts, j.d AS d
FROM t_legacy_json2_variant_payload
ORDER BY ts;

SHOW CREATE TABLE t_legacy_json2_variant_payload;
