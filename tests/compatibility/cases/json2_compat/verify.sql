SELECT host, json_to_string(j)
FROM t_json2_compat
ORDER BY host;

SELECT host, json_get_int(j, 'a') AS a
FROM t_json2_compat
WHERE json_get_int(j, 'a') >= 20
ORDER BY host;

SELECT host, json_get_string(j, 'nested.b') AS nested_b
FROM t_json2_compat
WHERE json_get_bool(j, 'flag') = true
ORDER BY host;

SHOW CREATE TABLE t_json2_compat;
