SELECT kind, json_to_string(j) FROM t_legacy_json_non_object ORDER BY kind;

SELECT kind FROM t_legacy_json_non_object WHERE json_is_array(j) ORDER BY kind;

SELECT kind FROM t_legacy_json_non_object WHERE json_is_string(j) ORDER BY kind;

SELECT kind FROM t_legacy_json_non_object WHERE json_is_int(j) ORDER BY kind;

SELECT kind FROM t_legacy_json_non_object WHERE json_is_bool(j) ORDER BY kind;

SELECT kind FROM t_legacy_json_non_object WHERE json_is_null(j) ORDER BY kind;

SELECT kind FROM t_legacy_json_non_object WHERE json_is_object(j) ORDER BY kind;
