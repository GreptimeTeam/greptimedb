SELECT ts, json_get_int(j, 'n') AS n,
       json_get_float(j, 'f') AS f,
       json_get_string(j, 'big') AS big,
       json_get_bool(j, 'flag') AS flag,
       json_to_string(json_get_object(j, '$."a.b"')) AS nested
FROM jsonb_upgrade ORDER BY ts;

INSERT INTO jsonb_upgrade VALUES
  (3, parse_json('{"a.b":{"name":"new","items":[4,5]},"n":42,"f":2.5,"big":18446744073709551615,"flag":true,"nil":null}'));

ADMIN FLUSH_TABLE('jsonb_upgrade');

SELECT ts, json_get_int(j, 'n') AS n,
       json_get_float(j, 'f') AS f,
       json_get_string(j, 'big') AS big,
       json_get_bool(j, 'flag') AS flag,
       json_to_string(json_get_object(j, '$."a.b"')) AS nested
FROM jsonb_upgrade ORDER BY ts;

SELECT SUM(json_get_int(j, 'n')) AS total FROM jsonb_upgrade;
