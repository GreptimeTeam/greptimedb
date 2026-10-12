CREATE TABLE jsonb_upgrade (ts TIMESTAMP TIME INDEX, j JSON);

INSERT INTO jsonb_upgrade VALUES
  (1, parse_json('{"a.b":{"name":"旧数据","items":[1,2,3]},"n":-42,"f":1.5,"big":18446744073709551615,"flag":true,"nil":null}')),
  (2, parse_json('{"a.b":{"name":"escaped \"quote\"","items":[]},"n":0,"f":0.125,"big":0,"flag":false,"nil":null}'));

ADMIN FLUSH_TABLE('jsonb_upgrade');
