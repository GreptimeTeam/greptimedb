CREATE TABLE json_dot_access (ts TIMESTAMP TIME INDEX, j JSON);

INSERT INTO json_dot_access VALUES
    (1, '{"a":{"b":2},"items":[{"name":"first"},{"name":"second"}],"a.b":"literal","UPPER":"quoted","enabled":true}'),
    (2, '{"a":{"b":4},"items":[],"enabled":false}'),
    (3, '{"a":null}'),
    (4, '{}'),
    (5, NULL);

SELECT ts, j.a.b AS nested, j."a.b" AS dotted_key, j."UPPER" AS quoted_key,
       j.missing AS missing, j.items[1].name AS item
FROM json_dot_access ORDER BY ts;

SELECT ts, j.a.b = json_get(j, '$.a.b') AS same_value,
       j.a.b + 1 AS incremented, j.a.b::BIGINT AS cast_value,
       abs(j.a.b) AS absolute
FROM json_dot_access ORDER BY ts;

SELECT t.ts, t.j.a.b AS qualified FROM json_dot_access AS t
WHERE t.j.a.b > 2 AND t.j.enabled = false ORDER BY t.ts;

SELECT sum(j.a.b) AS total FROM json_dot_access;

SELECT ts, payload.a.b AS nested
FROM (SELECT ts, j AS payload FROM json_dot_access) ORDER BY ts;

ADMIN FLUSH_TABLE('json_dot_access');

SELECT ts, j.a.b AS nested, j.items[1].name AS item
FROM json_dot_access ORDER BY ts;

DROP TABLE json_dot_access;
