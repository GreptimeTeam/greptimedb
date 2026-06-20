SELECT ts, host, json_to_string(j) AS j
FROM legacy_jsonb
ORDER BY ts, host;

SELECT host, json_to_string(j) AS j
FROM legacy_jsonb
WHERE json_get(j, 'a') = 10
ORDER BY host;

SHOW CREATE TABLE legacy_jsonb;
