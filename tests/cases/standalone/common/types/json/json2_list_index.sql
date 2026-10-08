CREATE TABLE json2_list_index (
    ts TIMESTAMP TIME INDEX,
    host STRING,
    j JSON2
) WITH (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

INSERT INTO json2_list_index VALUES
    (1, 'host1', '{"a.b":"root-dotted","l":[[10,11],["a","b"]],"o":{"l":[{"inner":{"l":[1,2,3]}},{"inner":{"l":["x","y","z"]},"casekey":"normalized","UPPER":"quoted","a.b":"dotted"}]}}'),
    (2, 'host2', '{"l":[[20],[21,22,23]],"o":{"l":[{"inner":{"l":[4,5,6]}},{"inner":{"l":[7,8]}}]}}'),
    (3, 'host3', '{"l":[null,[30]],"o":{"l":[null,{"inner":{"l":[true,false,null]}}]}}');

ADMIN FLUSH_TABLE('json2_list_index');

INSERT INTO json2_list_index VALUES
    (4, 'host4', '{"l":"not a list","o":{"l":{"inner":{"l":[40]}}}}'),
    (5, 'host5', '{"l":[[50,51],null],"o":{"l":[{},null]}}'),
    (6, 'host6', '{"other":"missing paths"}');

ADMIN FLUSH_TABLE('json2_list_index');

ADMIN COMPACT_TABLE('json2_list_index', 'swcs', '86400');

SELECT ts, host, j.l[0][1] AS nested_list
FROM json2_list_index
ORDER BY ts;

SELECT ts, j.l[1][0] AS second_list
FROM json2_list_index
ORDER BY ts;

SELECT ts, host, j.o.l[1].inner.l[2] AS deeply_nested
FROM json2_list_index
ORDER BY ts;

SELECT ts,
       j.o.l[1].CASEKEY AS normalized,
       j.o.l[1]."UPPER" AS quoted_upper,
       j.o.l[1]."a.b" AS dotted_key
FROM json2_list_index
ORDER BY ts;

SELECT ts, j."a.b" AS root_dotted_key
FROM json2_list_index
ORDER BY ts;

SELECT ts, j.l[0][0]::DOUBLE * 2 AS calculated
FROM json2_list_index
ORDER BY ts;

SELECT ts,
       json_get(j, '$."a.b"') AS quoted_path,
       json_get(j, '$["a.b"]') AS bracket_path,
       json_get(j, '$.o.l[1]."a.b"') AS nested_path
FROM json2_list_index
ORDER BY ts;

-- Invalid paths must fail planning instead of falling back to Variant.
SELECT json_get(j, '$.o[')::BIGINT FROM json2_list_index;

SELECT json_get(j, '$.missing')::BIGINT FROM json2_list_index ORDER BY ts;

DROP TABLE json2_list_index;
