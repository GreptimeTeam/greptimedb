-- Structured query for GreptimeDB and Clickhouse

-- query count
select count(*) from test_table;

-- query by timerange. Note: place the timestamp range in the where clause
-- GreptimeDB
-- you can use `select max(timestamp)::bigint from test_table;` and `select min(timestamp)::bigint from test_table;`
-- to get the full timestamp range
select * from test_table where timestamp between 1723710843619 and 1723711367588;
-- Clickhouse
-- you can use `select max(timestamp) from test_table;` and `select min(timestamp) from test_table;`
-- to get the full timestamp range
select * from test_table where timestamp between '2024-08-16T03:58:46Z' and '2024-08-16T04:03:50Z';

-- query by condition
SELECT * FROM test_table WHERE user = 'CrucifiX' and method = 'OPTION' and path = '/user/booperbot124' and http_version = 'HTTP/1.1' and status = 401;

-- query by condition and timerange
-- GreptimeDB
SELECT * FROM test_table WHERE user = "CrucifiX" and method = "OPTION" and path = "/user/booperbot124" and http_version = "HTTP/1.1" and status = 401 
and timestamp between 1723774396760 and 1723774788760;
-- Clickhouse
SELECT * FROM test_table WHERE user = 'CrucifiX' and method = 'OPTION' and path = '/user/booperbot124' and http_version = 'HTTP/1.1' and status = 401 
and timestamp between '2024-08-16T03:58:46Z' and '2024-08-16T04:03:50Z';

-- Unstructured query for GreptimeDB and Clickhouse


-- query by condition
-- GreptimeDB
SELECT * FROM test_table WHERE MATCHES(message, "+CrucifiX +OPTION +/user/booperbot124 +HTTP/1.1 +401");
-- Clickhouse
SELECT * FROM test_table WHERE (message LIKE '%CrucifiX%') 
AND (message LIKE '%OPTION%') 
AND (message LIKE '%/user/booperbot124%') 
AND (message LIKE '%HTTP/1.1%') 
AND (message LIKE '%401%');

-- query by condition and timerange
-- GreptimeDB
SELECT * FROM test_table WHERE MATCHES(message, "+CrucifiX +OPTION +/user/booperbot124 +HTTP/1.1 +401") 
and timestamp between 1723710843619 and 1723711367588;
-- Clickhouse
SELECT * FROM test_table WHERE (message LIKE '%CrucifiX%') 
AND (message LIKE '%OPTION%') 
AND (message LIKE '%/user/booperbot124%') 
AND (message LIKE '%HTTP/1.1%') 
AND (message LIKE '%401%') 
AND timestamp between '2024-08-15T10:25:26.524000000Z' AND '2024-08-15T10:31:31.746000000Z';
