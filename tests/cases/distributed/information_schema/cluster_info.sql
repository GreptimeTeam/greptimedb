USE INFORMATION_SCHEMA;

DESC TABLE CLUSTER_INFO;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE unknown UNKNOWN
-- SQLNESS REPLACE (\s\d\.\d\.\d\s) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{23}\s) Start_time
-- SQLNESS REPLACE (\s+uptime\s+) uptime
-- SQLNESS REPLACE (\s+(\d+(s|ms|m)\s)+) Uptime
SELECT * FROM CLUSTER_INFO ORDER BY peer_type;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE unknown UNKNOWN
-- SQLNESS REPLACE (\s\d\.\d\.\d\s) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{23}\s) Start_time
-- SQLNESS REPLACE (\s+uptime\s+) uptime
-- SQLNESS REPLACE (\s+(\d+(s|ms|m)\s)+) Uptime
SELECT * FROM CLUSTER_INFO WHERE PEER_TYPE = 'METASRV' ORDER BY peer_type;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE unknown UNKNOWN
-- SQLNESS REPLACE (\s\d\.\d\.\d\s) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{23}\s) Start_time
-- SQLNESS REPLACE (\s+uptime\s+) uptime
-- SQLNESS REPLACE (\s+(\d+(s|ms|m)\s)+) Uptime
SELECT * FROM CLUSTER_INFO WHERE PEER_TYPE = 'FRONTEND' ORDER BY peer_type;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE unknown UNKNOWN
-- SQLNESS REPLACE (\s\d\.\d\.\d\s) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{23}\s) Start_time
-- SQLNESS REPLACE (\s+uptime\s+) uptime
-- SQLNESS REPLACE (\s+(\d+(s|ms|m)\s)+) Uptime
SELECT * FROM CLUSTER_INFO WHERE PEER_TYPE != 'FRONTEND' ORDER BY peer_type;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE unknown UNKNOWN
-- SQLNESS REPLACE (\s\d\.\d\.\d\s) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{23}\s) Start_time
-- SQLNESS REPLACE (\s+uptime\s+) uptime
-- SQLNESS REPLACE (\s+(\d+(s|ms|m)\s)+) Uptime
SELECT * FROM CLUSTER_INFO WHERE PEER_ID > 1 ORDER BY peer_type;

USE PUBLIC;
