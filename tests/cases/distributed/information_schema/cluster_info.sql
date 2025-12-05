USE INFORMATION_SCHEMA;

DESC TABLE CLUSTER_INFO;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE (\d+\.\d+(?:\.\d+)*-[a-zA-Z0-9.-]+) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7,10}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{19,}) Start_time
-- SQLNESS REPLACE ((\d+(s|ms|m)\s)+) Duration
-- SQLNESS REPLACE (\s127\.0\.0\.1:\d+\s) Address
-- SQLNESS REPLACE [\s\-]+
SELECT peer_id, peer_type, peer_addr, version, git_commit, start_time, uptime, active_time FROM CLUSTER_INFO ORDER BY peer_type;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE (\d+\.\d+(?:\.\d+)*-[a-zA-Z0-9.-]+) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7,10}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{19,}) Start_time
-- SQLNESS REPLACE ((\d+(s|ms|m)\s)+) Duration
-- SQLNESS REPLACE (\s127\.0\.0\.1:\d+\s) Address
-- SQLNESS REPLACE [\s\-]+
SELECT peer_id, peer_type, peer_addr, version, git_commit, start_time, uptime, active_time FROM CLUSTER_INFO WHERE PEER_TYPE = 'METASRV' ORDER BY peer_type;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE (\d+\.\d+(?:\.\d+)*-[a-zA-Z0-9.-]+) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7,10}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{19,}) Start_time
-- SQLNESS REPLACE ((\d+(s|ms|m)\s)+) Duration
-- SQLNESS REPLACE (\s127\.0\.0\.1:\d+\s) Address
-- SQLNESS REPLACE [\s\-]+
SELECT peer_id, peer_type, peer_addr, version, git_commit, start_time, uptime, active_time FROM CLUSTER_INFO WHERE PEER_TYPE = 'FRONTEND' ORDER BY peer_type;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE (\d+\.\d+(?:\.\d+)*-[a-zA-Z0-9.-]+) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7,10}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{19,}) Start_time
-- SQLNESS REPLACE ((\d+(s|ms|m)\s)+) Duration
-- SQLNESS REPLACE (\s127\.0\.0\.1:\d+\s) Address
-- SQLNESS REPLACE [\s\-]+
SELECT peer_id, peer_type, peer_addr, version, git_commit, start_time, uptime, active_time FROM CLUSTER_INFO WHERE PEER_TYPE != 'FRONTEND' ORDER BY peer_type;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE (\d+\.\d+(?:\.\d+)*-[a-zA-Z0-9.-]+) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7,10}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{19,}) Start_time
-- SQLNESS REPLACE ((\d+(s|ms|m)\s)+) Duration
-- SQLNESS REPLACE (\s127\.0\.0\.1:\d+\s) Address
-- SQLNESS REPLACE [\s\-]+
SELECT peer_id, peer_type, peer_addr, version, git_commit, start_time, uptime, active_time FROM CLUSTER_INFO WHERE PEER_ID > 1 ORDER BY peer_type;

-- SQLNESS REPLACE (:\s*(\".*?\"|\[.*?\]|\{.*?\}|[0-9]+|true|false|null)) PLACEHOLDER
SELECT peer_id, node_status FROM CLUSTER_INFO WHERE PEER_TYPE = 'DATANODE' ORDER BY peer_id;

SELECT peer_type, total_cpu_millicores!=0, total_memory_bytes!=0 FROM CLUSTER_INFO ORDER BY peer_type;

USE PUBLIC;
