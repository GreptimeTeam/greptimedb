USE INFORMATION_SCHEMA;

DESC TABLE CLUSTER_INFO;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE (\d+\.\d+(?:\.\d+)*-[a-zA-Z0-9.-]+) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7,10}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{15,}) Start_time
-- SQLNESS REPLACE ((\d+(s|ms|m)\s)+) Duration
-- SQLNESS REPLACE [\s\-]+
SELECT peer_id, peer_type, peer_addr, version, git_commit, start_time, uptime, active_time FROM CLUSTER_INFO;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE (\d+\.\d+(?:\.\d+)*-[a-zA-Z0-9.-]+) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7,10}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{15,}) Start_time
-- SQLNESS REPLACE ((\d+(s|ms|m)\s)+) Duration
-- SQLNESS REPLACE [\s\-]+
SELECT peer_id, peer_type, peer_addr, version, git_commit, start_time, uptime, active_time FROM CLUSTER_INFO WHERE PEER_TYPE = 'STANDALONE';

SELECT peer_id, peer_type, peer_addr, version, git_commit, start_time, uptime, active_time FROM CLUSTER_INFO WHERE PEER_TYPE != 'STANDALONE';

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE (\d+\.\d+(?:\.\d+)*-[a-zA-Z0-9.-]+) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7,10}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{15,}) Start_time
-- SQLNESS REPLACE ((\d+(s|ms|m)\s)+) Duration
-- SQLNESS REPLACE [\s\-]+
SELECT peer_id, peer_type, peer_addr, version, git_commit, start_time, uptime, active_time FROM CLUSTER_INFO WHERE PEER_ID = 0;

SELECT peer_id, peer_type, peer_addr, version, git_commit, start_time, uptime, active_time FROM CLUSTER_INFO WHERE PEER_ID > 0;

SELECT peer_type, total_cpu_millicores!=0, total_memory_bytes!=0 FROM CLUSTER_INFO ORDER BY peer_type;

USE PUBLIC;
