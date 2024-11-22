USE INFORMATION_SCHEMA;

DESC TABLE CLUSTER_INFO;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE (\s\d+\.\d+\.\d+\s) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7,8}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{19,}) Start_time
-- SQLNESS REPLACE ((\d+(s|ms|m)\s)+) Duration
-- SQLNESS REPLACE [\s\-]+
SELECT * FROM CLUSTER_INFO ORDER BY peer_type;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE (\s\d+\.\d+\.\d+\s) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7,8}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{19,}) Start_time
-- SQLNESS REPLACE ((\d+(s|ms|m)\s)+) Duration
-- SQLNESS REPLACE [\s\-]+
SELECT * FROM CLUSTER_INFO WHERE PEER_TYPE = 'METASRV' ORDER BY peer_type;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE (\s\d+\.\d+\.\d+\s) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7,8}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{19,}) Start_time
-- SQLNESS REPLACE ((\d+(s|ms|m)\s)+) Duration
-- SQLNESS REPLACE [\s\-]+
SELECT * FROM CLUSTER_INFO WHERE PEER_TYPE = 'FRONTEND' ORDER BY peer_type;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE (\s\d+\.\d+\.\d+\s) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7,8}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{19,}) Start_time
-- SQLNESS REPLACE ((\d+(s|ms|m)\s)+) Duration
-- SQLNESS REPLACE [\s\-]+
SELECT * FROM CLUSTER_INFO WHERE PEER_TYPE != 'FRONTEND' ORDER BY peer_type;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE (\s\d+\.\d+\.\d+\s) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7,8}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{19,}) Start_time
-- SQLNESS REPLACE ((\d+(s|ms|m)\s)+) Duration
-- SQLNESS REPLACE [\s\-]+
SELECT * FROM CLUSTER_INFO WHERE PEER_ID > 1 ORDER BY peer_type;

USE PUBLIC;
