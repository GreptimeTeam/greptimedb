USE INFORMATION_SCHEMA;

DESC TABLE CLUSTER_INFO;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE (\d+\.\d+\.\d+) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7,8}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{15,}) Start_time
-- SQLNESS REPLACE ((\d+(s|ms|m)\s)+) Duration
-- SQLNESS REPLACE [\s\-]+
SELECT * FROM CLUSTER_INFO;

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE (\d+\.\d+\.\d+) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7,8}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{15,}) Start_time
-- SQLNESS REPLACE ((\d+(s|ms|m)\s)+) Duration
-- SQLNESS REPLACE [\s\-]+
SELECT * FROM CLUSTER_INFO WHERE PEER_TYPE = 'STANDALONE';

SELECT * FROM CLUSTER_INFO WHERE PEER_TYPE != 'STANDALONE';

-- SQLNESS REPLACE version node_version
-- SQLNESS REPLACE (\d+\.\d+\.\d+) Version
-- SQLNESS REPLACE (\s[a-z0-9]{7,8}\s) Hash
-- SQLNESS REPLACE (\s[\-0-9T:\.]{15,}) Start_time
-- SQLNESS REPLACE ((\d+(s|ms|m)\s)+) Duration
-- SQLNESS REPLACE [\s\-]+
SELECT * FROM CLUSTER_INFO WHERE PEER_ID = 0;

SELECT * FROM CLUSTER_INFO WHERE PEER_ID > 0;

USE PUBLIC;
