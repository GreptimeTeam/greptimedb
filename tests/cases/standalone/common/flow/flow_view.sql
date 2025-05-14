CREATE TABLE ngx_access_log ( ip_address STRING, http_method STRING, request STRING, status_code INT16, body_bytes_sent INT32, user_agent STRING, response_size INT32, ts TIMESTAMP TIME INDEX, PRIMARY KEY (ip_address, http_method, user_agent, status_code)) WITH ('append_mode'='true');

CREATE TABLE user_agent_statistics ( user_agent STRING, total_count INT64, update_at TIMESTAMP, __ts_placeholder TIMESTAMP TIME INDEX, PRIMARY KEY (user_agent));

INSERT INTO ngx_access_log VALUES ('192.168.1.1', 'GET', '/index.html', 200, 512, 'Mozilla/5.0', 1024, '2023-10-01T10:00:00Z'), ('192.168.1.2', 'POST', '/submit', 201, 256, 'curl/7.68.0', 512, '2023-10-01T10:01:00Z'), ('192.168.1.1', 'GET', '/about.html', 200, 128, 'Mozilla/5.0', 256, '2023-10-01T10:02:00Z'), ('192.168.1.3', 'GET', '/contact', 404, 64, 'curl/7.68.0', 128, '2023-10-01T10:03:00Z');

-- SQLNESS SLEEP 1s
INSERT INTO ngx_access_log VALUES ('192.168.1.1', 'GET', '/index.html', 200, 512, 'Mozilla/5.0', 1024, '2023-10-01T10:00:00Z'), ('192.168.1.2', 'POST', '/submit', 201, 256, 'curl/7.68.0', 512, '2023-10-01T10:01:00Z'), ('192.168.1.1', 'GET', '/about.html', 200, 128, 'Mozilla/5.0', 256, '2023-10-01T10:02:00Z'), ('192.168.1.3', 'GET', '/contact', 404, 64, 'curl/7.68.0', 128, '2023-10-01T10:03:00Z');

-- SQLNESS SLEEP 1s
INSERT INTO ngx_access_log VALUES ('192.168.1.1', 'GET', '/index.html', 200, 512, 'Mozilla/5.0', 1024, '2023-10-01T10:00:00Z'), ('192.168.1.2', 'POST', '/submit', 201, 256, 'curl/7.68.0', 512, '2023-10-01T10:01:00Z'), ('192.168.1.1', 'GET', '/about.html', 200, 128, 'Mozilla/5.0', 256, '2023-10-01T10:02:00Z'), ('192.168.1.3', 'GET', '/contact', 404, 64, 'curl/7.68.0', 128, '2023-10-01T10:03:00Z');

CREATE FLOW user_agent_flow SINK TO user_agent_statistics AS SELECT user_agent, COUNT(user_agent) AS total_count FROM ngx_access_log GROUP BY user_agent;

SELECT created_time <= updated_time, created_time IS NOT NULL, source_table_names FROM information_schema.flows WHERE flow_name = 'user_agent_flow';

CREATE OR REPLACE FLOW user_agent_flow SINK TO user_agent_statistics AS SELECT user_agent, COUNT(user_agent)+123 AS total_count FROM ngx_access_log GROUP BY user_agent;

SELECT created_time < updated_time, created_time IS NOT NULL, updated_time IS NOT NULL, source_table_names FROM information_schema.flows WHERE flow_name = 'user_agent_flow';

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('user_agent_flow');

INSERT INTO ngx_access_log VALUES ('192.168.1.1', 'GET', '/index.html', 200, 512, 'Mozilla/5.0', 1024, '2023-10-01T10:00:00Z'), ('192.168.1.2', 'POST', '/submit', 201, 256, 'curl/7.68.0', 512, '2023-10-01T10:01:00Z'), ('192.168.1.1', 'GET', '/about.html', 200, 128, 'Mozilla/5.0', 256, '2023-10-01T10:02:00Z'), ('192.168.1.3', 'GET', '/contact', 404, 64, 'curl/7.68.0', 128, '2023-10-01T10:03:00Z');

-- SQLNESS SLEEP 1s
INSERT INTO ngx_access_log VALUES ('192.168.1.1', 'GET', '/index.html', 200, 512, 'Mozilla/5.0', 1024, '2023-10-01T10:00:00Z'), ('192.168.1.2', 'POST', '/submit', 201, 256, 'curl/7.68.0', 512, '2023-10-01T10:01:00Z'), ('192.168.1.1', 'GET', '/about.html', 200, 128, 'Mozilla/5.0', 256, '2023-10-01T10:02:00Z'), ('192.168.1.3', 'GET', '/contact', 404, 64, 'curl/7.68.0', 128, '2023-10-01T10:03:00Z');

-- SQLNESS SLEEP 1s
INSERT INTO ngx_access_log VALUES ('192.168.1.1', 'GET', '/index.html', 200, 512, 'Mozilla/5.0', 1024, '2023-10-01T10:00:00Z'), ('192.168.1.2', 'POST', '/submit', 201, 256, 'curl/7.68.0', 512, '2023-10-01T10:01:00Z'), ('192.168.1.1', 'GET', '/about.html', 200, 128, 'Mozilla/5.0', 256, '2023-10-01T10:02:00Z'), ('192.168.1.3', 'GET', '/contact', 404, 64, 'curl/7.68.0', 128, '2023-10-01T10:03:00Z');

-- SQLNESS SLEEP 10s
INSERT INTO ngx_access_log VALUES ('192.168.1.1', 'GET', '/index.html', 200, 512, 'Mozilla/5.0', 1024, '2023-10-01T10:00:00Z'), ('192.168.1.2', 'POST', '/submit', 201, 256, 'curl/7.68.0', 512, '2023-10-01T10:01:00Z'), ('192.168.1.1', 'GET', '/about.html', 200, 128, 'Mozilla/5.0', 256, '2023-10-01T10:02:00Z'), ('192.168.1.3', 'GET', '/contact', 404, 64, 'curl/7.68.0', 128, '2023-10-01T10:03:00Z');

-- TODO(discord9): fix flow stat update for batching mode flow
SELECT created_time < last_execution_time, created_time IS NOT NULL, last_execution_time IS NOT NULL, source_table_names FROM information_schema.flows WHERE flow_name = 'user_agent_flow';

DROP TABLE ngx_access_log;
DROP TABLE user_agent_statistics;
DROP FLOW user_agent_flow;
