CREATE TABLE `api_requests` (
  `timestamp` TIMESTAMP NOT NULL,
  `request_id` STRING NOT NULL,
  `upstream_id` STRING NOT NULL,
  `application_id` STRING NULL,
  `url` STRING NOT NULL,
  `method` STRING NOT NULL,
  `status_code` INTEGER NOT NULL,
  `request_headers` JSON NULL,
  `request_body` STRING NULL,
  `response_headers` JSON NULL,
  `response_body` STRING NULL,
  `latency_ms` INTEGER NOT NULL,
  `client_ip` STRING NULL,
  `user_agent` STRING NULL,
  TIME INDEX (`timestamp`)
)
WITH(
  append_mode = 'true'
);

CREATE TABLE api_request_volume_upstream_stats (
  `upstream_id` STRING NOT NULL,
  `time_window` TIMESTAMP NOT NULL,
  `request_count` BIGINT NOT NULL,
  TIME INDEX (`time_window`)
);

CREATE FLOW api_request_volume_by_upstream
SINK TO api_request_volume_upstream_stats
AS
SELECT
    upstream_id,
    date_bin(INTERVAL '1 hour', timestamp, '2024-01-01 00:00:00'::TimestampNanosecond) AS time_window,
    COUNT(*) AS request_count
FROM api_requests
GROUP BY upstream_id, time_window;

DROP FLOW api_request_volume_by_upstream;
DROP TABLE api_request_volume_upstream_stats;
DROP TABLE api_requests;