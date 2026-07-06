SELECT ts, host, greptime_value
FROM metric_value_encoding_default
ORDER BY ts, host;

SELECT
    COUNT(*) > 0 AS has_region,
    SUM(
        CASE
            WHEN region_options LIKE '%experimental_metric_engine_value_encoding%' THEN 1
            ELSE 0
        END
    ) AS synthesized_option_count
FROM information_schema.region_info
WHERE region_id IN (
    SELECT region_id
    FROM information_schema.region_peers
    WHERE table_name = 'metric_value_encoding_default_phy'
);

INSERT INTO metric_value_encoding_default (host, ts, greptime_value) VALUES
    ('host_c', '2024-01-01 00:02:00+0000', 3.5);

SELECT ts, host, greptime_value
FROM metric_value_encoding_default
ORDER BY ts, host;
