-- Regression test for issue #8074
-- https://github.com/GreptimeTeam/greptimedb/issues/8074
CREATE TABLE monitoring_data (
    host STRING INVERTED INDEX,
    `region` STRING,
    cpu_usage DOUBLE INVERTED INDEX,
    `timestamp` TIMESTAMP TIME INDEX
) WITH ('append_mode'='true');

INSERT INTO monitoring_data (host, region, cpu_usage, `timestamp`) VALUES
('web-01', 'us-east', 12.5, '2026-05-06 10:00:00'),
('web-02', 'us-east', 18.3, '2026-05-06 10:00:00'),
('web-03', 'us-east', 91.2, '2026-05-06 10:00:00'),
('web-04', 'us-west', 73.8, '2026-05-06 10:00:00');

INSERT INTO monitoring_data (host, region, cpu_usage, `timestamp`) VALUES
('web-01', 'us-east', 15.2, '2026-05-06 10:01:00'),
('web-02', 'us-east', 23.7, '2026-05-06 10:01:00'),
('web-03', 'us-east', 94.5, '2026-05-06 10:01:00'),
('web-04', 'us-west', 78.1, '2026-05-06 10:01:00');

ADMIN FLUSH_TABLE('monitoring_data');

ALTER TABLE monitoring_data
MODIFY COLUMN cpu_usage STRING;

SELECT host FROM monitoring_data WHERE cpu_usage = '23.7' ORDER BY host;

DROP TABLE monitoring_data;
