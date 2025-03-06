CREATE TABLE grpc_latencies
(
    ts      TIMESTAMP TIME INDEX,
    host    VARCHAR(255),
    latency FLOAT,
    PRIMARY KEY (host),
);

INSERT INTO grpc_latencies
VALUES ('2023-10-01 10:00:00', 'host1', 120),
       ('2023-10-01 10:00:00', 'host2', 150),
       ('2023-10-01 10:00:05', 'host1', 130);


WITH latencies AS (SELECT ts,
                          host,
                          AVG(latency) RANGE '2s' AS avg_latency
                   FROM grpc_latencies ALIGN '2s' BY (host) FILL PREV
    )
SELECT latencies.ts,
       AVG(latencies.avg_latency)
FROM latencies
GROUP BY latencies.ts ORDER BY latencies.ts;

DROP TABLE grpc_latencies;