-- Simplified schema and queries for TSID on physical table

CREATE TABLE phy (
  ts TIMESTAMP(3) TIME INDEX,
  v DOUBLE NULL,
  tag1 STRING,
  tag2 STRING,
  le STRING,
  tag4 STRING,
  tag5 STRING,
  tag6 STRING NULL,
  tag7 STRING NULL,
  tag8 STRING NULL,
  PRIMARY KEY (
    tag1,
    tag2,
    le,
    tag4,
    tag5,
    tag6,
    tag7,
    tag8
  )
) ENGINE = metric WITH ("physical_metric_table" = "");

CREATE TABLE IF NOT EXISTS test_tsid (
  tag1 STRING,
  tag2 STRING,
  ts TIMESTAMP(3) NOT NULL,
  v DOUBLE NULL,
  le STRING,
  tag4 STRING,
  tag5 STRING,
  tag6 STRING NULL,
  tag7 STRING NULL,
  tag8 STRING NULL,
  TIME INDEX (ts),
  PRIMARY KEY (
    tag1,
    tag2,
    le,
    tag4,
    tag5,
    tag6,
    tag7,
    tag8
  )
) ENGINE=metric WITH(
  on_physical_table = 'phy'
);

INSERT INTO test_tsid
  (ts, v, tag1, tag2, le, tag4, tag5, tag8, tag6, tag7)
VALUES
  ('2026-01-23T03:40:00Z', 10.0, 'istio-ingressgateway', 'outbound', '0.5',  'svc-a', 'prod', 'peer.example', 'svc-b', 'prod'),
  ('2026-01-23T03:41:00Z', 5.0,  'istio-ingressgateway', 'outbound', '0.9',  'svc-a', 'prod', 'peer.example', 'svc-b', 'prod'),
  ('2026-01-23T03:41:30Z', 2.0,  'istio-ingressgateway', 'outbound', '+Inf', 'svc-a', 'prod', 'peer.example', 'svc-b', 'prod');

TQL EVAL (
  timestamp '2026-01-23 03:30:00+00' + (now() - now()),
  timestamp '2026-01-23 03:45:00+00' + (now() - now()),
  '1m'
) histogram_quantile(
  0.50,
  sum by (le, tag4, tag5) (
    avg_over_time(test_tsid[30m])
  )
);

-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
TQL EXPLAIN (
  timestamp '2026-01-23 03:30:00+00' + (now() - now()),
  timestamp '2026-01-23 03:45:00+00' + (now() - now()),
  '1m'
) histogram_quantile(
  0.50,
  sum by (le, tag4, tag5) (
    avg_over_time(test_tsid[30m])
  )
);

CREATE FLOW IF NOT EXISTS test_tsid
SINK TO 'test_tsid_output'
EVAL INTERVAL '3600 s'
AS
TQL EVAL (
  timestamp '2026-01-23 03:10:00+00' + (now() - now()),
  timestamp '2026-01-23 03:50:00+00' + (now() - now()),
  '1m'
)
histogram_quantile(
  0.50,
  sum by (le, tag4, tag5) (
    avg_over_time(test_tsid[30m])
  )
);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('test_tsid');

SELECT * FROM "test_tsid_output"
ORDER BY ts
LIMIT 5;

DROP FLOW test_tsid;
DROP TABLE IF EXISTS "test_tsid_output";
DROP TABLE test_tsid;
DROP TABLE phy;
