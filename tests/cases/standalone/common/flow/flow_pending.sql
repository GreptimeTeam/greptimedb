CREATE FLOW pending_user_cnt SINK TO pending_user_cnt_sink AS
SELECT host, COUNT(val) AS total_val
FROM pending_user_cnt_source
GROUP BY host;

SELECT source_table_ids, source_table_names, flownode_ids
FROM information_schema.flows
WHERE flow_name = 'pending_user_cnt';

SHOW CREATE TABLE pending_user_cnt_sink;

CREATE TABLE pending_user_cnt_source (
  ts TIMESTAMP TIME INDEX,
  host STRING,
  val DOUBLE,
  PRIMARY KEY(host)
);

-- SQLNESS SLEEP 15s
SHOW CREATE TABLE pending_user_cnt_sink;

INSERT INTO pending_user_cnt_source VALUES
  ('2026-03-12T00:00:00Z', 'host1', 1.0),
  ('2026-03-12T00:00:01Z', 'host1', 2.0),
  ('2026-03-12T00:00:02Z', 'host2', 3.0);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('pending_user_cnt');

SELECT host, total_val FROM pending_user_cnt_sink ORDER BY host;

DROP FLOW pending_user_cnt;
DROP TABLE pending_user_cnt_source;
DROP TABLE pending_user_cnt_sink;

CREATE TABLE pending_replace_src_a (
  ts TIMESTAMP TIME INDEX,
  host STRING,
  val DOUBLE,
  PRIMARY KEY(host)
);

CREATE FLOW pending_replace_flow SINK TO pending_replace_sink AS
SELECT host, COUNT(val) AS total_val
FROM pending_replace_src_a
GROUP BY host;

INSERT INTO pending_replace_src_a VALUES
  ('2026-03-12T00:10:00Z', 'host_old', 1.0),
  ('2026-03-12T00:10:01Z', 'host_old', 2.0);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('pending_replace_flow');

SELECT host, total_val FROM pending_replace_sink WHERE host = 'host_old';

CREATE OR REPLACE FLOW pending_replace_flow SINK TO pending_replace_sink AS
SELECT host, COUNT(val) AS total_val
FROM pending_replace_src_b
GROUP BY host;

CREATE TABLE pending_replace_src_b (
  ts TIMESTAMP TIME INDEX,
  host STRING,
  val DOUBLE,
  PRIMARY KEY(host)
);

-- SQLNESS SLEEP 15s
INSERT INTO pending_replace_src_a VALUES
  ('2026-03-12T00:11:00Z', 'host_old_after_replace', 9.0);

INSERT INTO pending_replace_src_b VALUES
  ('2026-03-12T00:11:00Z', 'host_new_after_replace', 3.0),
  ('2026-03-12T00:11:01Z', 'host_new_after_replace', 4.0);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('pending_replace_flow');

SELECT host, total_val
FROM pending_replace_sink
WHERE host IN ('host_old_after_replace', 'host_new_after_replace')
ORDER BY host;

DROP FLOW pending_replace_flow;
DROP TABLE pending_replace_src_a;
DROP TABLE pending_replace_src_b;
DROP TABLE pending_replace_sink;
