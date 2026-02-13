CREATE TABLE metric_cte (
  ts timestamp(3) time index,
  val DOUBLE,
);

INSERT INTO TABLE metric_cte VALUES
  (0::Timestamp, 0),
  (5000::Timestamp, 1),
  (10000::Timestamp, 2),
  (15000::Timestamp, 3);

CREATE FLOW calc_cte SINK TO metric_cte_sink EVAL INTERVAL '1m' AS
WITH tql (ts, the_value) AS (
  TQL EVAL (now() - now(), now() - (now() - '15s'::interval), '5s') metric_cte
)
SELECT * FROM tql;

CREATE TABLE tql (
  ts timestamp(3) time index,
  val DOUBLE,
);

INSERT INTO TABLE tql VALUES
  (0::Timestamp, 10),
  (5000::Timestamp, 20),
  (10000::Timestamp, 30),
  (15000::Timestamp, 40);

-- Fail due to case sensitivity of CTE name
CREATE FLOW calc_cte_case SINK TO metric_cte_join_sink EVAL INTERVAL '1m' AS
WITH "TQL"(ts, the_value) AS (
  TQL EVAL (now() - now(), now() - (now() - '15s'::interval), '5s') metric_cte
)
SELECT * FROM TQL;

CREATE FLOW calc_cte_case SINK TO metric_cte_join_sink EVAL INTERVAL '1m' AS
WITH "TQL"(ts, the_value) AS (
  TQL EVAL (now() - now(), now() - (now() - '15s'::interval), '5s') metric_cte
)
SELECT * FROM "TQL";

SHOW CREATE TABLE metric_cte_sink;

SELECT source_table_names FROM information_schema.flows WHERE flow_name = 'calc_cte';

SHOW CREATE TABLE metric_cte_join_sink;

SELECT source_table_names FROM information_schema.flows WHERE flow_name = 'calc_cte_case';

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_cte');

SELECT * FROM metric_cte_sink ORDER BY ts;

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_cte_case');

SELECT * FROM metric_cte_join_sink ORDER BY ts;

DROP FLOW calc_cte;
DROP FLOW calc_cte_case;
DROP TABLE metric_cte;
DROP TABLE tql;
DROP TABLE metric_cte_sink;
DROP TABLE metric_cte_join_sink;
