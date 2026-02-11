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

SHOW CREATE TABLE metric_cte_sink;

SELECT source_table_names FROM information_schema.flows WHERE flow_name = 'calc_cte';

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_cte');

SELECT * FROM metric_cte_sink ORDER BY ts;

DROP FLOW calc_cte;
DROP TABLE metric_cte;
DROP TABLE metric_cte_sink;
