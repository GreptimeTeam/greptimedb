-- Regression test for promql `group()` aggregator planning.

CREATE TABLE kubernetes_build_info (
  ts timestamp(3) time index,
  cluster_name STRING,
  service_name STRING,
  job_name STRING,
  instance STRING,
  val DOUBLE,
  PRIMARY KEY(cluster_name, service_name, job_name, instance),
);

INSERT INTO TABLE kubernetes_build_info VALUES
    (0, 'cluster_a', 'kubernetes', 'apiserver', '0', 123.0),
    (0, 'cluster_a', 'kubernetes', 'apiserver', '1', 456.0),
    (0, 'cluster_b', 'kubernetes', 'apiserver', '0', 789.0);

TQL EVAL (0, 0, '1s') sum(group by (cluster_name)(kubernetes_build_info{service_name="kubernetes",job_name="apiserver"}));

DROP TABLE kubernetes_build_info;

