-- Regression test for remote dynamic filter pushdown preserving a HAVING filter.

CREATE TABLE rdf_having_dim(k INTEGER, label STRING, ts TIMESTAMP TIME INDEX);
CREATE TABLE rdf_having_fact(k INTEGER, v DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO rdf_having_dim VALUES
  (1, 'keep', 1000),
  (2, 'drop', 2000);

INSERT INTO rdf_having_fact VALUES
  (1, 200.0, 1000),
  (1, 150.0, 2000),
  (2, 100.0, 3000);

SELECT d.label, s.total
FROM rdf_having_dim d
INNER JOIN (
  SELECT k, SUM(v) AS total
  FROM rdf_having_fact
  GROUP BY k
  HAVING SUM(v) > 300
) s ON d.k = s.k
ORDER BY s.total DESC, d.label;

DROP TABLE rdf_having_dim;
DROP TABLE rdf_having_fact;
