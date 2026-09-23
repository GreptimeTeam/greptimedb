CREATE TABLE matching_groups_left (
  host STRING NULL,
  "zone" STRING NULL,
  ts TIMESTAMP(3) TIME INDEX,
  greptime_value DOUBLE,
  PRIMARY KEY(host, "zone")
);
CREATE TABLE matching_groups_right (
  host STRING NULL,
  "zone" STRING NULL,
  ts TIMESTAMP(3) TIME INDEX,
  greptime_value DOUBLE,
  PRIMARY KEY(host, "zone")
);
INSERT INTO matching_groups_left VALUES
  ('x', 'a', 0, 12), ('x', 'b', 0, 24), ('y', 'a', 0, 36),
  ('', 'a', 0, 48), (NULL, 'a', 0, 60);
INSERT INTO matching_groups_right VALUES
  ('x', 'a', 0, 2), ('x', 'b', 0, 4), ('y', 'a', 0, 100),
  ('', 'a', 0, 8), (NULL, 'a', 0, 10);

-- The result labels are the right operand's tag set rather than what the modifier implies
-- (#9207), so `zone` is absent wherever the right side aggregates it away and rows differing
-- only by it are indistinguishable.
-- Filtering whole ranking partitions preserves the aggregate and scalar arithmetic.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '1s') (8 * matching_groups_left{host="x"}) / on(host) group_left topk by(host)(1, max by(host)(matching_groups_right));
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '1s') sum by(host)(matching_groups_left) / on(host) group_right() (matching_groups_right{host="x"} * 2);
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '1s') matching_groups_left{host!="y"} / on(host) group_left max by(host)(matching_groups_right);
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '1s') matching_groups_left{host=~"x|y"} / on(host) group_left max by(host)(matching_groups_right);
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '1s') matching_groups_left{host=""} / on(host) group_left max by(host)(matching_groups_right);
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '1s') matching_groups_left{host!~"x|y"} / on(host) group_left max by(host)(matching_groups_right);

-- bottomk partitions by host when zone is excluded from grouping.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '1s') bottomk without(zone)(1, matching_groups_left) / on(host,zone) matching_groups_right{host="x"};

-- A matcher can leave a global ranking operand without changing its candidates.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '1s') topk(1, matching_groups_left{host="x"}) / on(host,zone) matching_groups_right;
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '1s') matching_groups_left / on(host,zone) bottomk(1, matching_groups_right{host="x"});

-- The global winner is host y. Pushing host=x below either topk would change the result.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '1s') matching_groups_left{host="x"} / on(host) group_left topk(1, max by(host)(matching_groups_right));
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '1s') matching_groups_left{host="x"} / on(host) group_left topk by(host)(1, topk(1, max by(host)(matching_groups_right)));

-- Both one-sides carry two series per `host`. Prometheus rejects this with "many-to-many
-- matching not allowed: matching labels must be unique on one side"; GreptimeDB has no
-- cardinality check (#9209) and returns the cross product. The rewrite leaves these operands
-- alone, so the recorded output is the unchanged baseline.
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '1s') matching_groups_left{host="x"} / on(host) group_left matching_groups_right;
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 0, '1s') matching_groups_left{host="x"} / on(host) group_left max by(host,zone)(matching_groups_right);

DROP TABLE matching_groups_left;
DROP TABLE matching_groups_right;
