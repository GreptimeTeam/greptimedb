-- The native fused-series route renders the tree it runs as ordinary plan lines: one line per native
-- node, two spaces per level, children under the node that consumes them. `EXPLAIN` shows the
-- structure, `ANALYZE` the same lines with each node's own figures.
--
-- The route needs both the server option and the per-request hint; this case runs under
-- `--enable-native-promql`, which sets both.

CREATE TABLE native_ea(i DOUBLE, j TIMESTAMP TIME INDEX, k STRING PRIMARY KEY);

CREATE TABLE native_ebb(i DOUBLE, j TIMESTAMP TIME INDEX, k STRING PRIMARY KEY);

INSERT INTO native_ea VALUES (1.0, 1000, "a"), (2.0, 2000, "a"), (4.0, 1000, "b"), (8.0, 2000, "b");

INSERT INTO native_ebb VALUES (1.0, 1000, "a"), (3.0, 2000, "a"), (5.0, 1000, "b"), (7.0, 2000, "b");

-- An aggregate chain: the aggregate folds the window, which reads the scan. The aggregate is split
-- across the two nodes of a standalone instance, so `EXPLAIN` shows the final half here and
-- `ANALYZE` shows the partial half as the remote stage below it.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
TQL EXPLAIN (0, 10, '5s') sum by(k)(rate(native_ea[10s]));

-- Verbose explains the same plan and still reports no figures: nothing ran.
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
TQL EXPLAIN VERBOSE (0, 10, '5s') sum by(k)(rate(native_ea[10s]));

-- The same tree under ANALYZE: every figure sits on the node that recorded it.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (SeriesScanExec:.*) SeriesScanExec REDACTED
-- SQLNESS REPLACE (self_compute_time_ns:\s)[^,\]\n]+ ${1}REDACTED
-- SQLNESS REPLACE (current_reserved_bytes:\s)[^,\]\n]+ ${1}REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (peak_reserved_bytes:\s)[^,\]\n]+ ${1}REDACTED
-- SQLNESS REPLACE ((?m)\x20+\|$) |
TQL ANALYZE (0, 10, '5s') sum by(k)(rate(native_ea[10s]));

-- A three-level chain without an aggregate: the map reads the window, which reads the scan.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (SeriesScanExec:.*) SeriesScanExec REDACTED
-- SQLNESS REPLACE (self_compute_time_ns:\s)[^,\]\n]+ ${1}REDACTED
-- SQLNESS REPLACE (current_reserved_bytes:\s)[^,\]\n]+ ${1}REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (peak_reserved_bytes:\s)[^,\]\n]+ ${1}REDACTED
-- SQLNESS REPLACE ((?m)\x20+\|$) |
TQL ANALYZE (0, 10, '5s') abs(rate(native_ea[10s]));

-- A binary operation is one native node with two children, each with its own figures.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (SeriesScanExec:.*) SeriesScanExec REDACTED
-- SQLNESS REPLACE (self_compute_time_ns:\s)[^,\]\n]+ ${1}REDACTED
-- SQLNESS REPLACE (current_reserved_bytes:\s)[^,\]\n]+ ${1}REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (peak_reserved_bytes:\s)[^,\]\n]+ ${1}REDACTED
-- SQLNESS REPLACE ((?m)\x20+\|$) |
TQL EXPLAIN (0, 10, '5s') native_ea + native_ebb;

-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (SeriesScanExec:.*) SeriesScanExec REDACTED
-- SQLNESS REPLACE (self_compute_time_ns:\s)[^,\]\n]+ ${1}REDACTED
-- SQLNESS REPLACE (current_reserved_bytes:\s)[^,\]\n]+ ${1}REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (peak_reserved_bytes:\s)[^,\]\n]+ ${1}REDACTED
-- SQLNESS REPLACE ((?m)\x20+\|$) |
TQL ANALYZE (0, 10, '5s') native_ea + native_ebb;

-- A set operation reads both sides too.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (SeriesScanExec:.*) SeriesScanExec REDACTED
-- SQLNESS REPLACE (self_compute_time_ns:\s)[^,\]\n]+ ${1}REDACTED
-- SQLNESS REPLACE (current_reserved_bytes:\s)[^,\]\n]+ ${1}REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (peak_reserved_bytes:\s)[^,\]\n]+ ${1}REDACTED
-- SQLNESS REPLACE ((?m)\x20+\|$) |
TQL ANALYZE (0, 10, '5s') native_ea and native_ebb;

DROP TABLE native_ea;

DROP TABLE native_ebb;
