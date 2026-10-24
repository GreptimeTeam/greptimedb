-- Legacy SQLness regression: these TQL EVAL cases pin the legacy PromQL planner.
-- The native fused-series route is verified by test_prometheus_native_promql_route and
-- test_native_promql_route_plans_the_query in tests-integration/tests/http.rs.
CREATE TABLE native_scalar_entry (
    ts TIMESTAMP(3) TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE,
);

INSERT INTO native_scalar_entry VALUES
    (0, 'host1', 1.0),
    (0, 'host2', 2.0),
    (1000, 'host1', 3.0),
    (1000, 'host2', 4.0);

TQL EVAL (0, 2, '1s') 1 + 2;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 2, '1s') native_scalar_entry * 100;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 2, '1s') -native_scalar_entry;

DROP TABLE native_scalar_entry;
