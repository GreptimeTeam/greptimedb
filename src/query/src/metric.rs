//! query engine metrics

pub static METRIC_PARSE_SQL_ELAPSED: &str = "query.parse_sql_elapsed";
pub static METRIC_OPTIMIZE_LOGICAL_ELAPSED: &str = "query.optimize_logicalplan_elapsed";
pub static METRIC_OPTIMIZE_PHYSICAL_ELAPSED: &str = "query.optimize_physicalplan_elapsed";
pub static METRIC_CREATE_PHYSICAL_ELAPSED: &str = "query.create_physicalplan_elapsed";
pub static METRIC_EXEC_PLAN_ELAPSED: &str = "query.execute_plan_elapsed";
