//! query engine metrics

pub static METRIC_PARSE_SQL_USEDTIME: &str = "query.parse_sql_usedtime";
pub static METRIC_OPTIMIZE_LOGICAL_USEDTIME: &str = "query.optimize_logicalplan_usedtime";
pub static METRIC_OPTIMIZE_PHYSICAL_USEDTIME: &str = "query.optimize_physicalplan_usedtime";
pub static METRIC_CREATE_PHYSICAL_USEDTIME: &str = "query.create_physicalplan_usedtime";
pub static METRIC_EXEC_PLAN_USEDTIME: &str = "query.execute_plan_usedtime";
