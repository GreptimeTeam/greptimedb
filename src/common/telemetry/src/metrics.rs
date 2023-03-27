pub static METRIC_PARSE_SQL_ELAPSED: &str = "query.parse_sql_elapsed";
pub static METRIC_PARSE_PROMQL_ELAPSED: &str = "query.parse_promql_elapsed";
pub static METRIC_OPTIMIZE_LOGICAL_ELAPSED: &str = "query.optimize_logicalplan_elapsed";
pub static METRIC_OPTIMIZE_PHYSICAL_ELAPSED: &str = "query.optimize_physicalplan_elapsed";
pub static METRIC_CREATE_PHYSICAL_ELAPSED: &str = "query.create_physicalplan_elapsed";
pub static METRIC_EXEC_PLAN_ELAPSED: &str = "query.execute_plan_elapsed";

pub const THREAD_NAME_LABEL: &str = "thread.name";
pub const METRIC_RUNTIME_THREADS_ALIVE: &str = "runtime.threads.alive";
pub const METRIC_RUNTIME_THREADS_IDLE: &str = "runtime.threads.idle";

pub const METRIC_HANDLE_SQL_ELAPSED: &str = "datanode.handle_sql_elapsed";
pub const METRIC_HANDLE_SCRIPTS_ELAPSED: &str = "datanode.handle_scripts_elapsed";
pub const METRIC_RUN_SCRIPT_ELAPSED: &str = "datanode.run_script_elapsed";
pub const METRIC_HANDLE_PROMQL_ELAPSED: &str = "datanode.handle_promql_elapsed";
