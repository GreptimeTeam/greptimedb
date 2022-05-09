use sql::statements::statement::Statement;

use crate::{error::Result, plan::LogicalPlan};

/// SQL logical planner.
pub trait Planner: Send + Sync {
    fn statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan>;
}
