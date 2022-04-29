use datafusion::sql::planner::{ContextProvider, SqlToRel};
use snafu::ResultExt;
use sql::statements::query::Query;
use sql::statements::statement::Statement;

use crate::error;
use crate::error::PlannerError;
use crate::plan::LogicalPlan;

pub trait Planner {
    fn statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan>;
}

type Result<T> = std::result::Result<T, PlannerError>;

pub struct DfPlanner<'a, S: ContextProvider> {
    sql_to_rel: SqlToRel<'a, S>,
}

impl<'a, S: ContextProvider> DfPlanner<'a, S> {
    /// Creates a DataFusion planner instance
    pub fn new(schema_provider: &'a S) -> Self {
        let rel = SqlToRel::new(schema_provider);
        Self { sql_to_rel: rel }
    }

    /// Converts QUERY statement to logical plan.
    pub fn query_to_plan(&self, query: Box<Query>) -> Result<LogicalPlan> {
        let sql = query.inner.to_string();
        let result = self
            .sql_to_rel
            .query_to_plan(query.inner)
            .context(error::DfPlanSnafu { sql })?;

        Ok(LogicalPlan::DfPlan(result))
    }
}

impl<'a, S> Planner for DfPlanner<'a, S>
where
    S: ContextProvider,
{
    /// Converts statement to logical plan using datafusion planner
    fn statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        match statement {
            Statement::ShowDatabases(_) => {
                todo!("Currently not supported")
            }
            Statement::Query(qb) => self.query_to_plan(qb),
            Statement::Insert(_) => {
                todo!()
            }
        }
    }
}
