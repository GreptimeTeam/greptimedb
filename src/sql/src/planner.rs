use datafusion::sql::planner::{ContextProvider, SqlToRel};
use query::plan::LogicalPlan;
use snafu::ResultExt;

use crate::errors;
use crate::errors::PlannerError;
use crate::statements::query::Query;
use crate::statements::statement::Statement;

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
            .context(errors::DatafusionSnafu { sql })?;

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

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;

    use super::*;
    use crate::parser::ParserContext;

    #[test]
    pub fn test_plan_select() {
        let sql = "SELECT * FROM table_1";
        let statements = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, statements.len());
    }
}
