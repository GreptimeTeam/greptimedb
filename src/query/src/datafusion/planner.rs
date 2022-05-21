use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::catalog::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use snafu::ResultExt;
use sql::statements::query::Query;
use sql::statements::statement::Statement;

use crate::query_engine::QueryEngineState;
use crate::{datafusion::error, error::Result, plan::LogicalPlan, planner::Planner};

pub struct DfPlanner<'a, S: ContextProvider> {
    sql_to_rel: SqlToRel<'a, S>,
}

impl<'a, S: ContextProvider + Send + Sync> DfPlanner<'a, S> {
    /// Creates a DataFusion planner instance
    pub fn new(schema_provider: &'a S) -> Self {
        let rel = SqlToRel::new(schema_provider);
        Self { sql_to_rel: rel }
    }

    /// Converts QUERY statement to logical plan.
    pub fn query_to_plan(&self, query: Box<Query>) -> Result<LogicalPlan> {
        // todo(hl): original SQL should be provided as an argument
        let sql = query.inner.to_string();
        let result = self
            .sql_to_rel
            .query_to_plan(query.inner)
            .context(error::PlanSqlSnafu { sql })?;

        Ok(LogicalPlan::DfPlan(result))
    }
}

impl<'a, S> Planner for DfPlanner<'a, S>
where
    S: ContextProvider + Send + Sync,
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

pub(crate) struct DfContextProviderAdapter {
    state: QueryEngineState,
}

impl DfContextProviderAdapter {
    pub(crate) fn new(state: QueryEngineState) -> Self {
        Self { state }
    }
}

/// TODO(dennis): Delegate all requests to ExecutionContext right now,
///                           manage UDFs, UDAFs, variables by ourself in future.
impl ContextProvider for DfContextProviderAdapter {
    fn get_table_provider(&self, name: TableReference) -> Option<Arc<dyn TableProvider>> {
        self.state
            .df_context()
            .state
            .lock()
            .get_table_provider(name)
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.df_context().state.lock().get_function_meta(name)
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state
            .df_context()
            .state
            .lock()
            .get_aggregate_meta(name)
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        self.state
            .df_context()
            .state
            .lock()
            .get_variable_type(variable_names)
    }
}
