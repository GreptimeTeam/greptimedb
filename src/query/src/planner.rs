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
use table::table::adapter::DfTableProviderAdapter;

use crate::{
    catalog::{CatalogListRef, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME},
    error::{PlannerSnafu, Result},
    plan::LogicalPlan,
};

pub trait Planner: Send + Sync {
    fn statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan>;
}

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
            .context(PlannerSnafu { sql })?;

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

pub(crate) struct DfContextProviderAdapter<'a> {
    catalog_list: &'a CatalogListRef,
}

impl<'a> DfContextProviderAdapter<'a> {
    pub(crate) fn new(catalog_list: &'a CatalogListRef) -> Self {
        Self { catalog_list }
    }
}

impl<'a> ContextProvider for DfContextProviderAdapter<'a> {
    fn get_table_provider(&self, name: TableReference) -> Option<Arc<dyn TableProvider>> {
        let (catalog, schema, table) = match name {
            TableReference::Bare { table } => (DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, table),
            TableReference::Partial { schema, table } => (DEFAULT_CATALOG_NAME, schema, table),
            TableReference::Full {
                catalog,
                schema,
                table,
            } => (catalog, schema, table),
        };

        self.catalog_list
            .catalog(catalog)
            .and_then(|catalog_provider| catalog_provider.schema(schema))
            .and_then(|schema_provider| schema_provider.table(table))
            .map(|table| Arc::new(DfTableProviderAdapter::new(table)) as _)
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        // TODO(dennis)
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        // TODO(dennis)
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        // TODO(dennis)
        None
    }
}
