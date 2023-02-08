// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_error::prelude::BoxedError;
use common_query::logical_plan::create_aggregate_function;
use datafusion::catalog::TableReference;
use datafusion::error::Result as DfResult;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::sql::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::ScalarValue;
use datafusion_expr::TableSource;
use datatypes::arrow::datatypes::DataType;
use datatypes::prelude::DataType as DataTypeTrait;
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::statements::explain::Explain;
use sql::statements::query::Query;
use sql::statements::statement::Statement;

use crate::datafusion::error;
use crate::error::{QueryPlanSnafu, Result};
use crate::plan::LogicalPlan;
use crate::planner::Planner;
use crate::query_engine::QueryEngineState;

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
        let mut context = PlannerContext::new_with_prepare_param_data_types(
            query
                .param_types()
                .iter()
                .map(|v| v.as_arrow_type())
                .collect(),
        );
        let result = self
            .sql_to_rel
            .query_to_plan(query.inner, &mut context)
            .context(error::PlanSqlSnafu { sql })
            .map_err(BoxedError::new)
            .context(QueryPlanSnafu)?;

        Ok(LogicalPlan::DfPlan(result))
    }

    /// Converts EXPLAIN statement to logical plan.
    pub fn explain_to_plan(&self, explain: Explain) -> Result<LogicalPlan> {
        let result = self
            .sql_to_rel
            .sql_statement_to_plan(explain.inner.clone())
            .context(error::PlanSqlSnafu {
                sql: explain.to_string(),
            })
            .map_err(BoxedError::new)
            .context(QueryPlanSnafu)?;

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
            Statement::Query(qb) => self.query_to_plan(qb),
            Statement::Explain(explain) => self.explain_to_plan(explain),
            Statement::ShowTables(_)
            | Statement::Delete(_)
            | Statement::ShowDatabases(_)
            | Statement::ShowCreateTable(_)
            | Statement::DescribeTable(_)
            | Statement::CreateTable(_)
            | Statement::CreateDatabase(_)
            | Statement::Alter(_)
            | Statement::Insert(_)
            | Statement::DropTable(_)
            | Statement::Use(_)
            | Statement::Copy(_) => unreachable!(),
        }
    }
}

pub struct DfContextProviderAdapter {
    state: QueryEngineState,
    query_ctx: QueryContextRef,
}

impl DfContextProviderAdapter {
    pub fn new(state: QueryEngineState, query_ctx: QueryContextRef) -> Self {
        Self { state, query_ctx }
    }
}

impl ContextProvider for DfContextProviderAdapter {
    fn get_table_provider(&self, name: TableReference) -> DfResult<Arc<dyn TableSource>> {
        self.state.get_table_provider(self.query_ctx.clone(), name)
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.get_function_meta(name)
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_function(name).map(|func| {
            Arc::new(
                create_aggregate_function(func.name(), func.args_count(), func.create()).into(),
            )
        })
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        self.state.get_variable_type(variable_names)
    }

    fn get_config_option(&self, variable: &str) -> Option<ScalarValue> {
        self.state.get_config_option(variable)
    }
}
