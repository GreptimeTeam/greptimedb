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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::DataType;
use catalog::table_source::DfTableSourceProvider;
use common_query::logical_plan::create_aggregate_function;
use datafusion::catalog::TableReference;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::sql::planner::ContextProvider;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, OwnedTableReference};
use datafusion_expr::{TableSource, WindowUDF};
use datafusion_physical_expr::var_provider::{is_system_variables, VarType};
use datafusion_sql::parser::Statement as DfStatement;
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::error::{CatalogSnafu, DataFusionSnafu, Result};
use crate::query_engine::QueryEngineState;

pub struct DfContextProviderAdapter {
    engine_state: Arc<QueryEngineState>,
    session_state: SessionState,
    tables: HashMap<String, Arc<dyn TableSource>>,
    table_provider: DfTableSourceProvider,
}

impl DfContextProviderAdapter {
    pub(crate) async fn try_new(
        engine_state: Arc<QueryEngineState>,
        session_state: SessionState,
        df_stmt: &DfStatement,
        query_ctx: QueryContextRef,
    ) -> Result<Self> {
        let table_names = session_state
            .resolve_table_references(df_stmt)
            .context(DataFusionSnafu)?;

        let mut table_provider = DfTableSourceProvider::new(
            engine_state.catalog_manager().clone(),
            engine_state.disallow_cross_schema_query(),
            query_ctx.as_ref(),
        );

        let tables = resolve_tables(table_names, &mut table_provider).await?;

        Ok(Self {
            engine_state,
            session_state,
            tables,
            table_provider,
        })
    }
}

async fn resolve_tables(
    table_names: Vec<OwnedTableReference>,
    table_provider: &mut DfTableSourceProvider,
) -> Result<HashMap<String, Arc<dyn TableSource>>> {
    let mut tables = HashMap::with_capacity(table_names.len());

    for table_name in table_names {
        let resolved_name = table_provider
            .resolve_table_ref(table_name.clone())
            .context(CatalogSnafu)?;

        if let Entry::Vacant(v) = tables.entry(resolved_name.to_string()) {
            // Try our best to resolve the tables here, but we don't return an error if table is not found,
            // because the table name may be a temporary name of CTE or view, they can't be found until plan
            // execution.
            if let Ok(table) = table_provider.resolve_table(table_name).await {
                let _ = v.insert(table);
            }
        }
    }
    Ok(tables)
}

impl ContextProvider for DfContextProviderAdapter {
    fn get_table_provider(&self, name: TableReference) -> DfResult<Arc<dyn TableSource>> {
        let table_ref = self.table_provider.resolve_table_ref(name)?;
        self.tables
            .get(&table_ref.to_string())
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("Table not found: {}", table_ref)))
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.session_state.scalar_functions().get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.engine_state.aggregate_function(name).map(|func| {
            Arc::new(
                create_aggregate_function(func.name(), func.args_count(), func.create()).into(),
            )
        })
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        if variable_names.is_empty() {
            return None;
        }

        let provider_type = if is_system_variables(variable_names) {
            VarType::System
        } else {
            VarType::UserDefined
        };

        self.session_state
            .execution_props()
            .var_providers
            .as_ref()
            .and_then(|provider| provider.get(&provider_type)?.get_type(variable_names))
    }

    fn options(&self) -> &ConfigOptions {
        self.session_state.config_options()
    }
}
