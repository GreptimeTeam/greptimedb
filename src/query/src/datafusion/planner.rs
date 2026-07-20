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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use arrow_schema::DataType;
use catalog::table_source::DfTableSourceProvider;
use common_function::function::FunctionContext;
use datafusion::catalog::TableFunctionArgs;
use datafusion::common::{DFSchema, TableReference};
use datafusion::datasource::cte_worktable::CteWorkTable;
use datafusion::datasource::file_format::{FileFormatFactory, format_as_file_type};
use datafusion::datasource::provider_as_source;
use datafusion::error::Result as DfResult;
use datafusion::execution::SessionStateDefaults;
use datafusion::execution::context::SessionState;
use datafusion::sql::planner::ContextProvider;
use datafusion::variable::VarType;
use datafusion_common::DataFusionError;
use datafusion_common::config::ConfigOptions;
use datafusion_common::file_options::file_type::FileType;
use datafusion_expr::planner::{ExprPlanner, TypePlanner};
use datafusion_expr::var_provider::is_system_variables;
use datafusion_expr::{AggregateUDF, HigherOrderUDF, ScalarUDF, TableSource, WindowUDF};
use datafusion_sql::parser::Statement as DfStatement;
use session::context::QueryContextRef;
use snafu::{Location, ResultExt};

use crate::datafusion::json_expr_planner::JsonExprPlanner;
use crate::error::{CatalogSnafu, Result};
use crate::query_engine::{DefaultPlanDecoder, QueryEngineState};

pub struct DfContextProviderAdapter {
    engine_state: Arc<QueryEngineState>,
    session_state: SessionState,
    tables: HashMap<String, Arc<dyn TableSource>>,
    table_provider: DfTableSourceProvider,
    query_ctx: QueryContextRef,

    // Fields from session state defaults:
    /// Holds registered external FileFormat implementations
    /// DataFusion doesn't pub this field, so we need to store it here.
    file_formats: HashMap<String, Arc<dyn FileFormatFactory>>,
    /// Provides support for customising the SQL planner, e.g. to add support for custom operators like `->>` or `?`
    /// DataFusion doesn't pub this field, so we need to store it here.
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
}

impl DfContextProviderAdapter {
    pub(crate) async fn try_new(
        engine_state: Arc<QueryEngineState>,
        session_state: SessionState,
        df_stmt: Option<&DfStatement>,
        query_ctx: QueryContextRef,
    ) -> Result<Self> {
        let table_names = if let Some(df_stmt) = df_stmt {
            session_state.resolve_table_references(df_stmt)?
        } else {
            vec![]
        };

        let mut table_provider = DfTableSourceProvider::new(
            engine_state.catalog_manager().clone(),
            engine_state.disallow_cross_catalog_query(),
            query_ctx.clone(),
            Arc::new(DefaultPlanDecoder::new(session_state.clone(), &query_ctx)?),
            session_state
                .config_options()
                .sql_parser
                .enable_ident_normalization,
        );

        let tables = resolve_tables(table_names, &mut table_provider).await?;
        let file_formats = SessionStateDefaults::default_file_formats()
            .into_iter()
            .map(|format| (format.get_ext().to_lowercase(), format))
            .collect();

        let mut expr_planners = SessionStateDefaults::default_expr_planners();
        expr_planners.insert(0, Arc::new(JsonExprPlanner));

        Ok(Self {
            engine_state,
            session_state,
            tables,
            table_provider,
            query_ctx,
            file_formats,
            expr_planners,
        })
    }
}

async fn resolve_tables(
    table_names: Vec<TableReference>,
    table_provider: &mut DfTableSourceProvider,
) -> Result<HashMap<String, Arc<dyn TableSource>>> {
    let mut tables = HashMap::with_capacity(table_names.len());

    for table_name in table_names {
        let resolved_name = table_provider
            .resolve_table_ref(table_name.clone())
            .context(CatalogSnafu)?;

        if let Entry::Vacant(v) = tables.entry(resolved_name.to_string()) {
            // Try our best to resolve the tables here, but we don't return an error if table is not found,
            // because the table name may be a temporary name of CTE, they can't be found until plan
            // execution.
            match table_provider.resolve_table(table_name).await {
                Ok(table) => {
                    let _ = v.insert(table);
                }
                Err(e) if e.should_fail() => {
                    return Err(e).context(CatalogSnafu);
                }
                _ => {
                    // ignore
                }
            }
        }
    }
    Ok(tables)
}

impl ContextProvider for DfContextProviderAdapter {
    fn get_table_source(&self, name: TableReference) -> DfResult<Arc<dyn TableSource>> {
        let table_ref = self.table_provider.resolve_table_ref(name)?;
        self.tables
            .get(&table_ref.to_string())
            .cloned()
            .ok_or_else(|| {
                crate::error::Error::TableNotFound {
                    table: table_ref.to_string(),
                    location: Location::default(),
                }
                .into()
            })
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.engine_state.scalar_function(name).map_or_else(
            || self.session_state.scalar_functions().get(name).cloned(),
            |func| {
                Some(Arc::new(func.provide(FunctionContext {
                    query_ctx: self.query_ctx.clone(),
                    state: self.engine_state.function_state(),
                })))
            },
        )
    }

    fn get_higher_order_meta(&self, name: &str) -> Option<Arc<HigherOrderUDF>> {
        self.session_state
            .higher_order_functions()
            .get(name)
            .cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.engine_state.aggr_function(name).map_or_else(
            || self.session_state.aggregate_functions().get(name).cloned(),
            |func| Some(Arc::new(func)),
        )
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.session_state.window_functions().get(name).cloned()
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

    fn udf_names(&self) -> Vec<String> {
        let mut names = self.engine_state.scalar_names();
        names.extend(self.session_state.scalar_functions().keys().cloned());
        names
    }

    fn higher_order_function_names(&self) -> Vec<String> {
        self.session_state
            .higher_order_functions()
            .keys()
            .cloned()
            .collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        let mut names = self.engine_state.aggr_names();
        names.extend(self.session_state.aggregate_functions().keys().cloned());
        names
    }

    fn udwf_names(&self) -> Vec<String> {
        self.session_state
            .window_functions()
            .keys()
            .cloned()
            .collect()
    }

    fn get_file_type(&self, ext: &str) -> DfResult<Arc<dyn FileType>> {
        self.file_formats
            .get(&ext.to_lowercase())
            .ok_or_else(|| {
                DataFusionError::Plan(format!("There is no registered file format with ext {ext}"))
            })
            .map(|file_type| format_as_file_type(Arc::clone(file_type)))
    }

    fn get_table_function_source(
        &self,
        name: &str,
        args: Vec<datafusion_expr::Expr>,
    ) -> DfResult<Arc<dyn TableSource>> {
        // Constant-fold the args before resolving the table function. DataFusion's
        // SQL planner does not fold table-function arguments (constant folding
        // happens later, in the analyzer), but table functions such as
        // `generate_series`/`range` are resolved during planning and require
        // literal bounds. Folding here lets immutable-UDF bounds like
        // `array_upper(ARRAY[...], 1)` reach them as concrete literals.
        // Non-constant args are returned unchanged by the simplifier.
        let simplify_info = datafusion_expr::simplify::SimplifyContext::builder()
            .with_config_options(Arc::clone(self.session_state.config_options()))
            .with_query_execution_start_time(
                self.session_state
                    .execution_props()
                    .query_execution_start_time,
            )
            .build();
        let simplifier =
            datafusion_optimizer::simplify_expressions::ExprSimplifier::new(simplify_info);
        let schema = DFSchema::empty();
        let args = args
            .into_iter()
            .map(|arg| {
                simplifier
                    .coerce(arg, &schema)
                    .and_then(|arg| simplifier.simplify(arg))
            })
            .collect::<DfResult<Vec<_>>>()?;
        let table_args = TableFunctionArgs::new(&args, &self.session_state);
        let tbl_func = if let Some(tbl_func) = self.engine_state.table_function(name) {
            tbl_func
        } else {
            self.session_state
                .table_functions()
                .get(name)
                .cloned()
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("table function '{name}' not found"))
                })?
        };
        let provider = tbl_func.create_table_provider_with_args(table_args)?;

        Ok(provider_as_source(provider))
    }

    fn create_cte_work_table(
        &self,
        name: &str,
        schema: arrow_schema::SchemaRef,
    ) -> DfResult<Arc<dyn TableSource>> {
        let table = Arc::new(CteWorkTable::new(name, schema));
        Ok(provider_as_source(table))
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expr_planners
    }

    fn get_type_planner(&self) -> Option<Arc<dyn TypePlanner>> {
        // Provide the SQL planner with Postgres oid-alias type names
        // (`regclass`, `regproc`, `regtype`, `regnamespace`, `oid`, ...) and
        // `pg_catalog.`-qualified builtins. DataFusion rejects these as
        // "Unsupported SQL type" otherwise. The planner maps each to its Arrow
        // type so reverse / column-operand casts like `prorettype::regtype::text`
        // parse. Forward name->oid casts (`'x'::regclass`) are resolved earlier,
        // at SQL-parse time, by the `PostgresCompatibilityParser`'s built-in
        // `RewriteRegCastToSubquery` rule.
        // Stateless, so a fresh instance per query is cheap.
        Some(Arc::new(
            datafusion_pg_catalog::pg_catalog::oid_type_planner::PgOidTypePlanner,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use common_base::Plugins;
    use datafusion::catalog::{TableFunction, TableFunctionArgs, TableFunctionImpl, TableProvider};
    use datafusion::datasource::MemTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::context::{SessionConfig, SessionContext, SessionState};
    use datafusion_common::ScalarValue;
    use datafusion_expr::expr::BinaryExpr;
    use datafusion_expr::{Expr, Operator, lit};
    use session::context::QueryContext;

    use super::*;
    use crate::options::QueryOptions;

    #[derive(Debug, Default)]
    struct RecordingTableFunction {
        called: AtomicBool,
        args: Mutex<Vec<Expr>>,
        target_partitions: Mutex<Option<usize>>,
    }

    impl TableFunctionImpl for RecordingTableFunction {
        fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
            let session_state = args
                .session()
                .as_any()
                .downcast_ref::<SessionState>()
                .expect("table function must receive the SessionState");
            *self.args.lock().unwrap() = args.exprs().to_vec();
            *self.target_partitions.lock().unwrap() =
                Some(session_state.config().target_partitions());
            self.called.store(true, Ordering::SeqCst);

            Ok(Arc::new(MemTable::try_new(
                Arc::new(arrow_schema::Schema::empty()),
                vec![vec![]],
            )?))
        }
    }

    fn query_engine_state() -> Arc<QueryEngineState> {
        Arc::new(QueryEngineState::new(
            catalog::memory::new_memory_catalog_manager().unwrap(),
            None,
            None,
            None,
            None,
            None,
            false,
            Plugins::default(),
            QueryOptions::default(),
        ))
    }

    async fn context_provider(
        engine_state: Arc<QueryEngineState>,
        session_state: SessionState,
    ) -> DfContextProviderAdapter {
        DfContextProviderAdapter::try_new(engine_state, session_state, None, QueryContext::arc())
            .await
            .unwrap()
    }

    fn plus(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::Plus,
            right: Box::new(right),
        })
    }

    #[tokio::test]
    async fn table_function_arguments_are_folded_before_engine_function_creation() {
        let engine_state = query_engine_state();
        let function = Arc::new(RecordingTableFunction::default());
        engine_state.register_table_function(Arc::new(TableFunction::new(
            "capture_engine_args".to_string(),
            function.clone(),
        )));
        let provider = context_provider(engine_state.clone(), engine_state.session_state()).await;

        provider
            .get_table_function_source("capture_engine_args", vec![plus(lit(1_i64), lit(2_i64))])
            .unwrap();

        assert!(function.called.load(Ordering::SeqCst));
        assert_eq!(
            *function.args.lock().unwrap(),
            vec![Expr::Literal(ScalarValue::Int64(Some(3)), None)]
        );
    }

    #[tokio::test]
    async fn table_function_argument_simplification_errors_are_propagated() {
        let engine_state = query_engine_state();
        let function = Arc::new(RecordingTableFunction::default());
        engine_state.register_table_function(Arc::new(TableFunction::new(
            "reject_invalid_args".to_string(),
            function.clone(),
        )));
        let provider = context_provider(engine_state.clone(), engine_state.session_state()).await;

        let error = match provider
            .get_table_function_source("reject_invalid_args", vec![plus(lit(true), lit(1_i64))])
        {
            Ok(_) => panic!("invalid table-function argument must fail planning"),
            Err(error) => error,
        };

        assert!(!error.to_string().is_empty());
        assert!(!function.called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn session_table_function_receives_table_function_args_session() {
        let engine_state = query_engine_state();
        let session_state = SessionStateBuilder::new_from_existing(engine_state.session_state())
            .with_config(SessionConfig::new().with_target_partitions(7))
            .build();
        let session_context = SessionContext::new_with_state(session_state);
        let function = Arc::new(RecordingTableFunction::default());
        session_context.register_udtf("capture_session_args", function.clone());
        let provider = context_provider(engine_state, session_context.state()).await;

        provider
            .get_table_function_source("capture_session_args", vec![lit(1_i64)])
            .unwrap();

        assert!(function.called.load(Ordering::SeqCst));
        assert_eq!(*function.target_partitions.lock().unwrap(), Some(7));
    }
}
