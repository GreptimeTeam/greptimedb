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

use std::any::Any;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use arrow_schema::DataType;
use async_trait::async_trait;
use catalog::table_source::DfTableSourceProvider;
use common_error::ext::BoxedError;
use common_telemetry::tracing;
use datafusion::common::{DFSchema, plan_err};
use datafusion::execution::context::SessionState;
use datafusion::sql::planner::PlannerContext;
use datafusion_common::ToDFSchema;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::{
    Analyze, Explain, ExplainFormat, Expr as DfExpr, LogicalPlan, LogicalPlanBuilder, PlanType,
    ToStringifiedPlan, col,
};
use datafusion_sql::planner::{ParserOptions, SqlToRel};
use log_query::LogQuery;
use promql_parser::parser::EvalStmt;
use session::context::QueryContextRef;
use snafu::{ResultExt, ensure};
use sql::CteContent;
use sql::ast::Expr as SqlExpr;
use sql::statements::explain::ExplainStatement;
use sql::statements::query::Query;
use sql::statements::statement::Statement;
use sql::statements::tql::Tql;

use crate::error::{
    CteColumnSchemaMismatchSnafu, PlanSqlSnafu, QueryPlanSnafu, Result, SqlSnafu,
    UnimplementedSnafu,
};
use crate::log_query::planner::LogQueryPlanner;
use crate::parser::{DEFAULT_LOOKBACK_STRING, PromQuery, QueryLanguageParser, QueryStatement};
use crate::promql::planner::PromPlanner;
use crate::query_engine::{DefaultPlanDecoder, QueryEngineState};
use crate::range_select::plan_rewrite::RangePlanRewriter;
use crate::{DfContextProviderAdapter, QueryEngineContext};

#[async_trait]
pub trait LogicalPlanner: Send + Sync {
    async fn plan(&self, stmt: &QueryStatement, query_ctx: QueryContextRef) -> Result<LogicalPlan>;

    async fn plan_logs_query(
        &self,
        query: LogQuery,
        query_ctx: QueryContextRef,
    ) -> Result<LogicalPlan>;

    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan>;

    fn as_any(&self) -> &dyn Any;
}

pub struct DfLogicalPlanner {
    engine_state: Arc<QueryEngineState>,
    session_state: SessionState,
}

impl DfLogicalPlanner {
    pub fn new(engine_state: Arc<QueryEngineState>) -> Self {
        let session_state = engine_state.session_state();
        Self {
            engine_state,
            session_state,
        }
    }

    /// Basically the same with `explain_to_plan` in DataFusion, but adapted to Greptime's
    /// `plan_sql` to support Greptime Statements.
    async fn explain_to_plan(
        &self,
        explain: &ExplainStatement,
        query_ctx: QueryContextRef,
    ) -> Result<LogicalPlan> {
        let plan = self.plan_sql(&explain.statement, query_ctx).await?;
        if matches!(plan, LogicalPlan::Explain(_)) {
            return plan_err!("Nested EXPLAINs are not supported").context(PlanSqlSnafu);
        }

        let verbose = explain.verbose;
        let analyze = explain.analyze;
        let format = explain.format.map(|f| f.to_string());

        let plan = Arc::new(plan);
        let schema = LogicalPlan::explain_schema();
        let schema = ToDFSchema::to_dfschema_ref(schema)?;

        if verbose && format.is_some() {
            return plan_err!("EXPLAIN VERBOSE with FORMAT is not supported").context(PlanSqlSnafu);
        }

        if analyze {
            // notice format is already set in query context, so can be ignore here
            Ok(LogicalPlan::Analyze(Analyze {
                verbose,
                input: plan,
                schema,
            }))
        } else {
            let stringified_plans = vec![plan.to_stringified(PlanType::InitialLogicalPlan)];

            // default to configuration value
            let options = self.session_state.config().options();
            let format = format
                .map(|x| ExplainFormat::from_str(&x))
                .transpose()?
                .unwrap_or_else(|| options.explain.format.clone());

            Ok(LogicalPlan::Explain(Explain {
                verbose,
                explain_format: format,
                plan,
                stringified_plans,
                schema,
                logical_optimization_succeeded: false,
            }))
        }
    }

    #[tracing::instrument(skip_all)]
    #[async_recursion::async_recursion]
    async fn plan_sql(&self, stmt: &Statement, query_ctx: QueryContextRef) -> Result<LogicalPlan> {
        let mut planner_context = PlannerContext::new();
        let mut stmt = Cow::Borrowed(stmt);
        let mut is_tql_cte = false;

        // handle explain before normal processing so we can explain Greptime Statements
        if let Statement::Explain(explain) = stmt.as_ref() {
            return self.explain_to_plan(explain, query_ctx).await;
        }

        // Check for hybrid CTEs before normal processing
        if self.has_hybrid_ctes(stmt.as_ref()) {
            let stmt_owned = stmt.into_owned();
            let mut query = match stmt_owned {
                Statement::Query(query) => query.as_ref().clone(),
                _ => unreachable!("has_hybrid_ctes should only return true for Query statements"),
            };
            self.plan_query_with_hybrid_ctes(&query, query_ctx.clone(), &mut planner_context)
                .await?;

            // remove the processed TQL CTEs from the query
            query.hybrid_cte = None;
            stmt = Cow::Owned(Statement::Query(Box::new(query)));
            is_tql_cte = true;
        }

        let mut df_stmt = stmt.as_ref().try_into().context(SqlSnafu)?;

        // TODO(LFC): Remove this when Datafusion supports **both** the syntax and implementation of "explain with format".
        if let datafusion::sql::parser::Statement::Statement(
            box datafusion::sql::sqlparser::ast::Statement::Explain { .. },
        ) = &mut df_stmt
        {
            UnimplementedSnafu {
                operation: "EXPLAIN with FORMAT using raw datafusion planner",
            }
            .fail()?;
        }

        let table_provider = DfTableSourceProvider::new(
            self.engine_state.catalog_manager().clone(),
            self.engine_state.disallow_cross_catalog_query(),
            query_ctx.clone(),
            Arc::new(DefaultPlanDecoder::new(
                self.session_state.clone(),
                &query_ctx,
            )?),
            self.session_state
                .config_options()
                .sql_parser
                .enable_ident_normalization,
        );

        let context_provider = DfContextProviderAdapter::try_new(
            self.engine_state.clone(),
            self.session_state.clone(),
            Some(&df_stmt),
            query_ctx.clone(),
        )
        .await?;

        let config_options = self.session_state.config().options();
        let parser_options = &config_options.sql_parser;
        let parser_options = ParserOptions {
            map_string_types_to_utf8view: false,
            ..parser_options.into()
        };

        let sql_to_rel = SqlToRel::new_with_options(&context_provider, parser_options);

        // this IF is to handle different version of ASTs
        let result = if is_tql_cte {
            let Statement::Query(query) = stmt.into_owned() else {
                unreachable!("is_tql_cte should only be true for Query statements");
            };
            let sqlparser_stmt = sqlparser::ast::Statement::Query(Box::new(query.inner));
            sql_to_rel
                .sql_statement_to_plan_with_context(sqlparser_stmt, &mut planner_context)
                .context(PlanSqlSnafu)?
        } else {
            sql_to_rel
                .statement_to_plan(df_stmt)
                .context(PlanSqlSnafu)?
        };

        common_telemetry::debug!("Logical planner, statement to plan result: {result}");
        let plan = RangePlanRewriter::new(table_provider, query_ctx.clone())
            .rewrite(result)
            .await?;

        // Optimize logical plan by extension rules
        let context = QueryEngineContext::new(self.session_state.clone(), query_ctx);
        let plan = self
            .engine_state
            .optimize_by_extension_rules(plan, &context)?;
        common_telemetry::debug!("Logical planner, optimize result: {plan}");

        Ok(plan)
    }

    /// Generate a relational expression from a SQL expression
    #[tracing::instrument(skip_all)]
    pub(crate) async fn sql_to_expr(
        &self,
        sql: SqlExpr,
        schema: &DFSchema,
        normalize_ident: bool,
        query_ctx: QueryContextRef,
    ) -> Result<DfExpr> {
        let context_provider = DfContextProviderAdapter::try_new(
            self.engine_state.clone(),
            self.session_state.clone(),
            None,
            query_ctx,
        )
        .await?;

        let config_options = self.session_state.config().options();
        let parser_options = &config_options.sql_parser;
        let parser_options: ParserOptions = ParserOptions {
            map_string_types_to_utf8view: false,
            enable_ident_normalization: normalize_ident,
            ..parser_options.into()
        };

        let sql_to_rel = SqlToRel::new_with_options(&context_provider, parser_options);

        Ok(sql_to_rel.sql_to_expr(sql, schema, &mut PlannerContext::new())?)
    }

    #[tracing::instrument(skip_all)]
    async fn plan_pql(&self, stmt: &EvalStmt, query_ctx: QueryContextRef) -> Result<LogicalPlan> {
        let plan_decoder = Arc::new(DefaultPlanDecoder::new(
            self.session_state.clone(),
            &query_ctx,
        )?);
        let table_provider = DfTableSourceProvider::new(
            self.engine_state.catalog_manager().clone(),
            self.engine_state.disallow_cross_catalog_query(),
            query_ctx.clone(),
            plan_decoder,
            self.session_state
                .config_options()
                .sql_parser
                .enable_ident_normalization,
        );
        let plan = PromPlanner::stmt_to_plan(table_provider, stmt, &self.engine_state)
            .await
            .map_err(BoxedError::new)
            .context(QueryPlanSnafu)?;

        let context = QueryEngineContext::new(self.session_state.clone(), query_ctx);
        Ok(self
            .engine_state
            .optimize_by_extension_rules(plan, &context)?)
    }

    #[tracing::instrument(skip_all)]
    fn optimize_logical_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        Ok(self.engine_state.optimize_logical_plan(plan)?)
    }

    /// Check if a statement contains hybrid CTEs (mix of SQL and TQL)
    fn has_hybrid_ctes(&self, stmt: &Statement) -> bool {
        if let Statement::Query(query) = stmt {
            query
                .hybrid_cte
                .as_ref()
                .map(|hybrid_cte| !hybrid_cte.cte_tables.is_empty())
                .unwrap_or(false)
        } else {
            false
        }
    }

    /// Plan a query with hybrid CTEs using DataFusion's native PlannerContext
    async fn plan_query_with_hybrid_ctes(
        &self,
        query: &Query,
        query_ctx: QueryContextRef,
        planner_context: &mut PlannerContext,
    ) -> Result<()> {
        let hybrid_cte = query.hybrid_cte.as_ref().unwrap();

        for cte in &hybrid_cte.cte_tables {
            match &cte.content {
                CteContent::Tql(tql) => {
                    // Plan TQL and register in PlannerContext
                    let mut logical_plan = self.tql_to_logical_plan(tql, query_ctx.clone()).await?;
                    if !cte.columns.is_empty() {
                        let schema = logical_plan.schema();
                        let schema_fields = schema.fields().to_vec();
                        ensure!(
                            schema_fields.len() == cte.columns.len(),
                            CteColumnSchemaMismatchSnafu {
                                cte_name: cte.name.value.clone(),
                                original: schema_fields
                                    .iter()
                                    .map(|field| field.name().clone())
                                    .collect::<Vec<_>>(),
                                expected: cte
                                    .columns
                                    .iter()
                                    .map(|column| column.to_string())
                                    .collect::<Vec<_>>(),
                            }
                        );
                        let aliases = cte
                            .columns
                            .iter()
                            .zip(schema_fields.iter())
                            .map(|(column, field)| col(field.name()).alias(column.to_string()));
                        logical_plan = LogicalPlanBuilder::from(logical_plan)
                            .project(aliases)
                            .context(PlanSqlSnafu)?
                            .build()
                            .context(PlanSqlSnafu)?;
                    }

                    // Wrap in SubqueryAlias to ensure proper table qualification for CTE
                    logical_plan = LogicalPlan::SubqueryAlias(
                        datafusion_expr::SubqueryAlias::try_new(
                            Arc::new(logical_plan),
                            cte.name.value.clone(),
                        )
                        .context(PlanSqlSnafu)?,
                    );

                    planner_context.insert_cte(&cte.name.value, logical_plan);
                }
                CteContent::Sql(_) => {
                    // SQL CTEs should have been moved to the main query's WITH clause
                    // during parsing, so we shouldn't encounter them here
                    unreachable!("SQL CTEs should not be in hybrid_cte.cte_tables");
                }
            }
        }

        Ok(())
    }

    /// Convert TQL to LogicalPlan directly
    async fn tql_to_logical_plan(
        &self,
        tql: &Tql,
        query_ctx: QueryContextRef,
    ) -> Result<LogicalPlan> {
        match tql {
            Tql::Eval(eval) => {
                // Convert TqlEval to PromQuery then to QueryStatement::Promql
                let prom_query = PromQuery {
                    query: eval.query.clone(),
                    start: eval.start.clone(),
                    end: eval.end.clone(),
                    step: eval.step.clone(),
                    lookback: eval
                        .lookback
                        .clone()
                        .unwrap_or_else(|| DEFAULT_LOOKBACK_STRING.to_string()),
                    alias: eval.alias.clone(),
                };
                let stmt = QueryLanguageParser::parse_promql(&prom_query, &query_ctx)?;

                self.plan(&stmt, query_ctx).await
            }
            Tql::Explain(_) => UnimplementedSnafu {
                operation: "TQL EXPLAIN in CTEs",
            }
            .fail(),
            Tql::Analyze(_) => UnimplementedSnafu {
                operation: "TQL ANALYZE in CTEs",
            }
            .fail(),
        }
    }

    /// Extracts cast types for all placeholders in a logical plan.
    /// Returns a map where each placeholder ID is mapped to:
    /// - Some(DataType) if the placeholder is cast to a specific type
    /// - None if the placeholder exists but has no cast
    ///
    /// Example: `$1::TEXT` returns `{"$1": Some(DataType::Utf8)}`
    ///
    /// This function walks through all expressions in the logical plan,
    /// including subqueries, to identify placeholders and their cast types.
    fn extract_placeholder_cast_types(
        plan: &LogicalPlan,
    ) -> Result<HashMap<String, Option<DataType>>> {
        let mut placeholder_types = HashMap::new();
        let mut casted_placeholders = HashSet::new();

        plan.apply(|node| {
            for expr in node.expressions() {
                let _ = expr.apply(|e| {
                    if let DfExpr::Cast(cast) = e
                        && let DfExpr::Placeholder(ph) = &*cast.expr
                    {
                        placeholder_types.insert(ph.id.clone(), Some(cast.data_type.clone()));
                        casted_placeholders.insert(ph.id.clone());
                    }

                    if let DfExpr::Placeholder(ph) = e
                        && !casted_placeholders.contains(&ph.id)
                        && !placeholder_types.contains_key(&ph.id)
                    {
                        placeholder_types.insert(ph.id.clone(), None);
                    }

                    Ok(TreeNodeRecursion::Continue)
                });
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        Ok(placeholder_types)
    }

    /// Gets inferred parameter types from a logical plan.
    /// Returns a map where each parameter ID is mapped to:
    /// - Some(DataType) if the parameter type could be inferred
    /// - None if the parameter type could not be inferred
    ///
    /// This function first uses DataFusion's `get_parameter_types()` to infer types.
    /// If any parameters have `None` values (i.e., DataFusion couldn't infer their types),
    /// it falls back to using `extract_placeholder_cast_types()` to detect explicit casts.
    ///
    /// This is because datafusion can only infer types for a limited cases.
    ///
    /// Example: For query `WHERE $1::TEXT AND $2`, DataFusion may not infer `$2`'s type,
    /// but this function will return `{"$1": Some(DataType::Utf8), "$2": None}`.
    pub fn get_inferred_parameter_types(
        plan: &LogicalPlan,
    ) -> Result<HashMap<String, Option<DataType>>> {
        let param_types = plan.get_parameter_types().context(PlanSqlSnafu)?;

        let has_none = param_types.values().any(|v| v.is_none());

        if !has_none {
            Ok(param_types)
        } else {
            let cast_types = Self::extract_placeholder_cast_types(plan)?;

            let mut merged = param_types;

            for (id, opt_type) in cast_types {
                merged
                    .entry(id)
                    .and_modify(|existing| {
                        if existing.is_none() {
                            *existing = opt_type.clone();
                        }
                    })
                    .or_insert(opt_type);
            }

            Ok(merged)
        }
    }
}

#[async_trait]
impl LogicalPlanner for DfLogicalPlanner {
    #[tracing::instrument(skip_all)]
    async fn plan(&self, stmt: &QueryStatement, query_ctx: QueryContextRef) -> Result<LogicalPlan> {
        match stmt {
            QueryStatement::Sql(stmt) => self.plan_sql(stmt, query_ctx).await,
            QueryStatement::Promql(stmt, _alias) => self.plan_pql(stmt, query_ctx).await,
        }
    }

    async fn plan_logs_query(
        &self,
        query: LogQuery,
        query_ctx: QueryContextRef,
    ) -> Result<LogicalPlan> {
        let plan_decoder = Arc::new(DefaultPlanDecoder::new(
            self.session_state.clone(),
            &query_ctx,
        )?);
        let table_provider = DfTableSourceProvider::new(
            self.engine_state.catalog_manager().clone(),
            self.engine_state.disallow_cross_catalog_query(),
            query_ctx,
            plan_decoder,
            self.session_state
                .config_options()
                .sql_parser
                .enable_ident_normalization,
        );

        let mut planner = LogQueryPlanner::new(table_provider, self.session_state.clone());
        planner
            .query_to_plan(query)
            .await
            .map_err(BoxedError::new)
            .context(QueryPlanSnafu)
    }

    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        self.optimize_logical_plan(plan)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::DataType;
    use catalog::RegisterTableRequest;
    use catalog::memory::MemoryCatalogManager;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use session::context::QueryContext;
    use store_api::metric_engine_consts::{
        DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME, LOGICAL_TABLE_METADATA_KEY,
        METRIC_ENGINE_NAME,
    };
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::test_util::EmptyTable;

    use super::*;
    use crate::parser::{PromQuery, QueryLanguageParser};
    use crate::{QueryEngineFactory, QueryEngineRef};

    async fn create_test_engine() -> QueryEngineRef {
        let columns = vec![
            ColumnSchema::new("id", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new("name", ConcreteDataType::string_datatype(), true),
        ];
        let schema = Arc::new(Schema::new(columns));
        let table_meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![0])
            .value_indices(vec![1])
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::new("test", table_meta).build().unwrap();
        let table = EmptyTable::from_table_info(&table_info);

        crate::tests::new_query_engine_with_table(table)
    }

    fn create_promql_test_engine() -> QueryEngineRef {
        let catalog_manager = MemoryCatalogManager::with_default_setup();
        let physical_table_name = "phy";
        let physical_table_id = 999u32;

        let physical_schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string(),
                ConcreteDataType::uint32_datatype(),
                false,
            ),
            ColumnSchema::new(
                DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
                ConcreteDataType::uint64_datatype(),
                false,
            ),
            ColumnSchema::new("tag_0", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("tag_1", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                "timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("field_0", ConcreteDataType::float64_datatype(), true),
        ]));
        let physical_meta = TableMetaBuilder::empty()
            .schema(physical_schema)
            .primary_key_indices(vec![0, 1, 2, 3])
            .value_indices(vec![4, 5])
            .engine(METRIC_ENGINE_NAME.to_string())
            .next_column_id(1024)
            .build()
            .unwrap();
        let physical_info = TableInfoBuilder::default()
            .table_id(physical_table_id)
            .name(physical_table_name)
            .meta(physical_meta)
            .build()
            .unwrap();
        catalog_manager
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: physical_table_name.to_string(),
                table_id: physical_table_id,
                table: EmptyTable::from_table_info(&physical_info),
            })
            .unwrap();

        let mut options = table::requests::TableOptions::default();
        options.extra_options.insert(
            LOGICAL_TABLE_METADATA_KEY.to_string(),
            physical_table_name.to_string(),
        );
        let logical_schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("tag_0", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("tag_1", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                "timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("field_0", ConcreteDataType::float64_datatype(), true),
        ]));
        let logical_meta = TableMetaBuilder::empty()
            .schema(logical_schema)
            .primary_key_indices(vec![0, 1])
            .value_indices(vec![3])
            .engine(METRIC_ENGINE_NAME.to_string())
            .options(options)
            .next_column_id(1024)
            .build()
            .unwrap();
        let logical_info = TableInfoBuilder::default()
            .table_id(1024)
            .name("some_metric")
            .meta(logical_meta)
            .build()
            .unwrap();
        catalog_manager
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "some_metric".to_string(),
                table_id: 1024,
                table: EmptyTable::from_table_info(&logical_info),
            })
            .unwrap();

        QueryEngineFactory::new(
            catalog_manager,
            None,
            None,
            None,
            None,
            false,
            crate::options::QueryOptions::default(),
        )
        .query_engine()
    }

    async fn parse_sql_to_plan(sql: &str) -> LogicalPlan {
        let stmt = QueryLanguageParser::parse_sql(sql, &QueryContext::arc()).unwrap();
        let engine = create_test_engine().await;
        engine
            .planner()
            .plan(&stmt, QueryContext::arc())
            .await
            .unwrap()
    }

    async fn parse_promql_to_plan(query: &str) -> LogicalPlan {
        let engine = create_promql_test_engine();
        let query_ctx = QueryContext::arc();
        let stmt = QueryLanguageParser::parse_promql(
            &PromQuery {
                query: query.to_string(),
                start: "0".to_string(),
                end: "10".to_string(),
                step: "5s".to_string(),
                lookback: "300s".to_string(),
                alias: None,
            },
            &query_ctx,
        )
        .unwrap();

        engine.planner().plan(&stmt, query_ctx).await.unwrap()
    }

    #[tokio::test]
    async fn test_extract_placeholder_cast_types_multiple() {
        let plan = parse_sql_to_plan(
            "SELECT $1::INT, $2::TEXT, $3, $4::INTEGER FROM test WHERE $5::FLOAT > 0",
        )
        .await;
        let types = DfLogicalPlanner::extract_placeholder_cast_types(&plan).unwrap();

        assert_eq!(types.len(), 5);
        assert_eq!(types.get("$1"), Some(&Some(DataType::Int32)));
        assert_eq!(types.get("$2"), Some(&Some(DataType::Utf8)));
        assert_eq!(types.get("$3"), Some(&None));
        assert_eq!(types.get("$4"), Some(&Some(DataType::Int32)));
        assert_eq!(types.get("$5"), Some(&Some(DataType::Float32)));
    }

    #[tokio::test]
    async fn test_get_inferred_parameter_types_fallback_for_udf_args() {
        // datafusion is not able to infer type for scalar function arguments
        let plan = parse_sql_to_plan(
            "SELECT parse_ident($1), parse_ident($2::TEXT) FROM test WHERE id > $3",
        )
        .await;
        let types = DfLogicalPlanner::get_inferred_parameter_types(&plan).unwrap();

        assert_eq!(types.len(), 3);

        let type_1 = types.get("$1").unwrap();
        let type_2 = types.get("$2").unwrap();
        let type_3 = types.get("$3").unwrap();

        assert!(type_1.is_none(), "Expected $1 to be None");
        assert_eq!(type_2, &Some(DataType::Utf8));
        assert_eq!(type_3, &Some(DataType::Int32));
    }

    #[tokio::test]
    async fn test_plan_pql_applies_extension_rules() {
        for inner_agg in ["count", "sum", "avg", "min", "max", "stddev", "stdvar"] {
            let plan = parse_promql_to_plan(&format!(
                "sum(irate(some_metric[1h])) / scalar(count({inner_agg}(some_metric) by (tag_0)))"
            ))
            .await;
            let plan_str = plan.display_indent_schema().to_string();
            assert!(plan_str.contains("Distinct:"), "{inner_agg}: {plan_str}");
        }
    }

    #[tokio::test]
    async fn test_plan_pql_filters_null_only_groups_for_non_count_inner_aggs() {
        let count_plan = parse_promql_to_plan("scalar(count(count(some_metric) by (tag_0)))").await;
        let count_plan_str = count_plan.display_indent_schema().to_string();
        assert!(
            !count_plan_str.contains("field_0 IS NOT NULL"),
            "{count_plan_str}"
        );

        for inner_agg in ["sum", "avg", "min", "max", "stddev", "stdvar"] {
            let plan = parse_promql_to_plan(&format!(
                "scalar(count({inner_agg}(some_metric) by (tag_0)))"
            ))
            .await;
            let plan_str = plan.display_indent_schema().to_string();
            assert!(
                plan_str.contains("field_0 IS NOT NULL"),
                "{inner_agg}: {plan_str}"
            );
        }
    }

    #[tokio::test]
    async fn test_plan_pql_skips_extension_rules_for_non_direct_or_unsupported_inner_agg() {
        for query in [
            "sum(irate(some_metric[1h])) / scalar(count(sum(irate(some_metric[1h])) by (tag_0)))",
            "sum(irate(some_metric[1h])) / scalar(count(group(some_metric) by (tag_0)))",
        ] {
            let plan = parse_promql_to_plan(query).await;
            let plan_str = plan.display_indent_schema().to_string();
            assert!(!plan_str.contains("Distinct:"), "{query}: {plan_str}");
        }
    }

    #[tokio::test]
    async fn test_plan_sql_does_not_apply_nested_count_rule() {
        let plan = parse_sql_to_plan(
            "SELECT id, count(inner_count) \
             FROM ( \
                 SELECT id, count(name) AS inner_count \
                 FROM test \
                 GROUP BY id \
                 ORDER BY id \
                 LIMIT 1000000 \
             ) t \
             GROUP BY id \
             ORDER BY id",
        )
        .await;

        let plan_str = plan.display_indent_schema().to_string();
        assert!(!plan_str.contains("Distinct:"), "{plan_str}");
    }
}
