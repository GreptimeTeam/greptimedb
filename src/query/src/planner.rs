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
use std::sync::Arc;

use async_trait::async_trait;
use catalog::table_source::DfTableSourceProvider;
use common_error::ext::BoxedError;
use common_telemetry::tracing;
use datafusion::common::DFSchema;
use datafusion::execution::context::SessionState;
use datafusion::sql::planner::PlannerContext;
use datafusion_expr::{col, Expr as DfExpr, LogicalPlan, LogicalPlanBuilder};
use datafusion_sql::planner::{ParserOptions, SqlToRel};
use log_query::LogQuery;
use promql_parser::parser::EvalStmt;
use session::context::QueryContextRef;
use snafu::{ensure, ResultExt};
use sql::ast::Expr as SqlExpr;
use sql::statements::query::Query;
use sql::statements::statement::Statement;
use sql::statements::tql::Tql;
use sql::CteContent;

use crate::error::{
    CteColumnSchemaMismatchSnafu, PlanSqlSnafu, QueryPlanSnafu, Result, SqlSnafu,
    UnimplementedSnafu,
};
use crate::log_query::planner::LogQueryPlanner;
use crate::parser::{PromQuery, QueryLanguageParser, QueryStatement, DEFAULT_LOOKBACK_STRING};
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

    #[tracing::instrument(skip_all)]
    async fn plan_sql(&self, stmt: &Statement, query_ctx: QueryContextRef) -> Result<LogicalPlan> {
        let mut planner_context = PlannerContext::new();
        let mut stmt = Cow::Borrowed(stmt);
        let mut is_tql_cte = false;

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
            box datafusion::sql::sqlparser::ast::Statement::Explain { format, .. },
        ) = &mut df_stmt
        {
            format.take();
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
        let parser_options: ParserOptions = parser_options.into();

        let sql_to_rel = SqlToRel::new_with_options(&context_provider, parser_options);

        // this IF is to handle different version of ASTs
        let result = if is_tql_cte {
            let Statement::Query(query) = stmt.into_owned() else {
                unreachable!("is_tql_cte should only be true for Query statements");
            };
            let sqlparser_stmt =
                datafusion::sql::sqlparser::ast::Statement::Query(Box::new(query.inner.into()));
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
            enable_ident_normalization: normalize_ident,
            ..parser_options.into()
        };

        let sql_to_rel = SqlToRel::new_with_options(&context_provider, parser_options);

        Ok(sql_to_rel.sql_to_expr(sql.into(), schema, &mut PlannerContext::new())?)
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
            query_ctx,
            plan_decoder,
            self.session_state
                .config_options()
                .sql_parser
                .enable_ident_normalization,
        );
        PromPlanner::stmt_to_plan(table_provider, stmt, &self.engine_state)
            .await
            .map_err(BoxedError::new)
            .context(QueryPlanSnafu)
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
                                    .map(|field| field.name().to_string())
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
}

#[async_trait]
impl LogicalPlanner for DfLogicalPlanner {
    #[tracing::instrument(skip_all)]
    async fn plan(&self, stmt: &QueryStatement, query_ctx: QueryContextRef) -> Result<LogicalPlan> {
        match stmt {
            QueryStatement::Sql(stmt) => self.plan_sql(stmt, query_ctx).await,
            QueryStatement::Promql(stmt) => self.plan_pql(stmt, query_ctx).await,
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
