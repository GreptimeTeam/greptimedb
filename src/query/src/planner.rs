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
use std::sync::Arc;

use async_trait::async_trait;
use catalog::table_source::DfTableSourceProvider;
use common_error::ext::BoxedError;
use common_telemetry::tracing;
use datafusion::common::DFSchema;
use datafusion::execution::context::SessionState;
use datafusion::sql::planner::PlannerContext;
use datafusion_expr::{Expr as DfExpr, LogicalPlan};
use datafusion_sql::planner::{ParserOptions, SqlToRel};
use promql_parser::parser::EvalStmt;
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::ast::Expr as SqlExpr;
use sql::statements::statement::Statement;

use crate::error::{DataFusionSnafu, PlanSqlSnafu, QueryPlanSnafu, Result, SqlSnafu};
use crate::parser::QueryStatement;
use crate::promql::planner::PromPlanner;
use crate::query_engine::{DefaultPlanDecoder, QueryEngineState};
use crate::range_select::plan_rewrite::RangePlanRewriter;
use crate::{DfContextProviderAdapter, QueryEngineContext};

#[async_trait]
pub trait LogicalPlanner: Send + Sync {
    async fn plan(&self, stmt: QueryStatement, query_ctx: QueryContextRef) -> Result<LogicalPlan>;

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
    async fn plan_sql(&self, stmt: Statement, query_ctx: QueryContextRef) -> Result<LogicalPlan> {
        let df_stmt = (&stmt).try_into().context(SqlSnafu)?;

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
        let parser_options = ParserOptions {
            enable_ident_normalization: config_options.sql_parser.enable_ident_normalization,
            parse_float_as_decimal: config_options.sql_parser.parse_float_as_decimal,
        };

        let sql_to_rel = SqlToRel::new_with_options(&context_provider, parser_options);

        let result = sql_to_rel
            .statement_to_plan(df_stmt)
            .context(PlanSqlSnafu)?;
        let plan = RangePlanRewriter::new(table_provider, query_ctx.clone())
            .rewrite(result)
            .await?;

        // Optimize logical plan by extension rules
        let context = QueryEngineContext::new(self.session_state.clone(), query_ctx);
        let plan = self
            .engine_state
            .optimize_by_extension_rules(plan, &context)
            .context(DataFusionSnafu)?;

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
        let parser_options = ParserOptions {
            enable_ident_normalization: normalize_ident,
            parse_float_as_decimal: config_options.sql_parser.parse_float_as_decimal,
        };

        let sql_to_rel = SqlToRel::new_with_options(&context_provider, parser_options);

        sql_to_rel
            .sql_to_expr(sql.into(), schema, &mut PlannerContext::new())
            .context(DataFusionSnafu)
    }

    #[tracing::instrument(skip_all)]
    async fn plan_pql(&self, stmt: EvalStmt, query_ctx: QueryContextRef) -> Result<LogicalPlan> {
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
        PromPlanner::stmt_to_plan(table_provider, stmt, &self.session_state)
            .await
            .map_err(BoxedError::new)
            .context(QueryPlanSnafu)
    }

    #[tracing::instrument(skip_all)]
    fn optimize_logical_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        self.engine_state
            .optimize_logical_plan(plan)
            .context(DataFusionSnafu)
            .map(Into::into)
    }
}

#[async_trait]
impl LogicalPlanner for DfLogicalPlanner {
    #[tracing::instrument(skip_all)]
    async fn plan(&self, stmt: QueryStatement, query_ctx: QueryContextRef) -> Result<LogicalPlan> {
        match stmt {
            QueryStatement::Sql(stmt) => self.plan_sql(stmt, query_ctx).await,
            QueryStatement::Promql(stmt) => self.plan_pql(stmt, query_ctx).await,
        }
    }

    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        self.optimize_logical_plan(plan)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
