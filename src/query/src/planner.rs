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
use std::ops::ControlFlow;
use std::str::FromStr;
use std::sync::Arc;

use arrow_schema::DataType;
use async_trait::async_trait;
use catalog::table_source::DfTableSourceProvider;
use common_error::ext::BoxedError;
use common_query::promql_annotations::promql_annotation_collector;
use common_telemetry::tracing;
use datafusion::common::{DFSchema, plan_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::SessionState;
use datafusion::sql::planner::PlannerContext;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{ScalarValue, ToDFSchema};
use datafusion_expr::expr::{Exists, InSubquery};
use datafusion_expr::{
    Analyze, Explain, ExplainFormat, Expr as DfExpr, LogicalPlan, LogicalPlanBuilder, PlanType,
    ToStringifiedPlan, col,
};
use datafusion_sql::parser::Statement as DfStatement;
use datafusion_sql::planner::{IdentNormalizer, ParserOptions, SqlToRel};
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
use sqlparser::ast::{AccessExpr, Value, visit_expressions_mut};
use store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME;

use crate::datafusion::QUERY_FALLBACK_HINT;
use crate::dist_plan::DistPlannerOptions;
use crate::error::{
    CteColumnSchemaMismatchSnafu, PlanSqlSnafu, QueryPlanSnafu, Result, SqlSnafu,
    UnimplementedSnafu,
};
use crate::log_query::planner::LogQueryPlanner;
use crate::optimizer::expr_depth;
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

/// A remote native plan may place fused evaluation only above independent raw sources.
/// In particular, a bare scan under FusedSeries would require shipping the process-local
/// fused plan to a region, which this gate must not admit.
///
/// A fused node that reads no region is the one exception: it has nothing to place in a region, so
/// the frontend evaluates it and the transport this gate protects is never used.
fn remote_native_raw_plan_supported(plan: &LogicalPlan) -> bool {
    fn walk(plan: &LogicalPlan, found_raw: &mut bool) -> bool {
        if matches!(plan, LogicalPlan::TableScan(_)) {
            return false;
        }
        if let LogicalPlan::Extension(extension) = plan
            && extension
                .node
                .as_any()
                .is::<promql::extension_plan::RawSeriesSource>()
        {
            *found_raw = true;
            return matches!(
                extension.node.inputs().as_slice(),
                [LogicalPlan::TableScan(_)]
            );
        }
        plan.inputs().iter().all(|child| walk(child, found_raw))
    }
    let mut found_raw = false;
    walk(plan, &mut found_raw) && (found_raw || is_regionless_fused_plan(plan))
}

/// Whether `plan` is a native fused plan with no region below it.
///
/// Two shapes qualify, and both are already the finished plan: a statement that reads no data -
/// `time()`, `vector(1)`, a label rewrite over one of them - is one node with no input at all,
/// and a discovered selector that matched no metric table is planned over the typed empty frame
/// [`is_empty_discovered_input`](crate::promql::planner::native::is_empty_discovered_input)
/// builds. Neither has a scan to ship, so neither needs the remote transport.
///
/// The route stacks its own nodes above such an input: a split aggregate runs a `Final` half over
/// the `Partial` half that reads it, and a segmented statement runs its parent over the segment's
/// own node. So the inputs a node reads are followed through fused nodes only - never through
/// another extension, and never through a scan - and every leaf that is not one of them rejects
/// the shape.
fn is_regionless_fused_plan(plan: &LogicalPlan) -> bool {
    // `strip_tsid_column` publishes a statement's own columns over a node that reported the
    // internal series id, which a data-free statement can synthesize with `label_replace`. That
    // wrapper is this route's own, so it is read through when it drops exactly that one column.
    if let LogicalPlan::Projection(projection) = plan {
        return strips_only_tsid(plan, projection.input.as_ref())
            && is_regionless_fused_chain(projection.input.as_ref());
    }
    is_regionless_fused_chain(plan)
}

/// Whether `plan` is a fused node whose inputs are fused nodes or the typed empty frame.
fn is_regionless_fused_chain(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Extension(extension) = plan else {
        return false;
    };
    let Some(fused) = extension
        .node
        .as_any()
        .downcast_ref::<promql::extension_plan::FusedSeries>()
    else {
        return false;
    };
    fused.children().iter().all(|child| {
        crate::promql::planner::native::is_empty_discovered_input(child)
            || is_regionless_fused_chain(child)
    })
}

/// Whether `plan` is the projection that drops exactly the internal series-id column of `inner`.
///
/// That is the projection `strip_tsid_column` builds: one plain column per field of its input,
/// in order, except `__tsid__`. A computed expression, a reordering or a second dropped column is
/// some other plan and is not read through.
fn strips_only_tsid(plan: &LogicalPlan, inner: &LogicalPlan) -> bool {
    let LogicalPlan::Projection(projection) = plan else {
        return false;
    };
    if projection
        .expr
        .iter()
        .any(|expr| !matches!(expr, DfExpr::Column(_)))
    {
        return false;
    }
    let stripped = inner
        .schema()
        .fields()
        .iter()
        .filter(|field| field.name() != DATA_SCHEMA_TSID_COLUMN_NAME)
        .map(|field| field.name())
        .collect::<Vec<_>>();
    let projected = plan
        .schema()
        .fields()
        .iter()
        .map(|field| field.name())
        .collect::<Vec<_>>();
    projected == stripped && projected.len() + 1 == inner.schema().fields().len()
}

/// Whether `plan` runs on the native fused-series route.
fn contains_native_fused(plan: &LogicalPlan) -> bool {
    if let LogicalPlan::Extension(extension) = plan
        && extension
            .node
            .as_any()
            .is::<promql::extension_plan::FusedSeries>()
    {
        return true;
    }
    plan.inputs()
        .iter()
        .any(|input| contains_native_fused(input))
}

/// Whether `schema` is a series frame's declaration, which describes the columns below it.
fn publishes_series_frame(schema: &DFSchema) -> bool {
    schema
        .metadata()
        .keys()
        .any(|key| key.starts_with("greptime.series."))
}

/// Whether `published` publishes `wanted`'s columns: the same names, in the same order, with the
/// same types, nullability, per-column metadata and schema metadata.
///
/// Qualifiers are not compared: a CTE's plan is read through the alias the CTE registers, which
/// names every column after the CTE on both routes.
fn publishes_columns(published: &DFSchema, wanted: &DFSchema) -> bool {
    published.fields().len() == wanted.fields().len()
        && published.metadata() == wanted.metadata()
        && published
            .iter()
            .zip(wanted.iter())
            .all(|((_, published), (_, wanted))| {
                published.name() == wanted.name()
                    && published.data_type() == wanted.data_type()
                    && published.is_nullable() == wanted.is_nullable()
                    && published.metadata() == wanted.metadata()
            })
}

/// The type a column names once its dictionary encoding is taken off.
///
/// A label column is dictionary-encoded in the existing planner's plan and plain text in the fused
/// plan's. That is the one difference between the two answers for the same column that an
/// expression can carry: a cast reproduces the encoding, and the values stay the same strings.
fn unwrap_dictionary(data_type: &DataType) -> &DataType {
    let mut data_type = data_type;
    while let DataType::Dictionary(_, inner) = data_type {
        data_type = inner.as_ref();
    }
    data_type
}

/// The native plan, wrapped in the projection that publishes `legacy`'s columns over it.
///
/// A hybrid CTE registers the plan as a table the outer SQL query reads, so the columns this plan
/// publishes *are* the CTE's columns: their names, their order, their types, their nullability and
/// the metadata they carry are what the statement's SQL sees, and a column list the CTE declares is
/// applied to them by position. The two routes do not agree on that schema - the fused node takes
/// its names from the compiled tree and publishes the time index first, while the existing planner
/// names each value column after the expression it planned and publishes the labels first - so a CTE
/// answered natively would make one statement mean different things depending on which route planned
/// it. This wraps the native plan in the projection that publishes the existing planner's answer for
/// the same statement instead, so what a CTE exposes does not depend on the route.
///
/// The projection is made of expressions - an alias per column, and a cast where the two routes carry
/// the same values in different Arrow types - because that is what survives analysis: the analyzer
/// rebuilds a projection's schema from its expressions, so anything the projection declared but its
/// expressions did not carry would be gone by the time the CTE's columns are read.
///
/// The mapping is the identity on every column both plans publish alike - the time index and the
/// label columns, which carry the same metadata on both routes now - and is a cast for a column
/// whose identity is unambiguous: one native column of the same type once both are unencoded, no
/// per-column metadata to lose and the same nullability. `None` says the two plans admit no such
/// mapping - a differently named time index, an ambiguous set of columns left over, or a column a
/// cast cannot reproduce faithfully - and the caller then keeps the existing plan rather than publish
/// a column it cannot fill faithfully.
fn publish_legacy_schema(native: LogicalPlan, legacy: &LogicalPlan) -> Option<LogicalPlan> {
    let native_schema = native.schema().clone();
    let legacy_schema = legacy.schema();

    // A frame's declaration is schema metadata describing the columns under it, and a projection
    // that renames and reorders them cannot keep it. A statement that publishes one keeps the
    // existing plan.
    if publishes_series_frame(native_schema.as_ref()) {
        return None;
    }
    if publishes_columns(native_schema.as_ref(), legacy_schema.as_ref()) {
        return Some(native);
    }

    let native_fields = native_schema
        .iter()
        .map(|(qualifier, field)| (qualifier.cloned(), field))
        .collect::<Vec<_>>();
    let mut taken = vec![false; native_fields.len()];
    let mut source: Vec<Option<usize>> = vec![None; legacy_schema.fields().len()];
    // First the columns both plans publish alike: the same name and the same column, metadata
    // included. These are read as they are, so nothing about them is re-declared here.
    for (legacy_index, (_, legacy_field)) in legacy_schema.iter().enumerate() {
        let found =
            native_fields
                .iter()
                .enumerate()
                .find_map(|(native_index, (_, native_field))| {
                    (!taken[native_index]
                        && native_field.name() == legacy_field.name()
                        && native_field.data_type() == legacy_field.data_type()
                        && native_field.is_nullable() == legacy_field.is_nullable()
                        && native_field.metadata() == legacy_field.metadata())
                    .then_some(native_index)
                });
        if let Some(native_index) = found {
            taken[native_index] = true;
            source[legacy_index] = Some(native_index);
        }
    }
    // Then the columns whose only difference is that the routes carry them in different Arrow
    // types. A cast reproduces the existing planner's type, so it is only paired when exactly one
    // native column can be that column: the same type once both are unencoded, the same
    // nullability, and no metadata a cast would drop.
    for (legacy_index, source) in source.iter_mut().enumerate() {
        if source.is_some() {
            continue;
        }
        let legacy_field = legacy_schema.field(legacy_index);
        if !legacy_field.metadata().is_empty() {
            continue;
        }
        let candidates = native_fields
            .iter()
            .enumerate()
            .filter(|(native_index, (_, native_field))| {
                !taken[*native_index]
                    && unwrap_dictionary(native_field.data_type())
                        == unwrap_dictionary(legacy_field.data_type())
                    && native_field.is_nullable() == legacy_field.is_nullable()
            })
            .map(|(native_index, _)| native_index)
            .collect::<Vec<_>>();
        if let [native_index] = candidates.as_slice() {
            taken[*native_index] = true;
            *source = Some(*native_index);
        }
    }
    let source = source.into_iter().collect::<Option<Vec<usize>>>()?;

    let mut exprs = Vec::with_capacity(source.len());
    for ((_, legacy_field), native_index) in legacy_schema.iter().zip(source) {
        let (native_qualifier, native_field) = &native_fields[native_index];
        let column = DfExpr::Column(datafusion_common::Column::new(
            native_qualifier.clone(),
            native_field.name().clone(),
        ));
        // A column the two plans publish alike is read as it is; one they carry in different Arrow
        // types is cast to the existing planner's type, which keeps the same values under the same
        // name - and keeps them when the analyzer rebuilds this projection.
        let value = if native_field.data_type() == legacy_field.data_type() {
            column
        } else {
            DfExpr::Cast(datafusion_expr::Cast::new(
                Box::new(column),
                legacy_field.data_type().clone(),
            ))
        };
        exprs.push(value.alias(legacy_field.name().clone()));
    }
    let plan = LogicalPlanBuilder::from(native)
        .project(exprs)
        .ok()?
        .build()
        .ok()?;
    // The published columns are the ones this set out to publish, or the native plan is not used at
    // all: what a CTE exposes may not change for a reason this did not predict.
    publishes_columns(plan.schema().as_ref(), legacy_schema.as_ref()).then_some(plan)
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

    /// Derive a [`SessionState`] whose [`ExecutionProps`] includes
    /// `query_execution_start_time` if a scheduled time extension is present
    /// in the query context.
    fn derive_session_state_with_scheduled_time(
        &self,
        query_ctx: &QueryContextRef,
    ) -> Result<SessionState> {
        let extensions = query_ctx.extensions();
        match crate::options::parse_scheduled_time_datetime(&extensions)? {
            Some(dt) => {
                let execution_props = self
                    .session_state
                    .execution_props()
                    .clone()
                    .with_query_execution_start_time(dt);
                Ok(
                    SessionStateBuilder::new_from_existing(self.session_state.clone())
                        .with_execution_props(execution_props)
                        .build(),
                )
            }
            None => Ok(self.session_state.clone()),
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
                format: ExplainFormat::Indent,
                input: plan,
                schema,
                analyze_level: None,
                analyze_categories: None,
            }))
        } else {
            let stringified_plans = vec![plan.to_stringified(PlanType::InitialLogicalPlan)];

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
                show_statistics: None,
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
        normalize_field_access_after_subscript(
            &mut df_stmt,
            self.session_state
                .config_options()
                .sql_parser
                .enable_ident_normalization,
        );

        // TODO(LFC): Remove this when Datafusion supports **both** the syntax and implementation of "explain with format".
        if let datafusion::sql::parser::Statement::Statement(stmt) = &mut df_stmt
            && matches!(
                stmt.as_ref(),
                datafusion::sql::sqlparser::ast::Statement::Explain { .. }
            )
        {
            UnimplementedSnafu {
                operation: "EXPLAIN with FORMAT using raw datafusion planner",
            }
            .fail()?;
        }

        let scheduled_state = self.derive_session_state_with_scheduled_time(&query_ctx)?;
        let table_provider = DfTableSourceProvider::new(
            self.engine_state.catalog_manager().clone(),
            self.engine_state.disallow_cross_catalog_query(),
            query_ctx.clone(),
            Arc::new(DefaultPlanDecoder::new(
                scheduled_state.clone(),
                &query_ctx,
            )?),
            scheduled_state
                .config_options()
                .sql_parser
                .enable_ident_normalization,
        );

        let context_provider = DfContextProviderAdapter::try_new(
            self.engine_state.clone(),
            scheduled_state.clone(),
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

        expr_depth::validate_plan(&result)?;
        common_telemetry::debug!("Logical planner, statement to plan result: {result}");
        let plan = RangePlanRewriter::new(table_provider, query_ctx.clone())
            .rewrite(result)
            .await?;

        // Optimize logical plan by extension rules
        let context = QueryEngineContext::new(scheduled_state, query_ctx);
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
        let scheduled_state = self.derive_session_state_with_scheduled_time(&query_ctx)?;
        let context_provider = DfContextProviderAdapter::try_new(
            self.engine_state.clone(),
            scheduled_state,
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
    async fn plan_pql(
        &self,
        stmt: &EvalStmt,
        query_ctx: QueryContextRef,
        native_route_allowed: bool,
    ) -> Result<LogicalPlan> {
        let mut scheduled_state = self.derive_session_state_with_scheduled_time(&query_ctx)?;
        // Whether the distributed planner may fall back to the fallback plan rewriter. The
        // execution engine resolves the same three sources the same way: the engine option, which
        // it copies into the session state it plans with, and then, per query, the session
        // variable or the `query_fallback` hint. Each source only ever turns the fallback on, so
        // the value they end up with is the disjunction of them.
        let allow_query_fallback = scheduled_state
            .config_options()
            .extensions
            .get::<DistPlannerOptions>()
            .is_some_and(|options| options.allow_query_fallback)
            || query_ctx.configuration_parameter().allow_query_fallback()
            || query_ctx
                .extension(QUERY_FALLBACK_HINT)
                .is_some_and(|fallback| fallback.to_lowercase().parse::<bool>().unwrap_or(false));
        let promql_annotations = query_ctx.remote_query_id().map(promql_annotation_collector);
        if let Some(collector) = &promql_annotations {
            scheduled_state
                .config_mut()
                .options_mut()
                .extensions
                .insert(collector.clone());
        }
        let plan_decoder = Arc::new(DefaultPlanDecoder::new(
            scheduled_state.clone(),
            &query_ctx,
        )?);
        let table_provider = || {
            DfTableSourceProvider::new(
                self.engine_state.catalog_manager().clone(),
                self.engine_state.disallow_cross_catalog_query(),
                query_ctx.clone(),
                plan_decoder.clone(),
                scheduled_state
                    .config_options()
                    .sql_parser
                    .enable_ident_normalization,
            )
        };
        let native_requested = native_route_allowed
            && !allow_query_fallback
            && self.engine_state.experimental_native_promql()
            && crate::promql::planner::native::native_promql_requested(&query_ctx);
        let plan = PromPlanner::stmt_to_plan_with_annotations(
            table_provider(),
            stmt,
            &self.engine_state,
            promql_annotations.clone(),
            // A fallback rewriter can wrap a native region scan in an unusable MergeScan.
            native_requested,
        )
        .await
        .map_err(BoxedError::new)
        .context(QueryPlanSnafu)?;

        // Only per-selector raw sources have a supported remote transport, and a statement that
        // reads no table needs none. Decide while the original statement is still available:
        // physical planning cannot fall back safely.
        let plan = if native_requested
            && !self.engine_state.in_process_region_transport()
            && !remote_native_raw_plan_supported(&plan)
        {
            PromPlanner::stmt_to_plan_with_annotations(
                table_provider(),
                stmt,
                &self.engine_state,
                promql_annotations,
                // The fallback plan is the legacy one: it is not offered to the native route.
                false,
            )
            .await
            .map_err(BoxedError::new)
            .context(QueryPlanSnafu)?
        } else {
            plan
        };
        let context = QueryEngineContext::new(scheduled_state, query_ctx);
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
                // A hybrid CTE registers this plan as a table the outer SQL query reads, so the
                // `FusedSeries` node sits under a SQL alias and projection instead of at the plan
                // root. Neither decides where a fused subtree runs: placement asks whether the
                // node's own contract allows a region to run it, and the column requirements a
                // node above makes are enforced by their lineage rather than by the shape of the
                // subtree they travel through, so an alias or projection over the CTE does not
                // block a multi-input fused subtree. The route is therefore offered here like it
                // is for a top-level statement.
                let QueryStatement::Promql(eval, _alias) = &stmt else {
                    unreachable!("a TQL eval parses to a PromQL statement");
                };
                let plan = self.plan_pql(eval, query_ctx.clone(), true).await?;
                if !contains_native_fused(&plan) {
                    // The route was not taken - it is off for this query, or it declined this
                    // shape - so this is the existing plan, which already publishes what a CTE has
                    // always published.
                    return Ok(plan);
                }
                // What the CTE publishes is SQL-visible, and this route names and orders its output
                // its own way, so its own schema cannot stand in for the existing planner's: plan
                // the statement for that schema and publish it over the native answer. A statement
                // whose columns that projection cannot fill faithfully keeps the existing plan
                // instead of changing the schema the CTE exposes.
                let existing = self.plan_pql(eval, query_ctx, false).await?;
                Ok(publish_legacy_schema(plan, &existing).unwrap_or(existing))
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

        Self::extract_from_plan(plan, &mut placeholder_types, &mut casted_placeholders)?;

        Ok(placeholder_types)
    }

    fn extract_from_plan(
        plan: &LogicalPlan,
        placeholder_types: &mut HashMap<String, Option<DataType>>,
        casted_placeholders: &mut HashSet<String>,
    ) -> Result<()> {
        plan.apply(|node| {
            for expr in node.expressions() {
                let _ = expr.apply(|e| {
                    // Handle casted placeholders
                    if let DfExpr::Cast(cast) = e
                        && let DfExpr::Placeholder(ph) = &*cast.expr
                    {
                        placeholder_types
                            .insert(ph.id.clone(), Some(cast.field.data_type().clone()));
                        casted_placeholders.insert(ph.id.clone());
                    }

                    // Handle arrow_cast(Placeholder, 'type_string') generated by SQL rewriter
                    if let DfExpr::ScalarFunction(scalar_func) = e
                        && scalar_func.name() == "arrow_cast"
                        && scalar_func.args.len() == 2
                        && let DfExpr::Placeholder(ph) = &scalar_func.args[0]
                        && let DfExpr::Literal(ScalarValue::Utf8(Some(type_str)), _) =
                            &scalar_func.args[1]
                        && let Ok(data_type) = type_str.parse::<DataType>()
                    {
                        placeholder_types.insert(ph.id.clone(), Some(data_type));
                        casted_placeholders.insert(ph.id.clone());
                    }

                    // Handle bare (non-casted) placeholders
                    if let DfExpr::Placeholder(ph) = e
                        && !casted_placeholders.contains(&ph.id)
                        && !placeholder_types.contains_key(&ph.id)
                    {
                        placeholder_types.insert(ph.id.clone(), None);
                    }

                    // Recurse into subquery plans embedded in expressions
                    match e {
                        DfExpr::Exists(Exists { subquery, .. })
                        | DfExpr::InSubquery(InSubquery { subquery, .. })
                        | DfExpr::ScalarSubquery(subquery) => {
                            Self::extract_from_plan(
                                &subquery.subquery,
                                placeholder_types,
                                casted_placeholders,
                            )?;
                        }
                        _ => {}
                    }

                    Ok(TreeNodeRecursion::Continue)
                });
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }

    fn infer_limit_placeholder_types(
        plan: &LogicalPlan,
        placeholder_types: &mut HashMap<String, Option<DataType>>,
    ) -> Result<()> {
        plan.apply(|node| {
            if let LogicalPlan::Limit(limit) = node {
                for expr in limit.skip.iter().chain(limit.fetch.iter()) {
                    expr.apply(|e| {
                        if let DfExpr::Placeholder(ph) = e {
                            placeholder_types
                                .entry(ph.id.clone())
                                .and_modify(|existing| {
                                    if existing.is_none() {
                                        *existing = Some(DataType::Int64);
                                    }
                                })
                                .or_insert(Some(DataType::Int64));
                        }

                        Ok(TreeNodeRecursion::Continue)
                    })?;
                }
            }

            Ok(TreeNodeRecursion::Continue)
        })?;

        Ok(())
    }

    /// Gets inferred parameter types from a logical plan.
    /// Returns a map where each parameter ID is mapped to:
    /// - Some(DataType) if the parameter type could be inferred
    /// - None if the parameter type could not be inferred
    ///
    /// This function first uses DataFusion's `get_parameter_types()` to infer types.
    /// If any parameters have `None` values (i.e., DataFusion couldn't infer their types),
    /// it falls back to using `extract_placeholder_cast_types()` to detect explicit casts
    /// and applies context-specific inference such as LIMIT/OFFSET placeholders.
    ///
    /// This is because datafusion can only infer types for a limited cases.
    ///
    /// Example: For query `WHERE $1::TEXT AND $2`, DataFusion may not infer `$2`'s type,
    /// but this function will return `{"$1": Some(DataType::Utf8), "$2": None}`.
    pub fn get_inferred_parameter_types(
        plan: &LogicalPlan,
    ) -> Result<HashMap<String, Option<DataType>>> {
        let mut param_types = plan.get_parameter_types().context(PlanSqlSnafu)?;

        let has_none = param_types.values().any(|v| v.is_none());

        if has_none {
            let cast_types = Self::extract_placeholder_cast_types(plan)?;

            for (id, opt_type) in cast_types {
                param_types
                    .entry(id)
                    .and_modify(|existing| {
                        if existing.is_none() {
                            *existing = opt_type.clone();
                        }
                    })
                    .or_insert(opt_type);
            }

            Self::infer_limit_placeholder_types(plan, &mut param_types)?;
        }

        Ok(param_types)
    }
}

/// Normalizes dot field accesses that follow a subscript for DataFusion.
///
/// sqlparser represents `j.o.l[1].inner.l[2]` as a compound field access with
/// the following access chain:
///
/// ```text
/// Dot(Identifier("o")),
/// Dot(Identifier("l")),
/// Subscript(1),
/// Dot(Identifier("inner")),
/// Dot(Identifier("l")),
/// Subscript(2)
/// ```
///
/// DataFusion first resolves the leading `j.o.l` through
/// `JsonExprPlanner::plan_compound_identifier`, which produces an untyped
/// `json_get` with path `$.o.l`. Before invoking `JsonExprPlanner::plan_field_access`,
/// however, DataFusion eagerly converts every remaining access into a
/// `GetFieldAccess`. It accepts string values but not [`SqlExpr::Identifier`]s
/// in [`AccessExpr::Dot`] after a subscript. Without this normalization, that
/// conversion fails at `.inner`, and `plan_field_access` is never called, even
/// for the preceding `[1]`.
///
/// This function converts dot identifiers after the first subscript into
/// `Dot(Value(SingleQuotedString(...)))`, applying DataFusion's identifier
/// normalization before discarding whether each identifier was quoted. It
/// changes neither the SQL text nor the dot accesses into subscript nodes: the
/// resulting AST is conceptually `j.o.l[1].'inner'.'l'[2]`. DataFusion converts
/// the string-valued dot accesses into named field accesses, which
/// `plan_field_access` safely encodes as dot members, quoting names when needed. It can then extend the
/// JSON path to `$.o.l[1].inner.l[2]`.
///
/// This behavior is unchanged in the latest upstream releases checked here:
/// DataFusion 55.0.0 and sqlparser 0.62.0.
///
/// TODO(LFC): Remove this workaround after upstream supports dot identifiers after subscripts.
fn normalize_field_access_after_subscript(stmt: &mut DfStatement, normalize_ident: bool) {
    let DfStatement::Statement(stmt) = stmt else {
        return;
    };
    let normalizer = IdentNormalizer::new(normalize_ident);

    let _ = visit_expressions_mut(stmt.as_mut(), |expr| {
        let SqlExpr::CompoundFieldAccess { access_chain, .. } = expr else {
            return ControlFlow::<()>::Continue(());
        };
        let Some(index) = access_chain
            .iter()
            .position(|x| matches!(x, AccessExpr::Subscript(_)))
        else {
            return ControlFlow::Continue(());
        };

        for access in &mut access_chain[index + 1..] {
            let AccessExpr::Dot(SqlExpr::Identifier(ident)) = access else {
                continue;
            };
            let value = normalizer.normalize(ident.clone());
            *access = AccessExpr::Dot(SqlExpr::Value(
                Value::SingleQuotedString(value).with_span(ident.span),
            ));
        }
        ControlFlow::Continue(())
    });
}

#[async_trait]
impl LogicalPlanner for DfLogicalPlanner {
    #[tracing::instrument(skip_all)]
    async fn plan(&self, stmt: &QueryStatement, query_ctx: QueryContextRef) -> Result<LogicalPlan> {
        match stmt {
            QueryStatement::Sql(stmt) => self.plan_sql(stmt, query_ctx).await,
            // The statement's own plan is what a region receives, so the native route is offered.
            QueryStatement::Promql(stmt, _alias) => self.plan_pql(stmt, query_ctx, true).await,
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
    use common_query::OutputData;
    use common_recordbatch::RecordBatch;
    use common_recordbatch::util::collect_batches;
    use common_time::Timezone;
    use datatypes::arrow::array::{Array, Float64Array, StringArray};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use session::context::{
        MetricDiscoveryScope, QueryContext, QueryContextBuilder, QueryContextRef,
    };
    use store_api::metric_engine_consts::{
        DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME, LOGICAL_TABLE_METADATA_KEY,
        METRIC_ENGINE_NAME,
    };
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::test_util::EmptyTable;

    use super::*;
    use crate::datafusion::QUERY_FALLBACK_HINT;
    use crate::parser::{PromQuery, QueryLanguageParser};
    use crate::promql::planner::NATIVE_PROMQL_EXTENSION_KEY;
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

    async fn create_timestamp_test_engine() -> QueryEngineRef {
        let columns = vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(
                "st",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            ColumnSchema::new("note", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(
                "ts_ns",
                ConcreteDataType::timestamp_nanosecond_datatype(),
                true,
            ),
        ];
        let schema = Arc::new(Schema::new(columns));
        let table_meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![])
            .value_indices(vec![0, 1, 2, 3])
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::new("timestamps", table_meta)
            .build()
            .unwrap();
        let table = EmptyTable::from_table_info(&table_info);

        crate::tests::new_query_engine_with_table(table)
    }

    fn create_promql_test_engine() -> QueryEngineRef {
        create_promql_test_engine_with(crate::options::QueryOptions::default(), false)
    }

    /// The PromQL fixture, additionally stating the engine options and how its regions are reached.
    fn create_promql_test_engine_with(
        query_options: crate::options::QueryOptions,
        in_process_region_transport: bool,
    ) -> QueryEngineRef {
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
        let mut other_info = logical_info.clone();
        other_info.ident.table_id = 1025;
        other_info.name = "other_metric".to_string();
        catalog_manager
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: other_info.name.clone(),
                table_id: other_info.ident.table_id,
                table: EmptyTable::from_table_info(&other_info),
            })
            .unwrap();

        QueryEngineFactory::try_new_with_plugins_and_region_transport(
            catalog_manager,
            None,
            None,
            None,
            None,
            None,
            false,
            in_process_region_transport,
            Default::default(),
            query_options,
        )
        .unwrap()
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

    /// Plans `rate(some_metric[5m])` on the fixture above with the fallback sources the caller
    /// states — the engine option, the session variable and the `query_fallback` hint — while the
    /// native route is enabled and requested, so only the fallback guard decides.
    async fn plan_native_promql_with_fallback(
        engine_fallback: bool,
        session_fallback: bool,
        hint: Option<&str>,
    ) -> String {
        let engine = create_promql_test_engine_with(
            crate::options::QueryOptions {
                allow_query_fallback: engine_fallback,
                experimental_native_promql: true,
                ..Default::default()
            },
            // The native route plans a region input the region server in this process accepts.
            true,
        );
        // The native route request and the fallback hint both reach the planner as query context
        // extensions, the way a client sends them.
        let mut builder = QueryContextBuilder::default()
            .set_extension(NATIVE_PROMQL_EXTENSION_KEY.to_string(), "true".to_string());
        if let Some(hint) = hint {
            builder = builder.set_extension(QUERY_FALLBACK_HINT.to_string(), hint.to_string());
        }
        let query_ctx = builder.build();
        query_ctx
            .configuration_parameter()
            .set_allow_query_fallback(session_fallback);
        let query_ctx = Arc::new(query_ctx);
        let stmt = QueryLanguageParser::parse_promql(
            &PromQuery {
                query: "rate(some_metric[5m])".to_string(),
                start: "0".to_string(),
                end: "10".to_string(),
                step: "5s".to_string(),
                lookback: "300s".to_string(),
                alias: None,
            },
            &query_ctx,
        )
        .unwrap();

        engine
            .planner()
            .plan(&stmt, query_ctx)
            .await
            .unwrap()
            .display_indent()
            .to_string()
    }

    /// The native route is admitted only when the query cannot fall back, and every source the
    /// execution engine resolves the fallback from decides that at plan time too.
    #[tokio::test]
    async fn test_plan_pql_declines_native_route_when_query_may_fall_back() {
        // (engine option, session variable, hint, native route admitted)
        for (engine, session, hint, native) in [
            // Nothing asks for the fallback, so the route is planned as before.
            (false, false, None, true),
            // A hint that does not parse as `true` leaves the fallback off, as it does at execution.
            (false, false, Some("false"), true),
            (false, false, Some("yes"), true),
            // Either per-query source turns the fallback on, so the route is declined.
            (false, true, None, false),
            (false, false, Some("true"), false),
            (false, false, Some("TRUE"), false),
            // Each source only turns the fallback on: neither a `false` hint nor a `false` session
            // variable clears the engine option, and a `false` hint does not clear the variable.
            (true, false, None, false),
            (true, false, Some("false"), false),
            (true, true, Some("false"), false),
        ] {
            let plan = plan_native_promql_with_fallback(engine, session, hint).await;
            assert_eq!(
                plan.contains("FusedSeries"),
                native,
                "engine={engine}, session={session}, hint={hint:?}: {plan}"
            );
        }
    }

    /// Remote native planning accepts independent raw sources only. Unsupported shapes are
    /// replanned through the legacy route, while the in-process route remains unchanged.
    #[tokio::test]
    async fn test_remote_native_promql_admission_replans_unsupported_shapes() {
        for (in_process, query, native) in [
            (false, "some_metric + other_metric", true),
            (false, "rate(some_metric[5m])", false),
            (true, "rate(some_metric[5m])", true),
        ] {
            let engine = create_promql_test_engine_with(
                crate::options::QueryOptions {
                    experimental_native_promql: true,
                    ..Default::default()
                },
                in_process,
            );
            let query_ctx = Arc::new(
                QueryContextBuilder::default()
                    .set_extension(NATIVE_PROMQL_EXTENSION_KEY.to_string(), "true".to_string())
                    .build(),
            );
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
            let plan = engine.planner().plan(&stmt, query_ctx).await.unwrap();
            let display = plan.display_indent().to_string();
            assert_eq!(
                display.contains("FusedSeries"),
                native,
                "in_process={in_process}, query={query}: {display}"
            );
            if native && !in_process {
                assert!(remote_native_raw_plan_supported(&plan), "{display}");
                assert!(display.contains("RawSeriesSource"), "{display}");
            }
        }
    }

    /// Plans `query` on the fixture engine with the native route requested. The engine reaches its
    /// regions over the wire, which is the route the admission gate guards.
    async fn plan_remote_native_promql(
        engine: &QueryEngineRef,
        query_ctx: &QueryContextRef,
        query: &str,
    ) -> LogicalPlan {
        let stmt = QueryLanguageParser::parse_promql(
            &PromQuery {
                query: query.to_string(),
                start: "0".to_string(),
                end: "10".to_string(),
                step: "5s".to_string(),
                lookback: "300s".to_string(),
                alias: None,
            },
            query_ctx,
        )
        .unwrap();
        engine
            .planner()
            .plan(&stmt, query_ctx.clone())
            .await
            .unwrap()
    }

    /// A query context that asks for the native route and states what a nameless selector may
    /// discover; the default context declines discovery altogether.
    fn native_query_ctx(scope: MetricDiscoveryScope) -> QueryContextRef {
        let mut ctx = (*QueryContext::arc()).clone();
        ctx.set_metric_discovery_scope(scope);
        ctx.set_extension(NATIVE_PROMQL_EXTENSION_KEY.to_string(), "true".to_string());
        Arc::new(ctx)
    }

    fn native_discovery_query_ctx() -> QueryContextRef {
        native_query_ctx(MetricDiscoveryScope::Unrestricted)
    }

    /// The engine's physical plan for `plan` and the batches it answers.
    async fn execute_plan(
        engine: &QueryEngineRef,
        plan: LogicalPlan,
        query_ctx: &QueryContextRef,
    ) -> (
        Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        Vec<RecordBatch>,
    ) {
        let output = engine.execute(plan, query_ctx.clone()).await.unwrap();
        let physical = output
            .meta
            .plan
            .expect("the engine reports its physical plan");
        let batches = match output.data {
            OutputData::Stream(stream) => collect_batches(stream).await.unwrap(),
            OutputData::RecordBatches(batches) => batches,
            other => panic!("a PromQL query answers rows, not {other:?}"),
        }
        .take();
        (physical, batches)
    }

    /// Every node name in the tree rooted at `root`, the root included.
    fn physical_plan_names(
        root: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) -> Vec<String> {
        let mut names = Vec::new();
        let mut stack = vec![Arc::clone(root)];
        while let Some(node) = stack.pop() {
            names.push(node.name().to_string());
            stack.extend(node.children().into_iter().cloned());
        }
        names
    }

    /// Every logical plan node below `root`, the root included.
    fn logical_plan_nodes(root: &LogicalPlan) -> Vec<&LogicalPlan> {
        let mut nodes = Vec::new();
        let mut stack = vec![root];
        while let Some(node) = stack.pop() {
            nodes.push(node);
            stack.extend(node.inputs());
        }
        nodes
    }

    fn fused_series_nodes(plan: &LogicalPlan) -> Vec<&promql::extension_plan::FusedSeries> {
        logical_plan_nodes(plan)
            .into_iter()
            .filter_map(|node| match node {
                LogicalPlan::Extension(extension) => extension
                    .node
                    .as_any()
                    .downcast_ref::<promql::extension_plan::FusedSeries>(
                ),
                _ => None,
            })
            .collect()
    }

    fn numeric_values(batches: &[RecordBatch], column: &str) -> Vec<f64> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name(column)
                    .unwrap_or_else(|| panic!("no `{column}` column"))
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn label_values(batches: &[RecordBatch], column: &str) -> Vec<String> {
        batches
            .iter()
            .flat_map(|batch| {
                let array = batch
                    .column_by_name(column)
                    .unwrap_or_else(|| panic!("no `{column}` column"))
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("a label column is Utf8");
                (0..array.len())
                    .map(|row| array.value(row).to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Asserts `plan` is a native plan that reads no region, and that every fused node in it binds
    /// no metric: a data-free or zero-match statement has no table to name.
    fn assert_regionless_native_plan(plan: &LogicalPlan, query: &str) {
        let display = plan.display_indent().to_string();
        assert!(display.contains("FusedSeries"), "{query}: {display}");
        for absent in ["TableScan", "MergeScan", "RawSeriesSource"] {
            assert!(
                !display.contains(absent),
                "{query} contains {absent}: {display}"
            );
        }
        let fused = fused_series_nodes(plan);
        assert!(!fused.is_empty(), "{query}: {display}");
        for node in fused {
            assert_eq!(
                None,
                node.binding().metric_name,
                "{query} bound a metric to an input that reads no table: {display}"
            );
        }
    }

    /// Asserts every empty input under `plan` declares the typed empty raw series frame: the
    /// planner's own definition of that input, and the frame a scan would have declared, so the
    /// physical planner reads it as a frame producer instead of looking for a scan below it.
    fn assert_typed_empty_inputs(plan: &LogicalPlan, query: &str) {
        let empty = logical_plan_nodes(plan)
            .into_iter()
            .filter(|node| matches!(node, LogicalPlan::EmptyRelation(_)))
            .collect::<Vec<_>>();
        assert!(!empty.is_empty(), "{query} has no empty input");
        for node in empty {
            assert!(
                crate::promql::planner::native::is_empty_discovered_input(node),
                "{query}: {node:?}"
            );
            let LogicalPlan::EmptyRelation(relation) = node else {
                unreachable!();
            };
            assert!(!relation.produce_one_row, "{query}: {relation:?}");
            let schema = relation.schema.as_arrow();
            for (key, expected) in [
                ("greptime.series.content", "raw"),
                ("greptime.series.labels", "explicit"),
                ("greptime.series.ordering", "contiguous"),
            ] {
                assert_eq!(
                    Some(expected),
                    schema.metadata().get(key).map(String::as_str),
                    "{query}: {key}"
                );
            }
            // The one value field the empty input is bound to, named as the route names a value.
            let fields = schema.field_with_name("fields").expect("a series frame");
            let arrow_schema::DataType::Struct(payloads) = fields.data_type() else {
                panic!("{query}: {fields:?}");
            };
            assert_eq!(
                vec!["value"],
                payloads
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                "{query}"
            );
        }
    }

    /// Asserts the physical plan of a regionless native statement: the fused node runs on the
    /// frontend and no region read is planned.
    fn assert_regionless_physical_plan(
        physical: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        query: &str,
    ) {
        let names = physical_plan_names(physical);
        assert!(
            names.iter().any(|name| name == "FusedSeriesExec"),
            "{query} is evaluated on the frontend by the fused node: {physical:?}"
        );
        assert!(
            !names.iter().any(|name| name == "MergeScanExec"),
            "{query} reads no region: {physical:?}"
        );
    }

    /// A statement that reads no table plans natively over the wire, where there is no region to
    /// place anything in: the frontend evaluates the fused node under the native column names
    /// rather than the ones the existing planner derives from an expression.
    #[tokio::test]
    async fn test_remote_native_promql_plans_a_data_free_statement_natively() {
        let engine = create_promql_test_engine_with(
            crate::options::QueryOptions {
                experimental_native_promql: true,
                ..Default::default()
            },
            false,
        );
        let query_ctx = native_discovery_query_ctx();
        for (query, expected_columns, expected_values, expected_labels) in [
            // `time()` answers the evaluation grid in seconds.
            (
                "time()",
                &["time", "value"][..],
                &[0.0, 5.0, 10.0][..],
                &[][..],
            ),
            (
                "vector(1)",
                &["time", "value"][..],
                &[1.0, 1.0, 1.0][..],
                &[][..],
            ),
            (
                r#"label_replace(vector(1), "host", "host1", "", "")"#,
                &["time", "value", "host"][..],
                &[1.0, 1.0, 1.0][..],
                &[("host", "host1")][..],
            ),
            // A synthesized `__tsid` is published under the statement's own columns: the stripping
            // projection above the node is read through rather than mistaken for a remote plan.
            (
                r#"label_replace(vector(1), "__tsid", "x", "", "")"#,
                &["time", "value"][..],
                &[1.0, 1.0, 1.0][..],
                &[][..],
            ),
        ] {
            let plan = plan_remote_native_promql(&engine, &query_ctx, query).await;
            let display = plan.display_indent().to_string();
            assert_regionless_native_plan(&plan, query);
            assert!(
                !display.contains("greptime_value"),
                "{query} keeps the native value column: {display}"
            );
            // A data-free statement has no input at all, so nothing is scanned or left empty.
            assert_eq!(
                expected_columns,
                plan.schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                "{query}: {display}"
            );
            let (physical, batches) = execute_plan(&engine, plan, &query_ctx).await;
            assert_regionless_physical_plan(&physical, query);
            assert_eq!(
                expected_values,
                numeric_values(&batches, "value").as_slice(),
                "{query}: {batches:?}"
            );
            for (label, value) in expected_labels {
                assert_eq!(
                    vec![value.to_string(); batches_row_count(&batches)],
                    label_values(&batches, label),
                    "{query}: label `{label}`: {batches:?}"
                );
            }
        }
    }

    /// The rows every batch of `batches` holds.
    fn batches_row_count(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|batch| batch.num_rows()).sum()
    }

    /// A nameless selector that matches no metric table has no region to read either, so the typed
    /// empty input it is planned as stays that input on both routes and the node above it is
    /// evaluated on the frontend. Binding a metric to it is what used to turn this statement into
    /// an internal error over the wire.
    #[tokio::test]
    async fn test_remote_native_promql_plans_a_zero_match_selector_natively() {
        let engine = create_promql_test_engine_with(
            crate::options::QueryOptions {
                experimental_native_promql: true,
                ..Default::default()
            },
            false,
        );
        let query_ctx = native_discovery_query_ctx();
        for (query, expected_rows, expected_labels) in [
            // The selection itself, over the typed empty frame.
            (r#"{job="api"}"#, 0, &[][..]),
            // A whole-vector operation over it, which the route plans as a segment: its parent node
            // reads the segment's own fused node.
            (r#"sort({job="api"})"#, 0, &[][..]),
            // A fold splits into a `Final` half over the `Partial` half that reads the input.
            (r#"sum({job="api"})"#, 0, &[][..]),
            // `absent` answers one series per evaluation point over zero input, labelled by the
            // selector's matchers. The grid holds three points.
            (r#"absent({job="api"})"#, 3, &[("job", "api")][..]),
        ] {
            let plan = plan_remote_native_promql(&engine, &query_ctx, query).await;
            let display = plan.display_indent().to_string();
            assert_regionless_native_plan(&plan, query);
            assert!(
                display.contains("EmptyRelation"),
                "{query} keeps the typed empty input: {display}"
            );
            assert_typed_empty_inputs(&plan, query);
            let (physical, batches) = execute_plan(&engine, plan, &query_ctx).await;
            assert_regionless_physical_plan(&physical, query);
            // No table matched, so the answer is the empty vector, not an error.
            assert_eq!(
                expected_rows,
                batches_row_count(&batches),
                "{query} answered rows over a selector that matches no metric: {physical:?}"
            );
            for (label, value) in expected_labels {
                assert_eq!(
                    vec![value.to_string(); expected_rows],
                    label_values(&batches, label),
                    "{query}: label `{label}`: {batches:?}"
                );
            }
        }

        // An allowlist that admits no metric at all reaches the same empty input: the answer comes
        // from the statement, not from whatever the catalog holds.
        let query_ctx = native_query_ctx(MetricDiscoveryScope::allowed(std::iter::empty()));
        let query = r#"{job="api"}"#;
        let plan = plan_remote_native_promql(&engine, &query_ctx, query).await;
        assert_regionless_native_plan(&plan, "denied discovery");
        assert_typed_empty_inputs(&plan, "denied discovery");
        let (physical, batches) = execute_plan(&engine, plan, &query_ctx).await;
        assert_regionless_physical_plan(&physical, "denied discovery");
        assert_eq!(
            0,
            batches_row_count(&batches),
            "denied discovery: {physical:?}"
        );
    }

    /// Plans `sql` and runs the DataFusion analyzer, which is where
    /// `InsertAssignmentRule` sits. Planning alone stops short of it, so these
    /// assertions would not see the assignment rewrite at all.
    async fn analyze_insert(
        engine: &QueryEngineRef,
        sql: &str,
        query_ctx: &QueryContextRef,
    ) -> String {
        let stmt = QueryLanguageParser::parse_sql(sql, query_ctx).unwrap();
        let plan = engine
            .planner()
            .plan(&stmt, query_ctx.clone())
            .await
            .unwrap();
        let context = engine.engine_context(query_ctx.clone());
        let state = context.state();
        state
            .analyzer()
            .execute_and_check(plan, state.config_options(), |_, _| {})
            .unwrap()
            .display_indent_schema()
            .to_string()
    }

    #[tokio::test]
    async fn test_insert_timestamp_literals_use_query_timezone() {
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .timezone(Timezone::from_tz_string("Asia/Shanghai").unwrap())
                .build(),
        );
        let engine = create_timestamp_test_engine().await;

        for (sql, expected_timestamps) in [
            (
                "INSERT INTO timestamps (ts, st) \
                 VALUES ('2026-08-02 12:00:00.001', now())",
                &[1_785_643_200_001_i64][..],
            ),
            (
                "INSERT INTO timestamps (ts, st) \
                 SELECT '2026-08-03 12:00:00.001', now()",
                &[1_785_729_600_001_i64][..],
            ),
            (
                "INSERT INTO timestamps (ts, st) \
                 SELECT '2026-08-04 12:00:00.001', now() LIMIT 1",
                &[1_785_816_000_001_i64][..],
            ),
            (
                "INSERT INTO timestamps (ts, st) \
                 SELECT * FROM (\
                     SELECT '2026-08-05 12:00:00.001', now()\
                 ) AS source",
                &[1_785_902_400_001_i64][..],
            ),
            (
                "INSERT INTO timestamps (ts, st) \
                 SELECT '2026-08-18 12:00:00.001', max(st) \
                 FROM timestamps GROUP BY note",
                &[1_787_025_600_001_i64][..],
            ),
            (
                "INSERT INTO timestamps (ts, st) \
                 SELECT c, s FROM (\
                     SELECT '2026-08-13 12:00:00.001' AS c, now() AS s\
                 ) AS t WHERE c > '2026-01-01'",
                &[1_786_593_600_001_i64][..],
            ),
            (
                "INSERT INTO timestamps (ts, st) \
                 SELECT c, s FROM (\
                     SELECT '2026-08-14 12:00:00.001' AS c, now() AS s\
                 ) AS t ORDER BY c",
                &[1_786_680_000_001_i64][..],
            ),
            (
                "INSERT INTO timestamps (ts, st) \
                 SELECT DISTINCT c, s FROM (\
                     SELECT '2026-08-15 12:00:00.001' AS c, now() AS s\
                 ) AS t",
                &[1_786_766_400_001_i64][..],
            ),
        ] {
            let plan = analyze_insert(&engine, sql, &query_ctx).await;

            for expected_timestamp in expected_timestamps {
                assert!(
                    plan.contains(&format!("TimestampMillisecond({expected_timestamp}, None)")),
                    "{plan}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_insert_explicit_timestamp_cast_keeps_datafusion_semantics() {
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .timezone(Timezone::from_tz_string("Asia/Shanghai").unwrap())
                .build(),
        );
        let engine = create_timestamp_test_engine().await;
        let sql = "INSERT INTO timestamps (ts, st) \
                   VALUES (CAST('2026-08-08 12:00:00.001' AS TIMESTAMP), now())";
        let plan = analyze_insert(&engine, sql, &query_ctx).await;

        // An explicit cast reaches the analyzer as an `arrow_cast` call rather
        // than an `Expr::Cast`, which is how it stays out of the rewrite.
        assert!(
            plan.contains("arrow_cast(Utf8(\"2026-08-08 12:00:00.001\")"),
            "{plan}"
        );
        // 12:00:00.001 read as Shanghai local time; the source query keeps UTC.
        assert!(
            !plan.contains("TimestampMillisecond(1786104000001, None)"),
            "{plan}"
        );
    }

    #[tokio::test]
    async fn test_insert_converts_source_literal_shared_by_several_columns() {
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .timezone(Timezone::from_tz_string("Asia/Shanghai").unwrap())
                .build(),
        );
        let engine = create_timestamp_test_engine().await;

        // Rewriting `c` in place would also retype `note` and truncate `ts_ns`.
        for (sql, expected) in [
            (
                "INSERT INTO timestamps (ts, note) SELECT a, b FROM (\
                     SELECT c AS a, c AS b FROM (\
                         SELECT '2026-08-12 12:00:00.001' AS c\
                     ) AS t1\
                 ) AS t2",
                &[
                    "TimestampMillisecond(1786507200001, None)",
                    "[a:Utf8, b:Utf8]",
                ][..],
            ),
            (
                "INSERT INTO timestamps (ts, ts_ns) SELECT a, b FROM (\
                     SELECT c AS a, c AS b FROM (\
                         SELECT '2026-08-12 12:00:00.123456789' AS c\
                     ) AS t1\
                 ) AS t2",
                &[
                    "TimestampMillisecond(1786507200123, None)",
                    "TimestampNanosecond(1786507200123456789, None)",
                ][..],
            ),
        ] {
            let plan = analyze_insert(&engine, sql, &query_ctx).await;

            for expected in expected {
                assert!(plan.contains(expected), "{plan}");
            }
        }
    }

    #[tokio::test]
    async fn test_insert_union_converts_via_assignment_cast() {
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .timezone(Timezone::from_tz_string("Asia/Shanghai").unwrap())
                .build(),
        );
        let engine = create_timestamp_test_engine().await;
        // Branches disagree, so the conversion stays as a cast on the
        // assignment instead of folding. One cast covers every branch, which is
        // why a NULL branch no longer cancels the conversion for the column and
        // why UNION's dedup keys stay on the original strings.
        for sql in [
            "INSERT INTO timestamps (ts, st) \
             SELECT '2026-08-06 12:00:00.001', now() \
             UNION ALL \
             SELECT '2026-08-07 12:00:00.001', NULL",
            "INSERT INTO timestamps (ts, st) \
             SELECT '2026-08-16 12:00:00.001', now() \
             UNION \
             SELECT '2026-08-17 12:00:00.001', now()",
        ] {
            let plan = analyze_insert(&engine, sql, &query_ctx).await;

            assert!(
                plan.contains("AS Timestamp(ms, \"Asia/Shanghai\")"),
                "{plan}"
            );
            // The branches themselves are untouched.
            assert!(
                plan.contains("Utf8(\"2026-08-07 12:00:00.001\")")
                    || plan.contains("Utf8(\"2026-08-17 12:00:00.001\")"),
                "{plan}"
            );
        }
    }

    #[tokio::test]
    async fn test_insert_mixed_union_keeps_source_coercion() {
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .timezone(Timezone::from_tz_string("Asia/Shanghai").unwrap())
                .build(),
        );
        let engine = create_timestamp_test_engine().await;
        let sql = "INSERT INTO timestamps (ts, st) \
                   SELECT '2026-08-10 12:00:00.001', now() \
                   UNION ALL \
                   SELECT CAST('2026-08-11 12:00:00.001' AS TIMESTAMP), now()";
        let plan = analyze_insert(&engine, sql, &query_ctx).await;

        assert!(
            !plan.contains("TimestampMillisecond(1786334400001, None)"),
            "{plan}"
        );
        assert!(
            plan.contains("arrow_cast(Utf8(\"2026-08-11 12:00:00.001\")"),
            "{plan}"
        );
        // TypeCoercion has already settled this union to timestamp, so the
        // assignment has nothing left to reinterpret. Retargeting the cast here
        // would leave a Timestamp(None) -> Timestamp(Some(tz)) step behind,
        // which shifts the value instead of relabelling it.
        assert!(!plan.contains("Asia/Shanghai"), "{plan}");
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
    async fn test_get_inferred_parameter_types_limit_offset() {
        let plan = parse_sql_to_plan("SELECT id FROM test LIMIT $1 OFFSET $2").await;
        let types = DfLogicalPlanner::get_inferred_parameter_types(&plan).unwrap();

        assert_eq!(types.get("$1"), Some(&Some(DataType::Int64)));
        assert_eq!(types.get("$2"), Some(&Some(DataType::Int64)));
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

    #[tokio::test]
    async fn test_get_inferred_parameter_types_subquery() {
        let plan = parse_sql_to_plan(
            r#"SELECT * FROM test WHERE id = (SELECT id FROM test CROSS JOIN (SELECT parse_ident($1::TEXT) AS parts) p LIMIT 1)"#,
        ).await;
        let types = DfLogicalPlanner::get_inferred_parameter_types(&plan).unwrap();

        assert_eq!(types.len(), 1);
        let type_1 = types.get("$1").unwrap();
        assert_eq!(type_1, &Some(DataType::Utf8));
    }

    #[tokio::test]
    async fn test_get_inferred_parameter_types_insert() {
        let plan = parse_sql_to_plan("INSERT INTO test (id, name) VALUES ($1, $2), ($3, $4)").await;
        let types = DfLogicalPlanner::get_inferred_parameter_types(&plan).unwrap();

        assert_eq!(types.len(), 4);
        assert_eq!(types.get("$1"), Some(&Some(DataType::Int32)));
        assert_eq!(types.get("$2"), Some(&Some(DataType::Utf8)));
        assert_eq!(types.get("$3"), Some(&Some(DataType::Int32)));
        assert_eq!(types.get("$4"), Some(&Some(DataType::Utf8)));
    }

    /// A plan that publishes `columns` as its output, one literal per column, named and typed as
    /// the column is.
    fn plan_publishing(columns: &[(&str, ScalarValue)]) -> LogicalPlan {
        let exprs = columns
            .iter()
            .map(|(name, value)| DfExpr::Literal(value.clone(), None).alias(*name))
            .collect::<Vec<_>>();
        LogicalPlanBuilder::empty(false)
            .project(exprs)
            .unwrap()
            .build()
            .unwrap()
    }

    /// [`plan_publishing`], declaring what a plan of either route declares: the time index marked as
    /// one, and the schema version GreptimeDB's schemas carry.
    fn plan_publishing_marked(columns: &[(&str, ScalarValue)], time_index: &str) -> LogicalPlan {
        let plan = plan_publishing(columns);
        let LogicalPlan::Projection(projection) = plan else {
            unreachable!("plan_publishing builds a projection")
        };
        let fields = projection
            .schema
            .iter()
            .map(|(_, field)| {
                let field =
                    if field.name() == time_index {
                        Arc::new(field.as_ref().clone().with_metadata(
                            std::collections::HashMap::from([(
                                datatypes::schema::TIME_INDEX_KEY.to_string(),
                                "true".to_string(),
                            )]),
                        ))
                    } else {
                        Arc::clone(field)
                    };
                (None, field)
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(
            DFSchema::new_with_metadata(
                fields,
                std::collections::HashMap::from([(
                    datatypes::schema::VERSION_KEY.to_string(),
                    datatypes::schema::Schema::INITIAL_VERSION.to_string(),
                )]),
            )
            .unwrap(),
        );
        LogicalPlan::Projection(
            datafusion_expr::Projection::try_new_with_schema(
                projection.expr.clone(),
                projection.input.clone(),
                schema,
            )
            .unwrap(),
        )
    }

    /// The label column the existing planner publishes: dictionary-encoded text.
    fn dictionary_label(value: &str) -> ScalarValue {
        ScalarValue::Dictionary(
            Box::new(DataType::UInt32),
            Box::new(ScalarValue::Utf8(Some(value.to_string()))),
        )
    }

    /// The names of a plan's output columns, in order.
    fn column_names(plan: &LogicalPlan) -> Vec<String> {
        plan.schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    fn time_value() -> ScalarValue {
        ScalarValue::TimestampMillisecond(Some(0), None)
    }

    /// The native answer for the statement these tests use: time index first, the value under the
    /// tree's own field name, the labels last.
    fn native_answer() -> LogicalPlan {
        plan_publishing_marked(
            &[
                ("ts", time_value()),
                ("val", ScalarValue::Float64(Some(2.0))),
                ("host", ScalarValue::Utf8(Some("a".to_string()))),
            ],
            "ts",
        )
    }

    /// The existing planner's answer for the same statement: labels first, then the time index, then
    /// the value named after the expression it planned, with the label dictionary-encoded.
    fn existing_answer() -> LogicalPlan {
        plan_publishing_marked(
            &[
                ("host", dictionary_label("a")),
                ("ts", time_value()),
                ("lhs.val + rhs.val", ScalarValue::Float64(Some(2.0))),
            ],
            "ts",
        )
    }

    /// A CTE publishes the existing planner's columns over a native plan as well.
    ///
    /// The fused plan publishes the time index first, the value under the tree's field name and the
    /// labels last, in plain text; the existing planner publishes the labels first, then the time
    /// index, then the value under the name of the expression it planned, in dictionary-encoded text.
    /// What a CTE exposes is the second one, whichever route answered the statement, because a CTE's
    /// columns are SQL-visible - and a column list the CTE declares is applied to them by position.
    #[test]
    fn a_cte_publishes_the_existing_planners_columns_over_a_native_plan() {
        let native = native_answer();
        let existing = existing_answer();

        let published = publish_legacy_schema(native, &existing)
            .expect("every column has one answer in the other plan");

        assert_eq!(
            column_names(&published),
            vec!["host", "ts", "lhs.val + rhs.val"],
            "the CTE publishes the existing planner's column order and names"
        );
        assert!(
            publishes_columns(published.schema().as_ref(), existing.schema().as_ref()),
            "and every column it publishes: name, order, type, nullability and metadata: {}",
            published.schema()
        );
        assert_eq!(
            published
                .schema()
                .field_with_name(None, "host")
                .unwrap()
                .data_type(),
            &DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            "a label is published as the encoding the existing planner publishes"
        );
        let LogicalPlan::Projection(projection) = &published else {
            panic!("the native plan is wrapped in a projection: {published}");
        };
        assert_eq!(
            column_names(projection.input.as_ref()),
            vec!["ts", "val", "host"],
            "the native plan is kept as the projection's input"
        );
    }

    /// What the CTE publishes survives the analyzer, which rebuilds every projection from its
    /// expressions.
    ///
    /// The declared schema of a projection is not what analysis keeps: the analyzer calls
    /// `with_new_exprs`, which rebuilds the projection and derives its schema from the expressions
    /// and their input. So the columns have to be carried by the expressions themselves - an alias
    /// per column and a cast for the encoding - or they are gone by the time the CTE is read.
    #[test]
    fn what_a_cte_publishes_survives_the_analyzer_rebuilding_the_projection() {
        let existing = existing_answer();
        let published = publish_legacy_schema(native_answer(), &existing).unwrap();

        // The rebuild `DistPlannerAnalyzer` performs on every projection it visits.
        let rebuilt = published
            .with_new_exprs(
                published.expressions(),
                published.inputs().into_iter().cloned().collect(),
            )
            .unwrap();

        assert!(
            publishes_columns(rebuilt.schema().as_ref(), existing.schema().as_ref()),
            "the rebuilt projection publishes the same columns: {}",
            rebuilt.schema()
        );
        assert_eq!(
            rebuilt.to_string(),
            published.to_string(),
            "and the same expressions"
        );
    }

    /// A statement whose columns do not pair keeps the existing plan.
    ///
    /// Publishing a column this cannot fill would change what the CTE means, so a shape it cannot
    /// map faithfully is refused instead: the caller keeps the existing plan and the CTE keeps the
    /// columns it has always published.
    #[test]
    fn a_shape_with_no_faithful_column_mapping_keeps_the_existing_plan() {
        // One native value column cannot fill two of the existing plan's value columns.
        let two_values = plan_publishing_marked(
            &[
                ("host", dictionary_label("a")),
                ("ts", time_value()),
                ("lhs.val + rhs.val", ScalarValue::Float64(Some(2.0))),
                ("lhs.val - rhs.val", ScalarValue::Float64(Some(0.0))),
            ],
            "ts",
        );
        assert!(publish_legacy_schema(native_answer(), &two_values).is_none());

        // A time index the other route had to rename is marked as the time index on both sides, and
        // a column that carries metadata is never reproduced by a cast.
        let renamed_time = plan_publishing_marked(
            &[
                ("native_time_0", time_value()),
                ("val", ScalarValue::Float64(Some(2.0))),
                ("host", ScalarValue::Utf8(Some("a".to_string()))),
            ],
            "native_time_0",
        );
        let existing = plan_publishing_marked(
            &[
                ("host", dictionary_label("a")),
                ("ts", time_value()),
                ("val", ScalarValue::Float64(Some(2.0))),
            ],
            "ts",
        );
        assert!(publish_legacy_schema(renamed_time, &existing).is_none());
    }

    #[tokio::test]
    async fn test_get_inferred_parameter_types_arrow_cast() {
        let plan = parse_sql_to_plan("SELECT $1::INT64, $2::FLOAT64, $3::INT16, $4::INT32, $5::UINT8, $6::UINT16, $7::UINT32").await;
        let types = DfLogicalPlanner::get_inferred_parameter_types(&plan).unwrap();

        assert_eq!(types.get("$1"), Some(&Some(DataType::Int64)));
        assert_eq!(types.get("$2"), Some(&Some(DataType::Float64)));
        assert_eq!(types.get("$3"), Some(&Some(DataType::Int16)));
        assert_eq!(types.get("$4"), Some(&Some(DataType::Int32)));
        assert_eq!(types.get("$5"), Some(&Some(DataType::UInt8)));
        assert_eq!(types.get("$6"), Some(&Some(DataType::UInt16)));
        assert_eq!(types.get("$7"), Some(&Some(DataType::UInt32)));

        let plan = parse_sql_to_plan("SELECT $1::INT8, $2::FLOAT8, $3::INT2, $4::INT8").await;
        let types = DfLogicalPlanner::get_inferred_parameter_types(&plan).unwrap();

        assert_eq!(types.get("$1"), Some(&Some(DataType::Int64)));
        assert_eq!(types.get("$2"), Some(&Some(DataType::Float64)));
        assert_eq!(types.get("$3"), Some(&Some(DataType::Int16)));
    }
}
