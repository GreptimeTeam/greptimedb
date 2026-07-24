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

pub mod builder;
mod dashboard;
mod grpc;
mod influxdb;
mod jaeger;
mod log_handler;
mod logs;
mod opentsdb;
mod otlp;
pub mod prom_store;
mod promql;
mod region_query;
pub mod standalone;

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, atomic};
use std::time::{Duration, SystemTime};

use async_stream::stream;
use async_trait::async_trait;
use auth::{
    PermissionChecker, PermissionCheckerRef, PermissionReq, PermissionTableTarget,
    PermissionTableTargets,
};
use catalog::CatalogManagerRef;
use catalog::process_manager::{
    ProcessManagerRef, QueryStatement as CatalogQueryStatement, SlowQueryTimer,
};
use client::OutputData;
use common_base::Plugins;
use common_base::cancellation::CancellableFuture;
use common_error::ext::{BoxedError, ErrorExt};
use common_event_recorder::EventRecorderRef;
use common_meta::cache::TableFlownodeSetCacheRef;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::table_name::TableNameKey;
use common_meta::node_manager::NodeManagerRef;
use common_meta::procedure_executor::ProcedureExecutorRef;
use common_query::Output;
use common_recordbatch::RecordBatchStreamWrapper;
use common_recordbatch::error::StreamTimeoutSnafu;
use common_telemetry::logging::SlowQueryOptions;
use common_telemetry::{debug, error, tracing};
use dashmap::DashMap;
use datafusion_expr::LogicalPlan;
use futures::{Stream, StreamExt, future};
use lazy_static::lazy_static;
use operator::delete::DeleterRef;
use operator::insert::InserterRef;
use operator::statement::{StatementExecutor, StatementExecutorRef};
use partition::manager::PartitionRuleManagerRef;
use pipeline::pipeline_operator::PipelineOperator;
use prometheus::HistogramTimer;
use promql_parser::label::Matcher;
use query::QueryEngineRef;
use query::metrics::OnDone;
use query::parser::{PromQuery, QueryStatement};
use query::query_engine::DescribeResult;
use query::query_engine::options::{QueryOptions, validate_catalog_and_schema};
use servers::error::{
    self as server_error, AuthSnafu, CommonMetaSnafu, ExecuteQuerySnafu,
    OtlpMetricModeIncompatibleSnafu, UnexpectedResultSnafu,
};
use servers::interceptor::{
    PromQueryInterceptor, PromQueryInterceptorRef, SqlQueryInterceptor, SqlQueryInterceptorRef,
};
use servers::otlp::metrics::legacy_normalize_otlp_name;
use servers::prometheus_handler::{
    ParsedPromQuery, PrometheusHandler, resolve_schema_from_matchers,
};
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{Channel, QueryContextRef};
use session::table_name::table_idents_to_full_name;
use snafu::prelude::*;
use sql::ast::ObjectNamePartExt;
use sql::dialect::Dialect;
use sql::parser::{ParseOptions, ParserContext};
use sql::statements::comment::CommentObject;
use sql::statements::copy::{CopyDatabase, CopyTable};
use sql::statements::statement::Statement;
use sql::statements::tql::Tql;
use sql::util::{extract_tables_from_prom_expr_checked, extract_tables_from_statement_checked};
use sqlparser::ast::{AnalyzeFormat, ObjectName};
pub use standalone::StandaloneDatanodeManager;
use table::requests::{OTLP_METRIC_COMPAT_KEY, OTLP_METRIC_COMPAT_PROM};
use tracing::Span;

use crate::error::{
    self, CollectRecordbatchSnafu, Error, ExecLogicalPlanSnafu, ExecutePromqlSnafu, ExternalSnafu,
    InvalidSqlSnafu, ParseSqlSnafu, PermissionSnafu, PlanStatementSnafu, Result,
    SqlExecInterceptedSnafu, StatementTimeoutSnafu, TableOperationSnafu,
};
use crate::service_config::InfluxdbMergeMode;
use crate::stream_wrapper::CancellableStreamWrapper;

lazy_static! {
    static ref OTLP_LEGACY_DEFAULT_VALUE: String = "legacy".to_string();
}

/// The frontend instance contains necessary components, and implements many
/// traits, like [`servers::query_handler::grpc::GrpcQueryHandler`],
/// [`servers::query_handler::sql::SqlQueryHandler`], etc.
#[derive(Clone)]
pub struct Instance {
    frontend_peer_addr: String,
    catalog_manager: CatalogManagerRef,
    pipeline_operator: Arc<PipelineOperator>,
    statement_executor: Arc<StatementExecutor>,
    query_engine: QueryEngineRef,
    plugins: Plugins,
    inserter: InserterRef,
    deleter: DeleterRef,
    table_metadata_manager: TableMetadataManagerRef,
    event_recorder: Option<EventRecorderRef>,
    process_manager: ProcessManagerRef,
    slow_query_options: SlowQueryOptions,
    influxdb_default_merge_mode: InfluxdbMergeMode,
    trace_ingest_chunk_size: usize,
    suspend: Arc<AtomicBool>,

    // cache for otlp metrics
    // first layer key: db-string
    // key: direct input metric name
    // value: if runs in legacy mode
    otlp_metrics_table_legacy_cache: DashMap<String, DashMap<String, bool>>,
}

impl Instance {
    pub fn frontend_peer_addr(&self) -> &str {
        &self.frontend_peer_addr
    }

    pub fn catalog_manager(&self) -> &CatalogManagerRef {
        &self.catalog_manager
    }

    pub fn query_engine(&self) -> &QueryEngineRef {
        &self.query_engine
    }

    pub fn plugins(&self) -> &Plugins {
        &self.plugins
    }

    pub fn statement_executor(&self) -> &StatementExecutorRef {
        &self.statement_executor
    }

    pub fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }

    pub fn inserter(&self) -> &InserterRef {
        &self.inserter
    }

    pub fn process_manager(&self) -> &ProcessManagerRef {
        &self.process_manager
    }

    pub fn node_manager(&self) -> &NodeManagerRef {
        self.inserter.node_manager()
    }

    pub fn partition_manager(&self) -> &PartitionRuleManagerRef {
        self.inserter.partition_manager()
    }

    pub fn table_flownode_set_cache(&self) -> &TableFlownodeSetCacheRef {
        self.inserter.table_flownode_set_cache()
    }

    pub fn cache_invalidator(&self) -> &CacheInvalidatorRef {
        self.statement_executor.cache_invalidator()
    }

    pub fn procedure_executor(&self) -> &ProcedureExecutorRef {
        self.statement_executor.procedure_executor()
    }

    pub fn suspend_state(&self) -> Arc<AtomicBool> {
        self.suspend.clone()
    }

    pub(crate) fn is_suspended(&self) -> bool {
        self.suspend.load(atomic::Ordering::Relaxed)
    }
}

fn parse_stmt(sql: &str, dialect: &(dyn Dialect + Send + Sync)) -> Result<Vec<Statement>> {
    ParserContext::create_with_dialect(sql, dialect, ParseOptions::default()).context(ParseSqlSnafu)
}

fn validate_analyze_stream_statement(stmt: &mut Statement) -> Result<()> {
    let Statement::Explain(explain) = stmt else {
        return InvalidSqlSnafu {
            err_msg: "only EXPLAIN ANALYZE VERBOSE statement is supported",
        }
        .fail();
    };
    ensure!(
        explain.analyze && explain.verbose,
        InvalidSqlSnafu {
            err_msg: "statement must be EXPLAIN ANALYZE VERBOSE"
        }
    );
    match explain.format {
        None | Some(AnalyzeFormat::JSON) => {
            // Keep explicit FORMAT JSON accepted, but pass JSON through
            // QueryContext.explain_format instead of the statement to avoid the
            // planner's current `EXPLAIN VERBOSE with FORMAT` limitation.
            explain.format = None;
            Ok(())
        }
        Some(_) => InvalidSqlSnafu {
            err_msg: "only FORMAT JSON is supported for analyze stream",
        }
        .fail(),
    }
}

impl Instance {
    fn statement_slow_query_timer(
        &self,
        stmt: &Statement,
        schema_name: String,
    ) -> Option<SlowQueryTimer> {
        if !stmt.is_readonly() || !self.slow_query_options.enable {
            return None;
        }

        self.event_recorder.clone().map(|event_recorder| {
            SlowQueryTimer::new(
                CatalogQueryStatement::Sql(stmt.clone()),
                schema_name,
                self.slow_query_options.threshold,
                self.slow_query_options.sample_ratio,
                self.slow_query_options.record_type,
                event_recorder,
            )
        })
    }

    async fn query_statement(&self, stmt: Statement, query_ctx: QueryContextRef) -> Result<Output> {
        check_permission(self.plugins.clone(), &stmt, &query_ctx)?;

        let query_interceptor = self.plugins.get::<SqlQueryInterceptorRef<Error>>();
        let query_interceptor = query_interceptor.as_ref();

        if should_track_statement_process(&stmt) {
            let catalog_name = query_ctx.current_catalog().to_string();
            let schema_name = query_ctx.current_schema();
            let slow_query_timer = self.statement_slow_query_timer(&stmt, schema_name.clone());

            let ticket = self.process_manager.register_query(
                catalog_name,
                vec![schema_name],
                stmt.to_string(),
                query_ctx.conn_info().to_string(),
                Some(query_ctx.process_id()),
                slow_query_timer,
            );

            let query_fut = self.exec_statement_with_timeout(stmt, query_ctx, query_interceptor);

            CancellableFuture::new(query_fut, ticket.cancellation_handle.clone())
                .await
                .map_err(|_| error::CancelledSnafu.build())?
                .map(|output| {
                    let Output { meta, data } = output;

                    let data = match data {
                        OutputData::Stream(stream) => OutputData::Stream(Box::pin(
                            CancellableStreamWrapper::new(stream, ticket),
                        )),
                        other => other,
                    };
                    Output { data, meta }
                })
        } else {
            self.exec_statement_with_timeout(stmt, query_ctx, query_interceptor)
                .await
        }
    }

    async fn exec_statement_with_timeout(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
        query_interceptor: Option<&SqlQueryInterceptorRef<Error>>,
    ) -> Result<Output> {
        let timeout = derive_timeout(&stmt, &query_ctx);
        match timeout {
            Some(timeout) => {
                let start = tokio::time::Instant::now();
                let output = tokio::time::timeout(
                    timeout,
                    self.exec_statement(stmt, query_ctx, query_interceptor),
                )
                .await
                .map_err(|_| StatementTimeoutSnafu.build())??;
                let output = map_query_output(output)?;
                // compute remaining timeout
                let remaining_timeout = timeout.checked_sub(start.elapsed()).unwrap_or_default();
                attach_timeout(output, remaining_timeout)
            }
            None => self
                .exec_statement(stmt, query_ctx, query_interceptor)
                .await
                .and_then(map_query_output),
        }
    }

    async fn exec_statement(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
        query_interceptor: Option<&SqlQueryInterceptorRef<Error>>,
    ) -> Result<Output> {
        match stmt {
            Statement::Query(_) | Statement::Explain(_) | Statement::Delete(_) => {
                // TODO: remove this when format is supported in datafusion
                if let Statement::Explain(explain) = &stmt
                    && let Some(format) = explain.format()
                {
                    query_ctx.set_explain_format(format.to_string());
                }

                self.plan_and_exec_sql(stmt, &query_ctx, query_interceptor)
                    .await
            }
            Statement::Tql(tql) => {
                self.plan_and_exec_tql(&query_ctx, query_interceptor, tql)
                    .await
            }
            _ => {
                query_interceptor.pre_execute(Some(&stmt), None, query_ctx.clone())?;
                self.statement_executor
                    .execute_sql(stmt, query_ctx)
                    .await
                    .context(TableOperationSnafu)
            }
        }
    }

    async fn plan_and_exec_sql(
        &self,
        stmt: Statement,
        query_ctx: &QueryContextRef,
        query_interceptor: Option<&SqlQueryInterceptorRef<Error>>,
    ) -> Result<Output> {
        let stmt = QueryStatement::Sql(stmt);
        let plan = self
            .statement_executor
            .plan(&stmt, query_ctx.clone())
            .await?;
        let QueryStatement::Sql(stmt) = stmt else {
            unreachable!()
        };
        query_interceptor.pre_execute(Some(&stmt), Some(&plan), query_ctx.clone())?;

        self.statement_executor
            .exec_plan(plan, query_ctx.clone())
            .await
            .context(TableOperationSnafu)
    }

    async fn plan_and_exec_tql(
        &self,
        query_ctx: &QueryContextRef,
        query_interceptor: Option<&SqlQueryInterceptorRef<Error>>,
        tql: Tql,
    ) -> Result<Output> {
        let plan = self
            .statement_executor
            .plan_tql(tql.clone(), query_ctx)
            .await?;
        query_interceptor.pre_execute(
            Some(&Statement::Tql(tql)),
            Some(&plan),
            query_ctx.clone(),
        )?;
        self.statement_executor
            .exec_plan(plan, query_ctx.clone())
            .await
            .context(TableOperationSnafu)
    }

    async fn check_otlp_legacy(
        &self,
        names: &[String],
        ctx: &QueryContextRef,
    ) -> server_error::Result<bool> {
        let db_string = ctx.get_db_string();
        // fast cache check
        let cache = self
            .otlp_metrics_table_legacy_cache
            .entry(db_string.clone())
            .or_default();
        if let Some(flag) = fast_legacy_check(&cache, names)? {
            return Ok(flag);
        }
        // release cache reference to avoid lock contention
        drop(cache);

        let catalog = ctx.current_catalog();
        let schema = ctx.current_schema();

        // query legacy table names
        let normalized_names = names
            .iter()
            .map(|n| legacy_normalize_otlp_name(n))
            .collect::<Vec<_>>();
        let table_names = normalized_names
            .iter()
            .map(|n| TableNameKey::new(catalog, &schema, n))
            .collect::<Vec<_>>();
        let table_values = self
            .table_metadata_manager()
            .table_name_manager()
            .batch_get(table_names)
            .await
            .context(CommonMetaSnafu)?;
        let table_ids = table_values
            .into_iter()
            .filter_map(|v| v.map(|vi| vi.table_id()))
            .collect::<Vec<_>>();

        // means no existing table is found, use new mode
        if table_ids.is_empty() {
            return Ok(false);
        }

        // has existing table, check table options
        let table_infos = self
            .table_metadata_manager()
            .table_info_manager()
            .batch_get(&table_ids)
            .await
            .context(CommonMetaSnafu)?;
        let options = table_infos
            .values()
            .map(|info| {
                info.table_info
                    .meta
                    .options
                    .extra_options
                    .get(OTLP_METRIC_COMPAT_KEY)
                    .unwrap_or(&OTLP_LEGACY_DEFAULT_VALUE)
            })
            .collect::<Vec<_>>();
        if !options.is_empty() {
            // check value consistency
            let has_prom = options.iter().any(|opt| *opt == OTLP_METRIC_COMPAT_PROM);
            let has_legacy = options
                .iter()
                .any(|opt| *opt == OTLP_LEGACY_DEFAULT_VALUE.as_str());
            ensure!(!(has_prom && has_legacy), OtlpMetricModeIncompatibleSnafu);
            Ok(has_legacy)
        } else {
            // no table info, use new mode
            Ok(false)
        }
    }

    fn cache_otlp_legacy(
        &self,
        names: &[String],
        ctx: &QueryContextRef,
        is_legacy: bool,
    ) -> server_error::Result<()> {
        let cache = self
            .otlp_metrics_table_legacy_cache
            .entry(ctx.get_db_string())
            .or_default();
        cache_legacy_mode(&cache, names, is_legacy)
    }
}

fn fast_legacy_check(
    cache: &DashMap<String, bool>,
    names: &[String],
) -> server_error::Result<Option<bool>> {
    let hit_cache = names
        .iter()
        .filter_map(|name| cache.get(name))
        .collect::<Vec<_>>();
    if !hit_cache.is_empty() {
        let hit_legacy = hit_cache.iter().any(|en| *en.value());
        let hit_prom = hit_cache.iter().any(|en| !*en.value());

        // hit but have true and false, means both legacy and new mode are used
        // we cannot handle this case, so return error
        // add doc links in err msg later
        ensure!(!(hit_legacy && hit_prom), OtlpMetricModeIncompatibleSnafu);

        Ok(Some(hit_legacy))
    } else {
        Ok(None)
    }
}

fn cache_legacy_mode(
    cache: &DashMap<String, bool>,
    names: &[String],
    is_legacy: bool,
) -> server_error::Result<()> {
    for name in names {
        let cached = cache.entry(name.clone()).or_insert(is_legacy);
        ensure!(*cached == is_legacy, OtlpMetricModeIncompatibleSnafu);
    }
    Ok(())
}

/// If the relevant variables are set, the timeout is enforced for all PostgreSQL statements.
/// For MySQL, it applies only to read-only statements.
fn derive_timeout(stmt: &Statement, query_ctx: &QueryContextRef) -> Option<Duration> {
    let query_timeout = query_ctx.query_timeout()?;
    if query_timeout.is_zero() {
        return None;
    }
    match query_ctx.channel() {
        Channel::Mysql if stmt.is_readonly() => Some(query_timeout),
        Channel::Postgres => Some(query_timeout),
        _ => None,
    }
}

/// Derives timeout for plan execution.
fn derive_timeout_for_plan(plan: &LogicalPlan, query_ctx: &QueryContextRef) -> Option<Duration> {
    let query_timeout = query_ctx.query_timeout()?;
    if query_timeout.is_zero() {
        return None;
    }
    match query_ctx.channel() {
        Channel::Mysql if is_readonly_plan(plan) => Some(query_timeout),
        Channel::Postgres => Some(query_timeout),
        _ => None,
    }
}

fn attach_timeout(output: Output, mut timeout: Duration) -> Result<Output> {
    if timeout.is_zero() {
        return StatementTimeoutSnafu.fail();
    }

    let output = match output.data {
        OutputData::AffectedRows(_) | OutputData::RecordBatches(_) => output,
        OutputData::Stream(mut stream) => {
            let schema = stream.schema();
            let s = Box::pin(stream! {
                let mut start = tokio::time::Instant::now();
                while let Some(item) = tokio::time::timeout(timeout, stream.next()).await.map_err(|_| StreamTimeoutSnafu.build())? {
                    yield item;

                    let now = tokio::time::Instant::now();
                    timeout = timeout.checked_sub(now - start).unwrap_or(Duration::ZERO);
                    start = now;
                    // tokio::time::timeout may not return an error immediately when timeout is 0.
                    if timeout.is_zero() {
                        StreamTimeoutSnafu.fail()?;
                    }
                }
            }) as Pin<Box<dyn Stream<Item = _> + Send>>;
            let stream = RecordBatchStreamWrapper {
                schema,
                stream: s,
                output_ordering: None,
                metrics: Default::default(),
                span: Span::current(),
            };
            Output::new(OutputData::Stream(Box::pin(stream)), output.meta)
        }
    };

    Ok(output)
}

impl Instance {
    async fn check_sql_permission(
        &self,
        stmt: &Statement,
        query_ctx: &QueryContextRef,
    ) -> Result<()> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission_with_context(
                query_ctx.current_user(),
                PermissionReq::SqlStatement(stmt),
                Some(&query_ctx.current_schema()),
            )
            .context(PermissionSnafu)?;

        let targets = match extract_tables_from_statement_checked(stmt) {
            Some(tables) => PermissionTableTargets::resolved(
                tables
                    .map(|name| {
                        table_idents_to_full_name(&name, query_ctx).map(
                            |(catalog, schema, table)| {
                                PermissionTableTarget::new(catalog, schema, table)
                            },
                        )
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?,
            ),
            None => PermissionTableTargets::Unresolved,
        };
        let targets = self
            .resolve_query_permission_targets(targets, query_ctx)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        self.check_table_permission(query_ctx, PermissionReq::SqlStatement(stmt), targets)
            .context(PermissionSnafu)?;
        Ok(())
    }

    #[tracing::instrument(skip_all, name = "SqlQueryHandler::do_analyze_stream_query")]
    async fn do_analyze_stream_query_inner(
        &self,
        query: &str,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        ensure!(!self.is_suspended(), error::SuspendedSnafu);

        let query_interceptor_opt = self.plugins.get::<SqlQueryInterceptorRef<Error>>();
        let query_interceptor = query_interceptor_opt.as_ref();
        let query = query_interceptor.pre_parsing(query, query_ctx.clone())?;
        let mut stmts = parse_stmt(query.as_ref(), query_ctx.sql_dialect())
            .and_then(|stmts| query_interceptor.post_parsing(stmts, query_ctx.clone()))?;

        ensure!(
            stmts.len() == 1,
            InvalidSqlSnafu {
                err_msg: "only single EXPLAIN ANALYZE VERBOSE statement is supported"
            }
        );
        let mut stmt = stmts.remove(0);
        validate_analyze_stream_statement(&mut stmt)?;
        query_ctx.set_explain_format(AnalyzeFormat::JSON.to_string());

        self.check_sql_permission(&stmt, &query_ctx).await?;
        check_permission(self.plugins.clone(), &stmt, &query_ctx)?;
        let catalog_name = query_ctx.current_catalog().to_string();
        let schema_name = query_ctx.current_schema();
        let slow_query_timer = self.statement_slow_query_timer(&stmt, schema_name.clone());
        let ticket = self.process_manager.register_query(
            catalog_name,
            vec![schema_name],
            stmt.to_string(),
            query_ctx.conn_info().to_string(),
            Some(query_ctx.process_id()),
            slow_query_timer,
        );
        let query_fut =
            self.exec_statement_with_timeout(stmt, query_ctx.clone(), query_interceptor);
        let output = CancellableFuture::new(query_fut, ticket.cancellation_handle.clone())
            .await
            .map_err(|_| error::CancelledSnafu.build())??;
        let Output { meta, data } = output;
        let data = match data {
            OutputData::Stream(stream) => OutputData::Stream(Box::pin(
                CancellableStreamWrapper::new_cancel_on_drop(stream, ticket),
            )),
            other => other,
        };
        query_interceptor.post_execute(Output { data, meta }, query_ctx)
    }

    #[tracing::instrument(skip_all, name = "SqlQueryHandler::do_query")]
    async fn do_query_inner(&self, query: &str, query_ctx: QueryContextRef) -> Vec<Result<Output>> {
        if self.is_suspended() {
            return vec![error::SuspendedSnafu {}.fail()];
        }

        let query_interceptor_opt = self.plugins.get::<SqlQueryInterceptorRef<Error>>();
        let query_interceptor = query_interceptor_opt.as_ref();
        let query = match query_interceptor.pre_parsing(query, query_ctx.clone()) {
            Ok(q) => q,
            Err(e) => return vec![Err(e)],
        };

        match parse_stmt(query.as_ref(), query_ctx.sql_dialect())
            .and_then(|stmts| query_interceptor.post_parsing(stmts, query_ctx.clone()))
        {
            Ok(stmts) => {
                if stmts.is_empty() {
                    return vec![
                        InvalidSqlSnafu {
                            err_msg: "empty statements",
                        }
                        .fail(),
                    ];
                }

                let mut results = Vec::with_capacity(stmts.len());
                for stmt in stmts {
                    if let Err(e) = self.check_sql_permission(&stmt, &query_ctx).await {
                        results.push(Err(e));
                        break;
                    }

                    match self.query_statement(stmt.clone(), query_ctx.clone()).await {
                        Ok(output) => {
                            let output_result =
                                query_interceptor.post_execute(output, query_ctx.clone());
                            results.push(output_result);
                        }
                        Err(e) => {
                            if e.status_code().should_log_error() {
                                error!(e; "Failed to execute query: {stmt}");
                            } else {
                                debug!("Failed to execute query: {stmt}, {e}");
                            }
                            results.push(Err(e));
                            break;
                        }
                    }
                }
                results
            }
            Err(e) => {
                vec![Err(e)]
            }
        }
    }

    async fn exec_plan(&self, plan: LogicalPlan, query_ctx: QueryContextRef) -> Result<Output> {
        self.query_engine
            .execute(plan, query_ctx)
            .await
            .context(ExecLogicalPlanSnafu)
    }

    async fn exec_plan_with_timeout(
        &self,
        plan: LogicalPlan,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let timeout = derive_timeout_for_plan(&plan, &query_ctx);
        match timeout {
            Some(timeout) => {
                let start = tokio::time::Instant::now();
                let output = tokio::time::timeout(timeout, self.exec_plan(plan, query_ctx))
                    .await
                    .map_err(|_| StatementTimeoutSnafu.build())??;
                let output = map_query_output(output)?;
                let remaining_timeout = timeout.checked_sub(start.elapsed()).unwrap_or_default();
                attach_timeout(output, remaining_timeout)
            }
            None => self
                .exec_plan(plan, query_ctx)
                .await
                .and_then(map_query_output),
        }
    }

    async fn do_exec_plan_inner(
        &self,
        plan: LogicalPlan,
        stmt: Option<Statement>,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        ensure!(!self.is_suspended(), error::SuspendedSnafu);

        let query_interceptor_opt = self.plugins.get::<SqlQueryInterceptorRef<Error>>();
        let query_interceptor = query_interceptor_opt.as_ref();

        query_interceptor.pre_execute(stmt.as_ref(), Some(&plan), query_ctx.clone())?;

        let query = stmt
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| plan.display_indent().to_string());

        let plan_is_readonly = is_readonly_plan(&plan);
        let result = if should_track_plan_process(stmt.as_ref(), &plan) {
            let catalog_name = query_ctx.current_catalog().to_string();
            let schema_name = query_ctx.current_schema();
            let slow_query_timer = if plan_is_readonly {
                self.slow_query_options
                    .enable
                    .then(|| self.event_recorder.clone())
                    .flatten()
                    .map(|event_recorder| {
                        SlowQueryTimer::new(
                            CatalogQueryStatement::Plan(query.clone()),
                            schema_name.clone(),
                            self.slow_query_options.threshold,
                            self.slow_query_options.sample_ratio,
                            self.slow_query_options.record_type,
                            event_recorder,
                        )
                    })
            } else {
                None
            };

            let ticket = self.process_manager.register_query(
                catalog_name,
                vec![schema_name],
                query,
                query_ctx.conn_info().to_string(),
                Some(query_ctx.process_id()),
                slow_query_timer,
            );

            let query_fut = self.exec_plan_with_timeout(plan, query_ctx.clone());

            CancellableFuture::new(query_fut, ticket.cancellation_handle.clone())
                .await
                .map_err(|_| error::CancelledSnafu.build())?
                .map(|output| {
                    let Output { meta, data } = output;

                    let data = match data {
                        OutputData::Stream(stream) => OutputData::Stream(Box::pin(
                            CancellableStreamWrapper::new(stream, ticket),
                        )),
                        other => other,
                    };
                    Output { data, meta }
                })
        } else {
            self.exec_plan_with_timeout(plan, query_ctx.clone()).await
        };

        result.and_then(|output| query_interceptor.post_execute(output, query_ctx))
    }

    #[tracing::instrument(skip_all, name = "SqlQueryHandler::do_promql_query")]
    async fn do_promql_query_inner(
        &self,
        query: &PromQuery,
        query_ctx: QueryContextRef,
    ) -> Vec<Result<Output>> {
        if self.is_suspended() {
            return vec![error::SuspendedSnafu {}.fail()];
        }

        // check will be done in prometheus handler's do_query
        let result = PrometheusHandler::do_query(self, query, query_ctx)
            .await
            .with_context(|_| ExecutePromqlSnafu {
                query: format!("{query:?}"),
            });
        vec![result]
    }

    async fn do_describe_inner(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Option<DescribeResult>> {
        ensure!(!self.is_suspended(), error::SuspendedSnafu);

        // EXPLAIN / EXPLAIN ANALYZE wrap an inner statement; describe them when the
        // wrapped statement is something we already plan (so that bind parameters
        // in the inner query get their types inferred). See #8029.
        let is_inner_plannable = |s: &Statement| {
            matches!(
                s,
                Statement::Insert(_) | Statement::Query(_) | Statement::Delete(_)
            )
        };
        let plannable = is_inner_plannable(&stmt)
            || matches!(&stmt, Statement::Explain(explain) if is_inner_plannable(explain.statement.as_ref()));

        if plannable {
            self.check_sql_permission(&stmt, &query_ctx).await?;

            let plan = self
                .query_engine
                .planner()
                .plan(&QueryStatement::Sql(stmt), query_ctx.clone())
                .await
                .context(PlanStatementSnafu)?;
            self.query_engine
                .describe(plan, query_ctx)
                .await
                .map(Some)
                .context(error::DescribeStatementSnafu)
        } else {
            Ok(None)
        }
    }

    async fn is_valid_schema_inner(&self, catalog: &str, schema: &str) -> Result<bool> {
        self.catalog_manager
            .schema_exists(catalog, schema, None)
            .await
            .context(error::CatalogSnafu)
    }
}

#[async_trait]
impl SqlQueryHandler for Instance {
    async fn do_query(
        &self,
        query: &str,
        query_ctx: QueryContextRef,
    ) -> Vec<server_error::Result<Output>> {
        self.do_query_inner(query, query_ctx)
            .await
            .into_iter()
            .map(|result| result.map_err(BoxedError::new).context(ExecuteQuerySnafu))
            .collect()
    }

    async fn do_analyze_stream_query(
        &self,
        query: &str,
        query_ctx: QueryContextRef,
    ) -> server_error::Result<Output> {
        self.do_analyze_stream_query_inner(query, query_ctx)
            .await
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)
    }

    async fn do_exec_plan(
        &self,
        plan: LogicalPlan,
        stmt: Option<Statement>,
        query_ctx: QueryContextRef,
    ) -> server_error::Result<Output> {
        self.do_exec_plan_inner(plan, stmt, query_ctx)
            .await
            .map_err(BoxedError::new)
            .context(server_error::ExecutePlanSnafu)
    }

    async fn do_promql_query(
        &self,
        query: &PromQuery,
        query_ctx: QueryContextRef,
    ) -> Vec<server_error::Result<Output>> {
        self.do_promql_query_inner(query, query_ctx)
            .await
            .into_iter()
            .map(|result| result.map_err(BoxedError::new).context(ExecuteQuerySnafu))
            .collect()
    }

    async fn do_describe(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> server_error::Result<Option<DescribeResult>> {
        self.do_describe_inner(stmt, query_ctx)
            .await
            .map_err(BoxedError::new)
            .context(server_error::DescribeStatementSnafu)
    }

    async fn is_valid_schema(&self, catalog: &str, schema: &str) -> server_error::Result<bool> {
        self.is_valid_schema_inner(catalog, schema)
            .await
            .map_err(BoxedError::new)
            .context(server_error::CheckDatabaseValiditySnafu)
    }
}

/// Expands scan-time dictionaries only when a query result leaves the frontend.
pub(crate) fn map_query_output(output: Output) -> Result<Output> {
    output
        .map_dictionary_to_values()
        .context(CollectRecordbatchSnafu)
}

/// Attaches a timer to the output and observes it once the output is exhausted.
pub fn attach_timer(output: Output, timer: HistogramTimer) -> Output {
    match output.data {
        OutputData::AffectedRows(_) | OutputData::RecordBatches(_) => output,
        OutputData::Stream(stream) => {
            let stream = OnDone::new(stream, move || {
                timer.observe_duration();
            });
            Output::new(OutputData::Stream(Box::pin(stream)), output.meta)
        }
    }
}

impl Instance {
    fn check_prom_query_privilege(&self, query_ctx: &QueryContextRef) -> server_error::Result<()> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(query_ctx.current_user(), PermissionReq::PromQuery)
            .context(AuthSnafu)?;
        Ok(())
    }

    fn prom_expr_permission_targets(
        &self,
        expr: &promql_parser::parser::Expr,
        query_ctx: &QueryContextRef,
    ) -> server_error::Result<Option<Vec<PermissionTableTarget>>> {
        extract_tables_from_prom_expr_checked(expr)
            .map(|tables| {
                tables
                    .map(|name| {
                        table_idents_to_full_name(&name, query_ctx).map(
                            |(catalog, schema, table)| {
                                PermissionTableTarget::new(catalog, schema, table)
                            },
                        )
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(BoxedError::new)
                    .context(ExecuteQuerySnafu)
            })
            .transpose()
    }

    async fn is_physical_query_permission_target(
        &self,
        target: &PermissionTableTarget,
        query_ctx: &QueryContextRef,
    ) -> server_error::Result<bool> {
        self.catalog_manager
            .table(
                &target.catalog,
                &target.schema,
                &target.table,
                Some(query_ctx),
            )
            .await
            .map(|table| table.is_some_and(|table| table.table_info().is_physical_table()))
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)
    }

    async fn resolve_query_permission_targets(
        &self,
        targets: PermissionTableTargets,
        query_ctx: &QueryContextRef,
    ) -> server_error::Result<PermissionTableTargets> {
        const CONCURRENCY: usize = 8;

        let checker = self.plugins.get::<PermissionCheckerRef>();
        if !checker.as_ref().uses_table_targets() {
            return Ok(targets);
        }

        let PermissionTableTargets::Resolved(mut targets) = targets else {
            return Ok(PermissionTableTargets::Unresolved);
        };
        if targets.len() > 1 {
            let mut seen = HashSet::with_capacity(targets.len());
            targets.retain(|target| seen.insert(target.clone()));
        }
        if let [target] = targets.as_slice() {
            return if self
                .is_physical_query_permission_target(target, query_ctx)
                .await?
            {
                Ok(PermissionTableTargets::Unresolved)
            } else {
                Ok(PermissionTableTargets::resolved(targets))
            };
        }

        // Bound catalog load and inspect results in target order to preserve serial semantics.
        for chunk in targets.chunks(CONCURRENCY) {
            let results = future::join_all(
                chunk
                    .iter()
                    .map(|target| self.is_physical_query_permission_target(target, query_ctx)),
            )
            .await;
            for result in results {
                if result? {
                    return Ok(PermissionTableTargets::Unresolved);
                }
            }
        }

        Ok(PermissionTableTargets::resolved(targets))
    }

    fn prom_queries_permission_targets(
        &self,
        queries: &[ParsedPromQuery],
        query_ctx: &QueryContextRef,
    ) -> server_error::Result<PermissionTableTargets> {
        let mut targets = Vec::new();
        let mut resolved = true;

        for query in queries {
            let QueryStatement::Promql(eval_stmt, _) = query.statement() else {
                unreachable!("query is parsed from promql");
            };

            if let Some(query_targets) =
                self.prom_expr_permission_targets(&eval_stmt.expr, query_ctx)?
            {
                targets.extend(query_targets);
            } else {
                resolved = false;
            }
        }

        Ok(if resolved {
            PermissionTableTargets::resolved(targets)
        } else {
            PermissionTableTargets::Unresolved
        })
    }
}

#[async_trait]
impl PrometheusHandler for Instance {
    #[tracing::instrument(skip_all)]
    async fn do_query(
        &self,
        query: &PromQuery,
        query_ctx: QueryContextRef,
    ) -> server_error::Result<Output> {
        let query = ParsedPromQuery::parse(query.clone(), &query_ctx)?;
        self.do_query_parsed(query, query_ctx).await
    }

    #[tracing::instrument(skip_all)]
    async fn do_query_parsed(
        &self,
        query: ParsedPromQuery,
        query_ctx: QueryContextRef,
    ) -> server_error::Result<Output> {
        let interceptor = self
            .plugins
            .get::<PromQueryInterceptorRef<server_error::Error>>();

        self.check_prom_query_privilege(&query_ctx)?;

        let targets =
            self.prom_queries_permission_targets(std::slice::from_ref(&query), &query_ctx)?;
        self.check_query_target_permission(targets, &query_ctx)
            .await?;

        let (query, stmt) = query.into_parts();

        let QueryStatement::Promql(eval_stmt, _) = &stmt else {
            unreachable!("query is parsed from promql");
        };

        let plan = self
            .statement_executor
            .plan(&stmt, query_ctx.clone())
            .await
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;

        interceptor.pre_execute(&query, &eval_stmt.expr, Some(&plan), query_ctx.clone())?;

        // Take the EvalStmt from the original QueryStatement and use it to create the CatalogQueryStatement.
        let query_statement = if let QueryStatement::Promql(eval_stmt, alias) = stmt {
            CatalogQueryStatement::Promql(eval_stmt, alias)
        } else {
            // It should not happen since the query is already parsed successfully.
            return UnexpectedResultSnafu {
                reason: "The query should always be promql.".to_string(),
            }
            .fail();
        };
        let raw_query = query_statement.to_string();

        let slow_query_timer = self
            .slow_query_options
            .enable
            .then(|| self.event_recorder.clone())
            .flatten()
            .map(|event_recorder| {
                SlowQueryTimer::new(
                    query_statement,
                    query_ctx.current_schema(),
                    self.slow_query_options.threshold,
                    self.slow_query_options.sample_ratio,
                    self.slow_query_options.record_type,
                    event_recorder,
                )
            });

        let ticket = self.process_manager.register_query(
            query_ctx.current_catalog().to_string(),
            vec![query_ctx.current_schema()],
            raw_query,
            query_ctx.conn_info().to_string(),
            Some(query_ctx.process_id()),
            slow_query_timer,
        );

        let query_fut = self.statement_executor.exec_plan(plan, query_ctx.clone());

        let output = CancellableFuture::new(query_fut, ticket.cancellation_handle.clone())
            .await
            .map_err(|_| servers::error::CancelledSnafu.build())?
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;
        let output = map_query_output(output)
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;
        let Output { meta, data } = output;
        let data = match data {
            OutputData::Stream(stream) => {
                OutputData::Stream(Box::pin(CancellableStreamWrapper::new(stream, ticket)))
            }
            other => other,
        };
        let output = Output { data, meta };
        Ok(interceptor.post_execute(output, query_ctx)?)
    }

    async fn check_query_permission(
        &self,
        queries: &[PromQuery],
        query_ctx: &QueryContextRef,
    ) -> server_error::Result<()> {
        let queries = queries
            .iter()
            .cloned()
            .map(|query| ParsedPromQuery::parse(query, query_ctx))
            .collect::<server_error::Result<Vec<_>>>()?;
        self.check_query_permission_parsed(&queries, query_ctx)
            .await
    }

    async fn check_query_permission_parsed(
        &self,
        queries: &[ParsedPromQuery],
        query_ctx: &QueryContextRef,
    ) -> server_error::Result<()> {
        self.check_prom_query_privilege(query_ctx)?;
        let targets = self.prom_queries_permission_targets(queries, query_ctx)?;
        self.check_query_target_permission(targets, query_ctx).await
    }

    async fn check_query_target_permission(
        &self,
        targets: PermissionTableTargets,
        query_ctx: &QueryContextRef,
    ) -> server_error::Result<()> {
        let targets = self
            .resolve_query_permission_targets(targets, query_ctx)
            .await?;
        self.check_table_permission(query_ctx, PermissionReq::PromQuery, targets)
            .context(AuthSnafu)?;
        Ok(())
    }

    async fn filter_metadata_metric_names(
        &self,
        metric_names: Vec<String>,
        schema: &str,
        query_ctx: &QueryContextRef,
    ) -> server_error::Result<Vec<String>> {
        let checker = self.plugins.get::<PermissionCheckerRef>();
        if !checker.as_ref().uses_table_targets() {
            let Some(metric) = metric_names.first() else {
                return Ok(metric_names);
            };
            let target =
                PermissionTableTarget::new(query_ctx.current_catalog(), schema, metric.as_str());
            let result = checker
                .as_ref()
                .check_permission_with_table_targets(
                    query_ctx.current_user(),
                    PermissionReq::PromQuery,
                    PermissionTableTargets::resolved(vec![target]),
                )
                .context(AuthSnafu);
            return match result {
                Ok(_) => Ok(metric_names),
                Err(error)
                    if error.status_code()
                        == common_error::status_code::StatusCode::PermissionDenied =>
                {
                    Ok(Vec::new())
                }
                Err(error) => Err(error),
            };
        }

        let mut allowed = Vec::with_capacity(metric_names.len());
        for metric in metric_names {
            let target =
                PermissionTableTarget::new(query_ctx.current_catalog(), schema, metric.as_str());
            match checker
                .as_ref()
                .check_permission_with_table_targets(
                    query_ctx.current_user(),
                    PermissionReq::PromQuery,
                    PermissionTableTargets::resolved(vec![target]),
                )
                .context(AuthSnafu)
            {
                Ok(_) => allowed.push(metric),
                Err(error)
                    if error.status_code()
                        == common_error::status_code::StatusCode::PermissionDenied => {}
                Err(error) => return Err(error),
            }
        }
        Ok(allowed)
    }

    async fn query_metric_names(
        &self,
        matchers: Vec<Matcher>,
        schema: &str,
        ctx: &QueryContextRef,
    ) -> server_error::Result<Vec<String>> {
        self.handle_query_metric_names(matchers, schema, ctx)
            .await
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)
    }

    async fn query_label_values(
        &self,
        metric: String,
        label_name: String,
        matchers: Vec<Matcher>,
        start: SystemTime,
        end: SystemTime,
        ctx: &QueryContextRef,
    ) -> server_error::Result<Vec<String>> {
        let schema =
            resolve_schema_from_matchers(&matchers)?.unwrap_or_else(|| ctx.current_schema());
        let target = PermissionTableTarget::new(ctx.current_catalog(), schema.as_str(), &metric);
        self.check_query_target_permission(
            PermissionTableTargets::resolved(vec![target.clone()]),
            ctx,
        )
        .await?;

        self.handle_query_label_values(target, label_name, matchers, start, end, ctx)
            .await
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)
    }

    fn catalog_manager(&self) -> CatalogManagerRef {
        self.catalog_manager.clone()
    }
}

/// Validate `stmt.database` permission if it's presented.
macro_rules! validate_db_permission {
    ($stmt: expr, $query_ctx: expr) => {
        if let Some(database) = &$stmt.database {
            validate_catalog_and_schema($query_ctx.current_catalog(), database, $query_ctx)
                .map_err(BoxedError::new)
                .context(SqlExecInterceptedSnafu)?;
        }
    };
}

pub fn check_permission(
    plugins: Plugins,
    stmt: &Statement,
    query_ctx: &QueryContextRef,
) -> Result<()> {
    let need_validate = plugins
        .get::<QueryOptions>()
        .map(|opts| opts.disallow_cross_catalog_query)
        .unwrap_or_default();

    if !need_validate {
        return Ok(());
    }

    match stmt {
        // Will be checked in execution.
        // TODO(dennis): add a hook for admin commands.
        Statement::Admin(_) => {}
        // These are executed by query engine, and will be checked there.
        Statement::Query(_)
        | Statement::Explain(_)
        | Statement::Tql(_)
        | Statement::Delete(_)
        | Statement::DeclareCursor(_)
        | Statement::Copy(sql::statements::copy::Copy::CopyQueryTo(_)) => {}
        // database ops won't be checked
        Statement::CreateDatabase(_)
        | Statement::ShowDatabases(_)
        | Statement::DropDatabase(_)
        | Statement::AlterDatabase(_)
        | Statement::DropFlow(_)
        | Statement::Use(_) => {}
        #[cfg(feature = "enterprise")]
        Statement::DropTrigger(_) => {}
        Statement::ShowCreateDatabase(stmt) => {
            validate_database(&stmt.database_name, query_ctx)?;
        }
        Statement::ShowCreateTable(stmt) => {
            validate_param(&stmt.table_name, query_ctx)?;
        }
        Statement::ShowCreateFlow(stmt) => {
            validate_flow(&stmt.flow_name, query_ctx)?;
        }
        #[cfg(feature = "enterprise")]
        Statement::ShowCreateTrigger(stmt) => {
            validate_param(&stmt.trigger_name, query_ctx)?;
        }
        Statement::ShowCreateView(stmt) => {
            validate_param(&stmt.view_name, query_ctx)?;
        }
        Statement::CreateExternalTable(stmt) => {
            validate_param(&stmt.name, query_ctx)?;
        }
        Statement::CreateFlow(stmt) => {
            // TODO: should also validate source table name here?
            validate_param(&stmt.sink_table_name, query_ctx)?;
        }
        #[cfg(feature = "enterprise")]
        Statement::CreateTrigger(stmt) => {
            validate_param(&stmt.trigger_name, query_ctx)?;
        }
        Statement::CreateView(stmt) => {
            validate_param(&stmt.name, query_ctx)?;
        }
        Statement::AlterTable(stmt) => {
            validate_param(stmt.table_name(), query_ctx)?;
        }
        #[cfg(feature = "enterprise")]
        Statement::AlterTrigger(_) => {}
        // set/show variable now only alter/show variable in session
        Statement::SetVariables(_) | Statement::ShowVariables(_) => {}
        // show charset and show collation won't be checked
        Statement::ShowCharset(_) | Statement::ShowCollation(_) => {}

        Statement::Comment(comment) => match &comment.object {
            CommentObject::Table(table) => validate_param(table, query_ctx)?,
            CommentObject::Column { table, .. } => validate_param(table, query_ctx)?,
            CommentObject::Flow(flow) => validate_flow(flow, query_ctx)?,
        },

        Statement::Insert(insert) => {
            let name = insert.table_name().context(ParseSqlSnafu)?;
            validate_param(name, query_ctx)?;
        }
        Statement::CreateTable(stmt) => {
            validate_param(&stmt.name, query_ctx)?;
        }
        Statement::CreateTableLike(stmt) => {
            validate_param(&stmt.table_name, query_ctx)?;
            validate_param(&stmt.source_name, query_ctx)?;
        }
        Statement::DropTable(drop_stmt) => {
            for table_name in drop_stmt.table_names() {
                validate_param(table_name, query_ctx)?;
            }
        }
        Statement::UndropTable(stmt) => {
            validate_param(stmt.table_name(), query_ctx)?;
        }
        Statement::DropView(stmt) => {
            validate_param(&stmt.view_name, query_ctx)?;
        }
        Statement::ShowTables(stmt) => {
            validate_db_permission!(stmt, query_ctx);
        }
        Statement::ShowTableStatus(stmt) => {
            validate_db_permission!(stmt, query_ctx);
        }
        Statement::ShowColumns(stmt) => {
            validate_db_permission!(stmt, query_ctx);
        }
        Statement::ShowIndex(stmt) => {
            validate_db_permission!(stmt, query_ctx);
        }
        Statement::ShowRegion(stmt) => {
            validate_db_permission!(stmt, query_ctx);
        }
        Statement::ShowViews(stmt) => {
            validate_db_permission!(stmt, query_ctx);
        }
        Statement::ShowFlows(stmt) => {
            validate_db_permission!(stmt, query_ctx);
        }
        Statement::ShowFlowStatus(_stmt) => {
            // Flow statistics are organized based on the catalog dimension and
            // filtered by the current catalog, so there is no need to check the
            // permission of the database(schema).
        }
        #[cfg(feature = "enterprise")]
        Statement::ShowTriggers(_stmt) => {
            // The trigger is organized based on the catalog dimension, so there
            // is no need to check the permission of the database(schema).
        }
        Statement::ShowStatus(_stmt) => {}
        Statement::ShowSearchPath(_stmt) => {}
        Statement::DescribeTable(stmt) => {
            validate_param(stmt.name(), query_ctx)?;
        }
        Statement::Copy(sql::statements::copy::Copy::CopyTable(stmt)) => match stmt {
            CopyTable::To(copy_table_to) => validate_param(&copy_table_to.table_name, query_ctx)?,
            CopyTable::From(copy_table_from) => {
                validate_param(&copy_table_from.table_name, query_ctx)?
            }
        },
        Statement::Copy(sql::statements::copy::Copy::CopyDatabase(copy_database)) => {
            match copy_database {
                CopyDatabase::To(stmt) => validate_database(&stmt.database_name, query_ctx)?,
                CopyDatabase::From(stmt) => validate_database(&stmt.database_name, query_ctx)?,
            }
        }
        Statement::TruncateTable(stmt) => {
            validate_param(stmt.table_name(), query_ctx)?;
        }
        // cursor operations are always allowed once it's created
        Statement::FetchCursor(_) | Statement::CloseCursor(_) => {}
        // User can only kill process in their own catalog.
        Statement::Kill(_) => {}
        // SHOW PROCESSLIST
        Statement::ShowProcesslist(_) => {}
    }
    Ok(())
}

fn validate_param(name: &ObjectName, query_ctx: &QueryContextRef) -> Result<()> {
    let (catalog, schema, _) = table_idents_to_full_name(name, query_ctx)
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;

    validate_catalog_and_schema(&catalog, &schema, query_ctx)
        .map_err(BoxedError::new)
        .context(SqlExecInterceptedSnafu)
}

fn validate_flow(name: &ObjectName, query_ctx: &QueryContextRef) -> Result<()> {
    let catalog = match &name.0[..] {
        [_flow] => query_ctx.current_catalog().to_string(),
        [catalog, _flow] => catalog.to_string_unquoted(),
        _ => {
            return InvalidSqlSnafu {
                err_msg: format!(
                    "expect flow name to be <catalog>.<flow_name> or <flow_name>, actual: {name}",
                ),
            }
            .fail();
        }
    };

    let schema = query_ctx.current_schema();

    validate_catalog_and_schema(&catalog, &schema, query_ctx)
        .map_err(BoxedError::new)
        .context(SqlExecInterceptedSnafu)
}

fn validate_database(name: &ObjectName, query_ctx: &QueryContextRef) -> Result<()> {
    let (catalog, schema) = match &name.0[..] {
        [schema] => (
            query_ctx.current_catalog().to_string(),
            schema.to_string_unquoted(),
        ),
        [catalog, schema] => (catalog.to_string_unquoted(), schema.to_string_unquoted()),
        _ => InvalidSqlSnafu {
            err_msg: format!(
                "expect database name to be <catalog>.<schema> or <schema>, actual: {name}",
            ),
        }
        .fail()?,
    };

    validate_catalog_and_schema(&catalog, &schema, query_ctx)
        .map_err(BoxedError::new)
        .context(SqlExecInterceptedSnafu)
}

fn is_readonly_plan(plan: &LogicalPlan) -> bool {
    !matches!(plan, LogicalPlan::Dml(_) | LogicalPlan::Ddl(_))
}

fn should_track_statement_process(stmt: &Statement) -> bool {
    stmt.is_readonly()
        || matches!(stmt, Statement::Insert(insert) if insert.has_non_values_query_source())
}

fn should_track_plan_process(stmt: Option<&Statement>, plan: &LogicalPlan) -> bool {
    is_readonly_plan(plan)
        || matches!(stmt, Some(Statement::Insert(insert)) if insert.has_non_values_query_source())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use api::prom_store::remote::label_matcher::Type as PromMatcherType;
    use api::prom_store::remote::{LabelMatcher, Query as RemoteQuery, ReadRequest};
    use api::v1::meta::{ProcedureDetailResponse, ReconcileRequest, ReconcileResponse};
    use auth::{PermissionResp, UserInfoRef};
    use catalog::process_manager::ProcessManager;
    use common_base::Plugins;
    use common_error::ext::{BoxedError, PlainError};
    use common_error::status_code::StatusCode;
    use common_meta::cache::LayeredCacheRegistryBuilder;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::procedure_executor::{ExecutorContext, ProcedureExecutor};
    use common_meta::rpc::ddl::{SubmitDdlTaskRequest, SubmitDdlTaskResponse};
    use common_meta::rpc::procedure::{
        MigrateRegionRequest, MigrateRegionResponse, ProcedureStateResponse,
    };
    use common_query::Output;
    use common_recordbatch::{
        OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_expr::dml::InsertOp;
    use datafusion_expr::{LogicalPlanBuilder, LogicalTableSource};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema as GtSchema, SchemaRef as GtSchemaRef};
    use datatypes::vectors::{StringVector, VectorRef};
    use log_query::LogQuery;
    use query::query_engine::options::QueryOptions;
    use servers::query_handler::{LogQueryHandler, PromStoreProtocolHandler};
    use session::context::{Channel, ConnInfo, QueryContext, QueryContextBuilder};
    use snafu::{Location, Snafu};
    use sql::dialect::GreptimeDbDialect;
    use store_api::data_source::DataSource;
    use store_api::metric_engine_consts::{
        LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY,
    };
    use store_api::storage::ScanRequest;
    use strfmt::Format;
    use table::metadata::{FilterPushDownType, TableInfo, TableInfoBuilder, TableMetaBuilder};
    use table::table_name::TableName;
    use table::test_util::{EmptyTable, MemTable};
    use table::{Table, TableRef};
    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::frontend::FrontendOptions;
    use crate::instance::builder::FrontendBuilder;

    fn parse_test_sql(sql: &str) -> Vec<Statement> {
        parse_stmt(sql, &GreptimeDbDialect {}).unwrap()
    }

    #[test]
    fn test_validate_analyze_stream_statement_strictness() {
        for sql in [
            "select 1",
            "explain analyze select 1",
            "explain analyze verbose format text select 1",
            "explain analyze verbose format graphviz select 1",
        ] {
            let mut stmts = parse_test_sql(sql);
            assert!(
                validate_analyze_stream_statement(&mut stmts[0]).is_err(),
                "{sql}"
            );
        }

        for sql in [
            "explain analyze verbose select 1",
            "explain analyze verbose format json select 1",
        ] {
            let mut stmts = parse_test_sql(sql);
            assert!(
                validate_analyze_stream_statement(&mut stmts[0]).is_ok(),
                "{sql}"
            );
            let Statement::Explain(explain) = &stmts[0] else {
                unreachable!();
            };
            assert!(explain.format.is_none());
        }

        assert_eq!(
            parse_test_sql("explain analyze verbose select 1; select 2").len(),
            2
        );
    }

    #[derive(Debug, Snafu)]
    enum TestError {
        #[snafu(display("Failed to build test cache registry"))]
        BuildCacheRegistry {
            source: cache::error::Error,
            #[snafu(implicit)]
            location: Location,
        },

        #[snafu(display("Failed to build test table meta for table: {table_name}"))]
        BuildTableMeta {
            table_name: String,
            source: table::metadata::TableMetaBuilderError,
            #[snafu(implicit)]
            location: Location,
        },

        #[snafu(display("Failed to build test table info for table: {table_name}"))]
        BuildTableInfo {
            table_name: String,
            source: table::metadata::TableInfoBuilderError,
            #[snafu(implicit)]
            location: Location,
        },

        #[snafu(display("Failed to register test table: {table_name}"))]
        RegisterTable {
            table_name: String,
            source: catalog::error::Error,
            #[snafu(implicit)]
            location: Location,
        },

        #[snafu(display("Failed to build test frontend instance"))]
        BuildFrontend {
            source: crate::error::Error,
            #[snafu(implicit)]
            location: Location,
        },

        #[snafu(display("Expected exactly one output for SQL `{sql}`, got {actual}"))]
        UnexpectedOutputCount {
            sql: String,
            actual: usize,
            #[snafu(implicit)]
            location: Location,
        },

        #[snafu(display("Failed to execute SQL `{sql}`"))]
        ExecuteSql {
            sql: String,
            source: crate::error::Error,
            #[snafu(implicit)]
            location: Location,
        },

        #[snafu(display("Timed out waiting for insert-select start notification"))]
        InsertStartTimeout {
            source: tokio::time::error::Elapsed,
            #[snafu(implicit)]
            location: Location,
        },

        #[snafu(display("Insert-select start notification channel closed"))]
        InsertStartChannelClosed {
            #[snafu(implicit)]
            location: Location,
        },

        #[snafu(display("Failed to release blocking insert-select interceptor"))]
        ReleaseBlockedInsert {
            #[snafu(implicit)]
            location: Location,
        },

        #[snafu(display("Timed out waiting for insert-select source to be polled"))]
        SourcePollTimeout {
            source: tokio::time::error::Elapsed,
            #[snafu(implicit)]
            location: Location,
        },

        #[snafu(display("Insert-select source poll notification channel closed"))]
        SourcePollChannelClosed {
            source: oneshot::error::RecvError,
            #[snafu(implicit)]
            location: Location,
        },

        #[snafu(display("Timed out waiting for insert task to finish"))]
        InsertTaskTimeout {
            source: tokio::time::error::Elapsed,
            #[snafu(implicit)]
            location: Location,
        },

        #[snafu(display("Insert task panicked"))]
        InsertTaskPanic {
            source: tokio::task::JoinError,
            #[snafu(implicit)]
            location: Location,
        },

        #[snafu(display("Expected insert-select to be cancelled"))]
        InsertSelectNotCancelled {
            #[snafu(implicit)]
            location: Location,
        },
    }

    type TestResult<T> = std::result::Result<T, TestError>;

    fn parse_one_sql(sql: &str) -> Statement {
        parse_stmt(sql, &GreptimeDbDialect {}).unwrap().remove(0)
    }

    fn test_query_ctx(process_id: u32) -> QueryContextRef {
        Arc::new(
            QueryContextBuilder::default()
                .channel(Channel::Mysql)
                .conn_info(ConnInfo::new(None, Channel::Mysql))
                .process_id(process_id)
                .build(),
        )
    }

    struct RejectUnresolvedPermissionChecker;

    impl PermissionChecker for RejectUnresolvedPermissionChecker {
        fn check_permission(
            &self,
            _user_info: UserInfoRef,
            _req: PermissionReq,
        ) -> auth::error::Result<PermissionResp> {
            Ok(PermissionResp::Allow)
        }

        fn check_permission_with_table_targets(
            &self,
            _user_info: UserInfoRef,
            _req: PermissionReq,
            targets: PermissionTableTargets,
        ) -> auth::error::Result<PermissionResp> {
            let reject = match targets {
                PermissionTableTargets::Unresolved => true,
                PermissionTableTargets::Resolved(targets) => {
                    targets.iter().any(|target| target.table == "denied")
                }
            };
            Ok(if reject {
                PermissionResp::Reject
            } else {
                PermissionResp::Allow
            })
        }
    }

    #[derive(Default)]
    struct TargetIndependentPermissionChecker {
        checks: atomic::AtomicUsize,
    }

    impl PermissionChecker for TargetIndependentPermissionChecker {
        fn check_permission(
            &self,
            _user_info: UserInfoRef,
            _req: PermissionReq,
        ) -> auth::error::Result<PermissionResp> {
            self.checks.fetch_add(1, atomic::Ordering::Relaxed);
            Ok(PermissionResp::Allow)
        }

        fn uses_table_targets(&self) -> bool {
            false
        }

        fn check_permission_with_table_targets(
            &self,
            user_info: UserInfoRef,
            req: PermissionReq,
            _targets: PermissionTableTargets,
        ) -> auth::error::Result<PermissionResp> {
            self.check_permission(user_info, req)
        }
    }

    struct BlockingInsertSelectInterceptor {
        started_tx: mpsc::UnboundedSender<()>,
        finish_rx: std::sync::Mutex<Option<oneshot::Receiver<()>>>,
    }

    impl BlockingInsertSelectInterceptor {
        fn new(started_tx: mpsc::UnboundedSender<()>, finish_rx: oneshot::Receiver<()>) -> Self {
            Self {
                started_tx,
                finish_rx: std::sync::Mutex::new(Some(finish_rx)),
            }
        }
    }

    impl SqlQueryInterceptor for BlockingInsertSelectInterceptor {
        type Error = Error;

        fn pre_execute(
            &self,
            statement: Option<&Statement>,
            _plan: Option<&LogicalPlan>,
            _query_ctx: QueryContextRef,
        ) -> Result<()> {
            let Some(Statement::Insert(insert)) = statement else {
                return Ok(());
            };
            if !insert.has_non_values_query_source() {
                return Ok(());
            }

            let finish_rx = self.finish_rx.lock().unwrap().take().unwrap();
            let _ = self.started_tx.send(());
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(finish_rx)
                    .unwrap();
            });
            Ok(())
        }
    }

    struct PendingRecordBatchStream {
        schema: GtSchemaRef,
        polled_tx: Option<oneshot::Sender<()>>,
        _finish_tx: oneshot::Sender<()>,
        finish_rx: Pin<Box<oneshot::Receiver<()>>>,
    }

    impl RecordBatchStream for PendingRecordBatchStream {
        fn schema(&self) -> GtSchemaRef {
            self.schema.clone()
        }

        fn output_ordering(&self) -> Option<&[OrderOption]> {
            None
        }

        fn metrics(&self) -> Option<common_recordbatch::adapter::RecordBatchMetrics> {
            None
        }
    }

    impl Stream for PendingRecordBatchStream {
        type Item = common_recordbatch::error::Result<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Some(polled_tx) = self.polled_tx.take() {
                let _ = polled_tx.send(());
            }

            match self.finish_rx.as_mut().poll(cx) {
                Poll::Ready(_) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl Unpin for PendingRecordBatchStream {}

    struct PendingDataSource {
        schema: GtSchemaRef,
        polled_tx: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    }

    impl DataSource for PendingDataSource {
        fn get_stream(
            &self,
            _request: ScanRequest,
        ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
            let (finish_tx, finish_rx) = oneshot::channel();
            let mut polled_tx = self.polled_tx.lock().map_err(|_| {
                BoxedError::new(PlainError::new(
                    "pending data source lock poisoned".to_string(),
                    StatusCode::Unexpected,
                ))
            })?;
            Ok(Box::pin(PendingRecordBatchStream {
                schema: self.schema.clone(),
                polled_tx: polled_tx.take(),
                _finish_tx: finish_tx,
                finish_rx: Box::pin(finish_rx),
            }))
        }
    }

    struct NoopProcedureExecutor;

    #[async_trait::async_trait]
    impl ProcedureExecutor for NoopProcedureExecutor {
        async fn submit_ddl_task(
            &self,
            _ctx: &ExecutorContext,
            _request: SubmitDdlTaskRequest,
        ) -> common_meta::error::Result<SubmitDdlTaskResponse> {
            common_meta::error::UnsupportedSnafu {
                operation: "submit_ddl_task",
            }
            .fail()
        }

        async fn migrate_region(
            &self,
            _ctx: &ExecutorContext,
            _request: MigrateRegionRequest,
        ) -> common_meta::error::Result<MigrateRegionResponse> {
            common_meta::error::UnsupportedSnafu {
                operation: "migrate_region",
            }
            .fail()
        }

        async fn reconcile(
            &self,
            _ctx: &ExecutorContext,
            _request: ReconcileRequest,
        ) -> common_meta::error::Result<ReconcileResponse> {
            common_meta::error::UnsupportedSnafu {
                operation: "reconcile",
            }
            .fail()
        }

        async fn query_procedure_state(
            &self,
            _ctx: &ExecutorContext,
            _pid: &str,
        ) -> common_meta::error::Result<ProcedureStateResponse> {
            common_meta::error::UnsupportedSnafu {
                operation: "query_procedure_state",
            }
            .fail()
        }

        async fn list_procedures(
            &self,
            _ctx: &ExecutorContext,
        ) -> common_meta::error::Result<ProcedureDetailResponse> {
            common_meta::error::UnsupportedSnafu {
                operation: "list_procedures",
            }
            .fail()
        }
    }

    fn test_cache_registry(
        kv_backend: common_meta::kv_backend::KvBackendRef,
    ) -> TestResult<common_meta::cache::LayeredCacheRegistryRef> {
        Ok(Arc::new(
            cache::with_default_composite_cache_registry(
                LayeredCacheRegistryBuilder::default()
                    .add_cache_registry(cache::build_fundamental_cache_registry(kv_backend)),
            )
            .context(BuildCacheRegistrySnafu)?
            .build(),
        ))
    }

    fn test_table_info(table_id: u32, table_name: &str) -> TestResult<TableInfo> {
        let schema = Arc::new(GtSchema::new(vec![
            ColumnSchema::new("id", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        ]));
        let table_meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![0])
            .value_indices(vec![1])
            .next_column_id(1024)
            .build()
            .with_context(|_| BuildTableMetaSnafu {
                table_name: table_name.to_string(),
            })?;

        TableInfoBuilder::new(table_name, table_meta)
            .table_id(table_id)
            .build()
            .with_context(|_| BuildTableInfoSnafu {
                table_name: table_name.to_string(),
            })
    }

    fn test_table(table_id: u32, table_name: &str) -> TestResult<table::TableRef> {
        let table_info = test_table_info(table_id, table_name)?;
        Ok(EmptyTable::from_table_info(&table_info))
    }

    fn test_physical_table(table_id: u32, table_name: &str) -> TestResult<table::TableRef> {
        let mut table_info = test_table_info(table_id, table_name)?;
        table_info
            .meta
            .options
            .extra_options
            .insert(PHYSICAL_TABLE_METADATA_KEY.to_string(), String::new());
        Ok(EmptyTable::from_table_info(&table_info))
    }

    fn test_logical_table(table_id: u32, table_name: &str) -> TestResult<table::TableRef> {
        let mut table_info = test_table_info(table_id, table_name)?;
        table_info.meta.engine = METRIC_ENGINE_NAME.to_string();
        table_info.meta.options.extra_options.insert(
            LOGICAL_TABLE_METADATA_KEY.to_string(),
            "physical_metric".to_string(),
        );
        Ok(EmptyTable::from_table_info(&table_info))
    }

    fn test_metric_names_table() -> TableRef {
        let schema = Arc::new(GtSchema::new(vec![
            ColumnSchema::new("table_catalog", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("table_schema", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("table_name", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("engine", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("create_options", ConcreteDataType::string_datatype(), false),
        ]));
        let columns: Vec<VectorRef> = vec![
            Arc::new(StringVector::from(vec!["greptime", "greptime"])),
            Arc::new(StringVector::from(vec!["public", "public"])),
            Arc::new(StringVector::from(vec!["denied", "target"])),
            Arc::new(StringVector::from(vec!["metric", "metric"])),
            Arc::new(StringVector::from(vec![
                "on_physical_table=physical_metric",
                "on_physical_table=physical_metric",
            ])),
        ];
        let record_batch = RecordBatch::new(schema, columns).unwrap();
        MemTable::new_with_catalog(
            "tables",
            record_batch,
            2048,
            "greptime".to_string(),
            "information_schema".to_string(),
        )
    }

    fn pending_table(
        table_id: u32,
        table_name: &str,
        polled_tx: oneshot::Sender<()>,
    ) -> TestResult<table::TableRef> {
        let table_info = test_table_info(table_id, table_name)?;
        let data_source = Arc::new(PendingDataSource {
            schema: table_info.meta.schema.clone(),
            polled_tx: std::sync::Mutex::new(Some(polled_tx)),
        });

        Ok(Arc::new(Table::new(
            Arc::new(table_info),
            FilterPushDownType::Unsupported,
            data_source,
        )))
    }

    async fn test_instance_with_tables(
        source_table: TableRef,
        target_table: TableRef,
    ) -> TestResult<Instance> {
        test_instance_with_plugins(source_table, target_table, Plugins::new()).await
    }

    async fn test_instance_with_insert_select_interceptor(
        interceptor: SqlQueryInterceptorRef<Error>,
    ) -> TestResult<Instance> {
        let plugins = Plugins::new();
        plugins.insert::<SqlQueryInterceptorRef<Error>>(interceptor);

        test_instance_with_plugins(
            test_table(1024, "source")?,
            test_table(1025, "target")?,
            plugins,
        )
        .await
    }

    async fn test_instance_with_plugins(
        source_table: TableRef,
        target_table: TableRef,
        plugins: Plugins,
    ) -> TestResult<Instance> {
        test_instance_with_plugins_and_metric_names(source_table, target_table, plugins, None).await
    }

    async fn test_instance_with_plugins_and_metric_names(
        source_table: TableRef,
        target_table: TableRef,
        plugins: Plugins,
        metric_names_table: Option<TableRef>,
    ) -> TestResult<Instance> {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let process_manager = Arc::new(ProcessManager::new("test-frontend".to_string(), None));
        let catalog_manager = catalog::memory::MemoryCatalogManager::new_with_table(source_table);
        let target_table_name = "target";
        catalog_manager
            .register_table_sync(catalog::RegisterTableRequest {
                catalog: "greptime".to_string(),
                schema: "public".to_string(),
                table_name: target_table_name.to_string(),
                table_id: 1025,
                table: target_table,
            })
            .with_context(|_| RegisterTableSnafu {
                table_name: target_table_name.to_string(),
            })?;
        if let Some(table) = metric_names_table {
            catalog_manager
                .deregister_table_sync(catalog::DeregisterTableRequest {
                    catalog: "greptime".to_string(),
                    schema: "information_schema".to_string(),
                    table_name: "tables".to_string(),
                })
                .unwrap();
            catalog_manager
                .register_table_sync(catalog::RegisterTableRequest {
                    catalog: "greptime".to_string(),
                    schema: "information_schema".to_string(),
                    table_name: "tables".to_string(),
                    table_id: 2048,
                    table,
                })
                .unwrap();
        }
        catalog_manager.register_process_list_table(process_manager.clone());

        let cache_registry = test_cache_registry(kv_backend.clone())?;

        FrontendBuilder::new(
            FrontendOptions::default(),
            kv_backend,
            cache_registry,
            catalog_manager,
            Arc::new(client::client_manager::NodeClients::default()),
            Arc::new(NoopProcedureExecutor),
            process_manager,
        )
        .with_plugin(plugins)
        .try_build()
        .await
        .context(BuildFrontendSnafu)
    }

    async fn execute_one_sql(
        instance: &Instance,
        sql: &str,
        query_ctx: QueryContextRef,
    ) -> TestResult<Output> {
        let mut results = instance.do_query_inner(sql, query_ctx).await;
        ensure!(
            results.len() == 1,
            UnexpectedOutputCountSnafu {
                sql: sql.to_string(),
                actual: results.len(),
            }
        );
        results.remove(0).with_context(|_| ExecuteSqlSnafu {
            sql: sql.to_string(),
        })
    }

    #[tokio::test]
    async fn test_target_independent_checker_skips_target_resolution() -> TestResult<()> {
        let physical_table = "physical_metric";
        let checker = Arc::new(TargetIndependentPermissionChecker::default());
        let plugins = Plugins::new();
        plugins.insert::<PermissionCheckerRef>(checker.clone());
        let instance = test_instance_with_plugins(
            test_physical_table(1024, physical_table)?,
            test_table(1025, "target")?,
            plugins,
        )
        .await?;

        let ctx = test_query_ctx(1);
        let physical_target = PermissionTableTarget::new("greptime", "public", physical_table);
        assert_eq!(
            PermissionTableTargets::Resolved(vec![physical_target.clone()]),
            instance
                .resolve_query_permission_targets(
                    PermissionTableTargets::resolved(vec![physical_target]),
                    &ctx,
                )
                .await
                .unwrap()
        );
        assert_eq!(
            vec![physical_table.to_string(), "target".to_string()],
            PrometheusHandler::filter_metadata_metric_names(
                &instance,
                vec![physical_table.to_string(), "target".to_string()],
                "public",
                &ctx,
            )
            .await
            .unwrap()
        );
        assert_eq!(1, checker.checks.load(atomic::Ordering::Relaxed));

        Ok(())
    }

    #[tokio::test]
    async fn test_query_permission_targets_are_deduplicated() -> TestResult<()> {
        let plugins = Plugins::new();
        plugins.insert::<PermissionCheckerRef>(Arc::new(RejectUnresolvedPermissionChecker));
        let instance = test_instance_with_plugins(
            test_table(1024, "source")?,
            test_table(1025, "target")?,
            plugins,
        )
        .await?;
        let ctx = test_query_ctx(1);
        let target = PermissionTableTarget::new("greptime", "public", "target");

        assert_eq!(
            PermissionTableTargets::Resolved(vec![target.clone()]),
            instance
                .resolve_query_permission_targets(
                    PermissionTableTargets::resolved(vec![target.clone(), target]),
                    &ctx,
                )
                .await
                .unwrap()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_physical_query_targets_fail_closed() -> TestResult<()> {
        let physical_table = "physical_metric";
        let plugins = Plugins::new();
        plugins.insert::<PermissionCheckerRef>(Arc::new(RejectUnresolvedPermissionChecker));
        let instance = test_instance_with_plugins(
            test_physical_table(1024, physical_table)?,
            test_table(1025, "target")?,
            plugins,
        )
        .await?;

        let ctx = test_query_ctx(1);
        let logical_target = PermissionTableTarget::new("greptime", "public", "target");
        assert_eq!(
            PermissionTableTargets::Resolved(vec![logical_target.clone()]),
            instance
                .resolve_query_permission_targets(
                    PermissionTableTargets::resolved(vec![logical_target.clone()]),
                    &ctx,
                )
                .await
                .unwrap()
        );
        let physical_target = PermissionTableTarget::new("greptime", "public", physical_table);
        assert_eq!(
            PermissionTableTargets::Unresolved,
            instance
                .resolve_query_permission_targets(
                    PermissionTableTargets::resolved(
                        vec![logical_target, physical_target.clone(),]
                    ),
                    &ctx,
                )
                .await
                .unwrap()
        );
        assert_eq!(
            vec!["target".to_string()],
            PrometheusHandler::filter_metadata_metric_names(
                &instance,
                vec!["target".to_string(), "denied".to_string()],
                "public",
                &ctx,
            )
            .await
            .unwrap()
        );

        let query = PromQuery {
            query: physical_table.to_string(),
            ..Default::default()
        };
        let err = PrometheusHandler::check_query_target_permission(
            &instance,
            PermissionTableTargets::resolved(vec![physical_target]),
            &ctx,
        )
        .await
        .unwrap_err();
        assert_eq!(StatusCode::PermissionDenied, err.status_code());
        let err = PrometheusHandler::check_query_permission(
            &instance,
            std::slice::from_ref(&query),
            &ctx,
        )
        .await
        .unwrap_err();
        assert_eq!(StatusCode::PermissionDenied, err.status_code());
        let err = PrometheusHandler::do_query(&instance, &query, ctx.clone())
            .await
            .unwrap_err();
        assert_eq!(StatusCode::PermissionDenied, err.status_code());

        for sql in [
            "SELECT * FROM physical_metric",
            "TQL EVAL (0, 10, '5s') physical_metric",
            "INSERT INTO target SELECT * FROM physical_metric",
        ] {
            let mut results = instance.do_query_inner(sql, ctx.clone()).await;
            assert_eq!(1, results.len(), "{sql}");
            let err = results.remove(0).unwrap_err();
            assert_eq!(StatusCode::PermissionDenied, err.status_code(), "{sql}");
        }
        let err = LogQueryHandler::query(
            &instance,
            LogQuery {
                table: TableName::new("greptime", "public", physical_table),
                ..Default::default()
            },
            ctx.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(StatusCode::PermissionDenied, err.status_code());
        let err = instance
            .do_describe_inner(parse_one_sql("SELECT * FROM physical_metric"), ctx.clone())
            .await
            .unwrap_err();
        assert_eq!(StatusCode::PermissionDenied, err.status_code());

        let request = ReadRequest {
            queries: vec![RemoteQuery {
                matchers: vec![LabelMatcher {
                    r#type: PromMatcherType::Eq as i32,
                    name: servers::prom_store::METRIC_NAME_LABEL.to_string(),
                    value: physical_table.to_string(),
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let Err(err) = PromStoreProtocolHandler::read(&instance, request, ctx.clone()).await else {
            panic!("physical remote-read target must be rejected");
        };
        assert_eq!(StatusCode::PermissionDenied, err.status_code());

        let err = PrometheusHandler::query_label_values(
            &instance,
            physical_table.to_string(),
            "host".to_string(),
            vec![],
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            &ctx,
        )
        .await
        .unwrap_err();
        assert_eq!(StatusCode::PermissionDenied, err.status_code());

        Ok(())
    }

    #[tokio::test]
    async fn test_non_exact_query_discovery_keeps_denied_targets_for_batch_check() -> TestResult<()>
    {
        let plugins = Plugins::new();
        plugins.insert::<PermissionCheckerRef>(Arc::new(RejectUnresolvedPermissionChecker));
        let instance = test_instance_with_plugins_and_metric_names(
            test_logical_table(1024, "denied")?,
            test_logical_table(1025, "target")?,
            plugins,
            Some(test_metric_names_table()),
        )
        .await?;
        let ctx = test_query_ctx(1);

        let mut metric_names = PrometheusHandler::query_metric_names(
            &instance,
            vec![Matcher::new(
                promql_parser::label::MatchOp::NotEqual,
                "__name__",
                "",
            )],
            "public",
            &ctx,
        )
        .await
        .unwrap();
        metric_names.sort_unstable();
        assert_eq!(
            vec!["denied".to_string(), "target".to_string()],
            metric_names
        );

        let queries = metric_names
            .into_iter()
            .map(|query| PromQuery {
                query,
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let err = PrometheusHandler::check_query_permission(&instance, &queries, &ctx)
            .await
            .unwrap_err();
        assert_eq!(StatusCode::PermissionDenied, err.status_code());

        Ok(())
    }

    #[test]
    fn test_fast_legacy_check_is_read_only() {
        let cache = DashMap::new();
        cache.insert("metric1".to_string(), true);

        let names = vec!["metric1".to_string(), "metric2".to_string()];
        assert_eq!(Some(true), fast_legacy_check(&cache, &names).unwrap());
        assert!(!cache.contains_key("metric2"));

        cache_legacy_mode(&cache, &names, true).unwrap();
        assert!(*cache.get("metric2").unwrap().value());
        assert!(cache_legacy_mode(&cache, &names, false).is_err());
        assert!(*cache.get("metric2").unwrap().value());

        let cache_incompatible = DashMap::new();
        cache_incompatible.insert("metric1".to_string(), true);
        cache_incompatible.insert("metric2".to_string(), false);
        assert!(fast_legacy_check(&cache_incompatible, &names).is_err());
    }

    #[test]
    fn test_should_track_statement_process() {
        assert!(should_track_statement_process(&parse_one_sql(
            "SELECT * FROM demo"
        )));
        assert!(should_track_statement_process(&parse_one_sql(
            "INSERT INTO demo SELECT * FROM source"
        )));
        assert!(!should_track_statement_process(&parse_one_sql(
            "INSERT INTO demo VALUES (1)"
        )));
        assert!(!should_track_statement_process(&parse_one_sql(
            "INSERT INTO demo VALUES (now())"
        )));
    }

    #[test]
    fn test_should_track_plan_process() {
        let select_stmt = parse_one_sql("SELECT * FROM demo");
        let insert_select_stmt = parse_one_sql("INSERT INTO demo SELECT * FROM source");
        let insert_values_stmt = parse_one_sql("INSERT INTO demo VALUES (now())");

        let empty_plan = LogicalPlanBuilder::empty(false).build().unwrap();
        assert!(should_track_plan_process(Some(&select_stmt), &empty_plan));
        assert!(should_track_plan_process(
            Some(&insert_select_stmt),
            &insert_dml_plan()
        ));
        assert!(!should_track_plan_process(
            Some(&insert_values_stmt),
            &insert_dml_plan()
        ));
        assert!(!should_track_plan_process(None, &insert_dml_plan()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_insert_select_is_visible_in_show_processlist() -> TestResult<()> {
        let insert_sql = "INSERT INTO target SELECT * FROM source";
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let interceptor = Arc::new(BlockingInsertSelectInterceptor::new(started_tx, finish_rx));
        let instance = Arc::new(test_instance_with_insert_select_interceptor(interceptor).await?);

        let insert_task = tokio::spawn({
            let instance = instance.clone();
            async move { execute_one_sql(&instance, insert_sql, test_query_ctx(4242)).await }
        });

        tokio::time::timeout(Duration::from_secs(5), started_rx.recv())
            .await
            .context(InsertStartTimeoutSnafu)?
            .context(InsertStartChannelClosedSnafu)?;

        let output = execute_one_sql(&instance, "SHOW PROCESSLIST", test_query_ctx(43)).await?;
        let process_list = output.data.pretty_print().await;
        assert!(
            process_list.contains(insert_sql),
            "process list did not contain running insert:\n{process_list}"
        );

        finish_tx
            .send(())
            .map_err(|_| ReleaseBlockedInsertSnafu.build())?;
        insert_task.await.context(InsertTaskPanicSnafu)??;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_kill_query_cancels_insert_select() -> TestResult<()> {
        assert_kill_cancels_insert_select("KILL QUERY 4242").await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_kill_process_id_cancels_insert_select() -> TestResult<()> {
        assert_kill_cancels_insert_select("KILL 'test-frontend/4242'").await
    }

    async fn assert_kill_cancels_insert_select(kill_sql: &str) -> TestResult<()> {
        let insert_sql = "INSERT INTO target SELECT * FROM source";
        let (source_polled_tx, source_polled_rx) = oneshot::channel();
        let instance = Arc::new(
            test_instance_with_tables(
                pending_table(1024, "source", source_polled_tx)?,
                test_table(1025, "target")?,
            )
            .await?,
        );

        let insert_task = tokio::spawn({
            let instance = instance.clone();
            async move { execute_one_sql(&instance, insert_sql, test_query_ctx(4242)).await }
        });

        tokio::time::timeout(Duration::from_secs(5), source_polled_rx)
            .await
            .context(SourcePollTimeoutSnafu)?
            .context(SourcePollChannelClosedSnafu)?;

        let output = execute_one_sql(&instance, kill_sql, test_query_ctx(43)).await?;
        assert!(matches!(output.data, OutputData::AffectedRows(1)));

        let insert_result = tokio::time::timeout(Duration::from_secs(5), insert_task)
            .await
            .context(InsertTaskTimeoutSnafu)?
            .context(InsertTaskPanicSnafu)?;
        let err = match insert_result {
            Ok(_) => return InsertSelectNotCancelledSnafu.fail(),
            Err(TestError::ExecuteSql { source, .. }) => source,
            Err(err) => return Err(err),
        };
        assert_eq!(StatusCode::Cancelled, err.status_code());

        let output = execute_one_sql(&instance, "SHOW PROCESSLIST", test_query_ctx(43)).await?;
        let process_list = output.data.pretty_print().await;
        assert!(
            !process_list.contains(insert_sql),
            "process list still contains killed insert:\n{process_list}"
        );

        Ok(())
    }

    fn insert_dml_plan() -> LogicalPlan {
        let schema = SchemaRef::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let target = Arc::new(LogicalTableSource::new(schema));
        let input = LogicalPlanBuilder::empty(false).build().unwrap();

        LogicalPlanBuilder::insert_into(input, "demo", target, InsertOp::Append)
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_exec_validation() {
        let query_ctx = QueryContext::arc();
        let plugins: Plugins = Plugins::new();
        plugins.insert(QueryOptions {
            disallow_cross_catalog_query: true,
        });

        let sql = r#"
        SELECT * FROM demo;
        EXPLAIN SELECT * FROM demo;
        CREATE DATABASE test_database;
        SHOW DATABASES;
        "#;
        let stmts = parse_stmt(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(stmts.len(), 4);
        for stmt in stmts {
            let re = check_permission(plugins.clone(), &stmt, &query_ctx);
            re.unwrap();
        }

        let sql = r#"
        SHOW CREATE TABLE demo;
        ALTER TABLE demo ADD COLUMN new_col INT;
        "#;
        let stmts = parse_stmt(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(stmts.len(), 2);
        for stmt in stmts {
            let re = check_permission(plugins.clone(), &stmt, &query_ctx);
            re.unwrap();
        }

        fn replace_test(template_sql: &str, plugins: Plugins, query_ctx: &QueryContextRef) {
            // test right
            let right = vec![("", ""), ("", "public."), ("greptime.", "public.")];
            for (catalog, schema) in right {
                let sql = do_fmt(template_sql, catalog, schema);
                do_test(&sql, plugins.clone(), query_ctx, true);
            }

            let wrong = vec![
                ("wrongcatalog.", "public."),
                ("wrongcatalog.", "wrongschema."),
            ];
            for (catalog, schema) in wrong {
                let sql = do_fmt(template_sql, catalog, schema);
                do_test(&sql, plugins.clone(), query_ctx, false);
            }
        }

        fn do_fmt(template: &str, catalog: &str, schema: &str) -> String {
            let vars = HashMap::from([
                ("catalog".to_string(), catalog),
                ("schema".to_string(), schema),
            ]);
            template.format(&vars).unwrap()
        }

        fn do_test(sql: &str, plugins: Plugins, query_ctx: &QueryContextRef, is_ok: bool) {
            let stmt = &parse_stmt(sql, &GreptimeDbDialect {}).unwrap()[0];
            let re = check_permission(plugins, stmt, query_ctx);
            if is_ok {
                re.unwrap();
            } else {
                assert!(re.is_err());
            }
        }

        // test insert
        let sql = "INSERT INTO {catalog}{schema}monitor(host) VALUES ('host1');";
        replace_test(sql, plugins.clone(), &query_ctx);

        // test create table
        let sql = r#"CREATE TABLE {catalog}{schema}demo(
                            host STRING,
                            ts TIMESTAMP,
                            TIME INDEX (ts),
                            PRIMARY KEY(host)
                        ) engine=mito;"#;
        replace_test(sql, plugins.clone(), &query_ctx);

        // test drop table
        let sql = "DROP TABLE {catalog}{schema}demo;";
        replace_test(sql, plugins.clone(), &query_ctx);

        // test undrop table
        let sql = "UNDROP TABLE {catalog}{schema}demo;";
        replace_test(sql, plugins.clone(), &query_ctx);

        // test show tables
        let sql = "SHOW TABLES FROM public";
        let stmt = parse_stmt(sql, &GreptimeDbDialect {}).unwrap();
        check_permission(plugins.clone(), &stmt[0], &query_ctx).unwrap();

        let sql = "SHOW TABLES FROM private";
        let stmt = parse_stmt(sql, &GreptimeDbDialect {}).unwrap();
        let re = check_permission(plugins.clone(), &stmt[0], &query_ctx);
        assert!(re.is_ok());

        // test describe table
        let sql = "DESC TABLE {catalog}{schema}demo;";
        replace_test(sql, plugins.clone(), &query_ctx);

        let comment_flow_cases = [
            ("COMMENT ON FLOW my_flow IS 'comment';", true),
            ("COMMENT ON FLOW greptime.my_flow IS 'comment';", true),
            ("COMMENT ON FLOW wrongcatalog.my_flow IS 'comment';", false),
        ];
        for (sql, is_ok) in comment_flow_cases {
            let stmt = &parse_stmt(sql, &GreptimeDbDialect {}).unwrap()[0];
            let result = check_permission(plugins.clone(), stmt, &query_ctx);
            assert_eq!(result.is_ok(), is_ok);
        }

        let show_flow_cases = [
            ("SHOW CREATE FLOW my_flow;", true),
            ("SHOW CREATE FLOW greptime.my_flow;", true),
            ("SHOW CREATE FLOW wrongcatalog.my_flow;", false),
        ];
        for (sql, is_ok) in show_flow_cases {
            let stmt = &parse_stmt(sql, &GreptimeDbDialect {}).unwrap()[0];
            let result = check_permission(plugins.clone(), stmt, &query_ctx);
            assert_eq!(result.is_ok(), is_ok);
        }
    }
}
