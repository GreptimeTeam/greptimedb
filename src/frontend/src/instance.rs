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

use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_stream::stream;
use async_trait::async_trait;
use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq};
use catalog::process_manager::{ProcessManagerRef, QueryStatement as CatalogQueryStatement};
use catalog::CatalogManagerRef;
use client::OutputData;
use common_base::cancellation::CancellableFuture;
use common_base::Plugins;
use common_config::KvBackendConfig;
use common_error::ext::{BoxedError, ErrorExt};
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::key::runtime_switch::RuntimeSwitchManager;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::TableMetadataManagerRef;
use common_meta::kv_backend::KvBackendRef;
use common_meta::node_manager::NodeManagerRef;
use common_meta::procedure_executor::ProcedureExecutorRef;
use common_meta::state_store::KvStateStore;
use common_procedure::local::{LocalManager, ManagerConfig};
use common_procedure::options::ProcedureConfig;
use common_procedure::ProcedureManagerRef;
use common_query::Output;
use common_recordbatch::error::StreamTimeoutSnafu;
use common_recordbatch::RecordBatchStreamWrapper;
use common_telemetry::{debug, error, info, tracing};
use dashmap::DashMap;
use datafusion_expr::LogicalPlan;
use futures::{Stream, StreamExt};
use lazy_static::lazy_static;
use log_store::raft_engine::RaftEngineBackend;
use operator::delete::DeleterRef;
use operator::insert::InserterRef;
use operator::statement::{StatementExecutor, StatementExecutorRef};
use partition::manager::PartitionRuleManagerRef;
use pipeline::pipeline_operator::PipelineOperator;
use prometheus::HistogramTimer;
use promql_parser::label::Matcher;
use query::metrics::OnDone;
use query::parser::{PromQuery, QueryLanguageParser, QueryStatement};
use query::query_engine::options::{validate_catalog_and_schema, QueryOptions};
use query::query_engine::DescribeResult;
use query::QueryEngineRef;
use servers::error::{
    self as server_error, AuthSnafu, CommonMetaSnafu, ExecuteQuerySnafu,
    OtlpMetricModeIncompatibleSnafu, ParsePromQLSnafu, UnexpectedResultSnafu,
};
use servers::interceptor::{
    PromQueryInterceptor, PromQueryInterceptorRef, SqlQueryInterceptor, SqlQueryInterceptorRef,
};
use servers::otlp::metrics::legacy_normalize_otlp_name;
use servers::prometheus_handler::PrometheusHandler;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{Channel, QueryContextRef};
use session::table_name::table_idents_to_full_name;
use snafu::prelude::*;
use sql::ast::ObjectNamePartExt;
use sql::dialect::Dialect;
use sql::parser::{ParseOptions, ParserContext};
use sql::statements::copy::{CopyDatabase, CopyTable};
use sql::statements::statement::Statement;
use sql::statements::tql::Tql;
use sqlparser::ast::ObjectName;
pub use standalone::StandaloneDatanodeManager;
use table::requests::{OTLP_METRIC_COMPAT_KEY, OTLP_METRIC_COMPAT_PROM};

use crate::error::{
    self, Error, ExecLogicalPlanSnafu, ExecutePromqlSnafu, ExternalSnafu, InvalidSqlSnafu,
    ParseSqlSnafu, PermissionSnafu, PlanStatementSnafu, Result, SqlExecInterceptedSnafu,
    StatementTimeoutSnafu, TableOperationSnafu,
};
use crate::limiter::LimiterRef;
use crate::slow_query_recorder::SlowQueryRecorder;
use crate::stream_wrapper::CancellableStreamWrapper;

lazy_static! {
    static ref OTLP_LEGACY_DEFAULT_VALUE: String = "legacy".to_string();
}

/// The frontend instance contains necessary components, and implements many
/// traits, like [`servers::query_handler::grpc::GrpcQueryHandler`],
/// [`servers::query_handler::sql::SqlQueryHandler`], etc.
#[derive(Clone)]
pub struct Instance {
    catalog_manager: CatalogManagerRef,
    pipeline_operator: Arc<PipelineOperator>,
    statement_executor: Arc<StatementExecutor>,
    query_engine: QueryEngineRef,
    plugins: Plugins,
    inserter: InserterRef,
    deleter: DeleterRef,
    table_metadata_manager: TableMetadataManagerRef,
    slow_query_recorder: Option<SlowQueryRecorder>,
    limiter: Option<LimiterRef>,
    process_manager: ProcessManagerRef,

    // cache for otlp metrics
    // first layer key: db-string
    // key: direct input metric name
    // value: if runs in legacy mode
    otlp_metrics_table_legacy_cache: DashMap<String, DashMap<String, bool>>,
}

impl Instance {
    pub async fn try_build_standalone_components(
        dir: String,
        kv_backend_config: KvBackendConfig,
        procedure_config: ProcedureConfig,
    ) -> Result<(KvBackendRef, ProcedureManagerRef)> {
        info!(
            "Creating metadata kvbackend with config: {:?}",
            kv_backend_config
        );
        let kv_backend = RaftEngineBackend::try_open_with_cfg(dir, &kv_backend_config)
            .map_err(BoxedError::new)
            .context(error::OpenRaftEngineBackendSnafu)?;

        let kv_backend = Arc::new(kv_backend);
        let kv_state_store = Arc::new(KvStateStore::new(kv_backend.clone()));

        let manager_config = ManagerConfig {
            max_retry_times: procedure_config.max_retry_times,
            retry_delay: procedure_config.retry_delay,
            max_running_procedures: procedure_config.max_running_procedures,
            ..Default::default()
        };
        let runtime_switch_manager = Arc::new(RuntimeSwitchManager::new(kv_backend.clone()));
        let procedure_manager = Arc::new(LocalManager::new(
            manager_config,
            kv_state_store.clone(),
            kv_state_store,
            Some(runtime_switch_manager),
            None,
        ));

        Ok((kv_backend, procedure_manager))
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

    pub fn cache_invalidator(&self) -> &CacheInvalidatorRef {
        self.statement_executor.cache_invalidator()
    }

    pub fn procedure_executor(&self) -> &ProcedureExecutorRef {
        self.statement_executor.procedure_executor()
    }
}

fn parse_stmt(sql: &str, dialect: &(dyn Dialect + Send + Sync)) -> Result<Vec<Statement>> {
    ParserContext::create_with_dialect(sql, dialect, ParseOptions::default()).context(ParseSqlSnafu)
}

impl Instance {
    async fn query_statement(&self, stmt: Statement, query_ctx: QueryContextRef) -> Result<Output> {
        check_permission(self.plugins.clone(), &stmt, &query_ctx)?;

        let query_interceptor = self.plugins.get::<SqlQueryInterceptorRef<Error>>();
        let query_interceptor = query_interceptor.as_ref();

        if should_capture_statement(Some(&stmt)) {
            let slow_query_timer = self.slow_query_recorder.as_ref().and_then(|recorder| {
                recorder.start(CatalogQueryStatement::Sql(stmt.clone()), query_ctx.clone())
            });

            let ticket = self.process_manager.register_query(
                query_ctx.current_catalog().to_string(),
                vec![query_ctx.current_schema()],
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
                // compute remaining timeout
                let remaining_timeout = timeout.checked_sub(start.elapsed()).unwrap_or_default();
                attach_timeout(output, remaining_timeout)
            }
            None => {
                self.exec_statement(stmt, query_ctx, query_interceptor)
                    .await
            }
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
                if let Statement::Explain(explain) = &stmt {
                    if let Some(format) = explain.format() {
                        query_ctx.set_explain_format(format.to_string());
                    }
                }

                self.plan_and_exec_sql(stmt, &query_ctx, query_interceptor)
                    .await
            }
            Statement::Tql(tql) => {
                self.plan_and_exec_tql(&query_ctx, query_interceptor, tql)
                    .await
            }
            _ => {
                query_interceptor.pre_execute(&stmt, None, query_ctx.clone())?;
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
        query_interceptor.pre_execute(&stmt, Some(&plan), query_ctx.clone())?;
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
        query_interceptor.pre_execute(&Statement::Tql(tql), Some(&plan), query_ctx.clone())?;
        self.statement_executor
            .exec_plan(plan, query_ctx.clone())
            .await
            .context(TableOperationSnafu)
    }

    async fn check_otlp_legacy(
        &self,
        names: &[&String],
        ctx: QueryContextRef,
    ) -> server_error::Result<bool> {
        let db_string = ctx.get_db_string();
        let cache = self
            .otlp_metrics_table_legacy_cache
            .entry(db_string)
            .or_default();

        // check cache
        let hit_cache = names
            .iter()
            .filter_map(|name| cache.get(*name))
            .collect::<Vec<_>>();
        if !hit_cache.is_empty() {
            let hit_legacy = hit_cache.iter().any(|en| *en.value());
            let hit_prom = hit_cache.iter().any(|en| !*en.value());

            // hit but have true and false, means both legacy and new mode are used
            // we cannot handle this case, so return error
            // add doc links in err msg later
            ensure!(!(hit_legacy && hit_prom), OtlpMetricModeIncompatibleSnafu);

            let flag = hit_legacy;
            // set cache for all names
            names.iter().for_each(|name| {
                if !cache.contains_key(*name) {
                    cache.insert(name.to_string(), flag);
                }
            });
            return Ok(flag);
        }

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
            // set cache
            names.iter().for_each(|name| {
                cache.insert(name.to_string(), false);
            });
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
            let flag = has_legacy;
            names.iter().for_each(|name| {
                cache.insert(name.to_string(), flag);
            });
            Ok(flag)
        } else {
            // no table info, use new mode
            names.iter().for_each(|name| {
                cache.insert(name.to_string(), false);
            });
            Ok(false)
        }
    }
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
            };
            Output::new(OutputData::Stream(Box::pin(stream)), output.meta)
        }
    };

    Ok(output)
}

#[async_trait]
impl SqlQueryHandler for Instance {
    type Error = Error;

    #[tracing::instrument(skip_all)]
    async fn do_query(&self, query: &str, query_ctx: QueryContextRef) -> Vec<Result<Output>> {
        let query_interceptor_opt = self.plugins.get::<SqlQueryInterceptorRef<Error>>();
        let query_interceptor = query_interceptor_opt.as_ref();
        let query = match query_interceptor.pre_parsing(query, query_ctx.clone()) {
            Ok(q) => q,
            Err(e) => return vec![Err(e)],
        };

        let checker_ref = self.plugins.get::<PermissionCheckerRef>();
        let checker = checker_ref.as_ref();

        match parse_stmt(query.as_ref(), query_ctx.sql_dialect())
            .and_then(|stmts| query_interceptor.post_parsing(stmts, query_ctx.clone()))
        {
            Ok(stmts) => {
                if stmts.is_empty() {
                    return vec![InvalidSqlSnafu {
                        err_msg: "empty statements",
                    }
                    .fail()];
                }

                let mut results = Vec::with_capacity(stmts.len());
                for stmt in stmts {
                    if let Err(e) = checker
                        .check_permission(
                            query_ctx.current_user(),
                            PermissionReq::SqlStatement(&stmt),
                        )
                        .context(PermissionSnafu)
                    {
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

    async fn do_exec_plan(
        &self,
        stmt: Option<Statement>,
        plan: LogicalPlan,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        if should_capture_statement(stmt.as_ref()) {
            // It's safe to unwrap here because we've already checked the type.
            let stmt = stmt.unwrap();
            let query = stmt.to_string();
            let slow_query_timer = self.slow_query_recorder.as_ref().and_then(|recorder| {
                recorder.start(CatalogQueryStatement::Sql(stmt), query_ctx.clone())
            });

            let ticket = self.process_manager.register_query(
                query_ctx.current_catalog().to_string(),
                vec![query_ctx.current_schema()],
                query,
                query_ctx.conn_info().to_string(),
                Some(query_ctx.process_id()),
                slow_query_timer,
            );

            let query_fut = self.query_engine.execute(plan.clone(), query_ctx);

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
                .context(ExecLogicalPlanSnafu)
        } else {
            // plan should be prepared before exec
            // we'll do check there
            self.query_engine
                .execute(plan.clone(), query_ctx)
                .await
                .context(ExecLogicalPlanSnafu)
        }
    }

    #[tracing::instrument(skip_all)]
    async fn do_promql_query(
        &self,
        query: &PromQuery,
        query_ctx: QueryContextRef,
    ) -> Vec<Result<Output>> {
        // check will be done in prometheus handler's do_query
        let result = PrometheusHandler::do_query(self, query, query_ctx)
            .await
            .with_context(|_| ExecutePromqlSnafu {
                query: format!("{query:?}"),
            });
        vec![result]
    }

    async fn do_describe(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Option<DescribeResult>> {
        if matches!(
            stmt,
            Statement::Insert(_) | Statement::Query(_) | Statement::Delete(_)
        ) {
            self.plugins
                .get::<PermissionCheckerRef>()
                .as_ref()
                .check_permission(query_ctx.current_user(), PermissionReq::SqlStatement(&stmt))
                .context(PermissionSnafu)?;

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

    async fn is_valid_schema(&self, catalog: &str, schema: &str) -> Result<bool> {
        self.catalog_manager
            .schema_exists(catalog, schema, None)
            .await
            .context(error::CatalogSnafu)
    }
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

#[async_trait]
impl PrometheusHandler for Instance {
    #[tracing::instrument(skip_all)]
    async fn do_query(
        &self,
        query: &PromQuery,
        query_ctx: QueryContextRef,
    ) -> server_error::Result<Output> {
        let interceptor = self
            .plugins
            .get::<PromQueryInterceptorRef<server_error::Error>>();

        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(query_ctx.current_user(), PermissionReq::PromQuery)
            .context(AuthSnafu)?;

        let stmt = QueryLanguageParser::parse_promql(query, &query_ctx).with_context(|_| {
            ParsePromQLSnafu {
                query: query.clone(),
            }
        })?;

        let plan = self
            .statement_executor
            .plan(&stmt, query_ctx.clone())
            .await
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;

        interceptor.pre_execute(query, Some(&plan), query_ctx.clone())?;

        // Take the EvalStmt from the original QueryStatement and use it to create the CatalogQueryStatement.
        let query_statement = if let QueryStatement::Promql(eval_stmt) = stmt {
            CatalogQueryStatement::Promql(eval_stmt)
        } else {
            // It should not happen since the query is already parsed successfully.
            return UnexpectedResultSnafu {
                reason: "The query should always be promql.".to_string(),
            }
            .fail();
        };
        let query = query_statement.to_string();

        let slow_query_timer = self
            .slow_query_recorder
            .as_ref()
            .and_then(|recorder| recorder.start(query_statement, query_ctx.clone()));

        let ticket = self.process_manager.register_query(
            query_ctx.current_catalog().to_string(),
            vec![query_ctx.current_schema()],
            query,
            query_ctx.conn_info().to_string(),
            Some(query_ctx.process_id()),
            slow_query_timer,
        );

        let query_fut = self.statement_executor.exec_plan(plan, query_ctx.clone());

        let output = CancellableFuture::new(query_fut, ticket.cancellation_handle.clone())
            .await
            .map_err(|_| servers::error::CancelledSnafu.build())?
            .map(|output| {
                let Output { meta, data } = output;
                let data = match data {
                    OutputData::Stream(stream) => {
                        OutputData::Stream(Box::pin(CancellableStreamWrapper::new(stream, ticket)))
                    }
                    other => other,
                };
                Output { data, meta }
            })
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;

        Ok(interceptor.post_execute(output, query_ctx)?)
    }

    async fn query_metric_names(
        &self,
        matchers: Vec<Matcher>,
        ctx: &QueryContextRef,
    ) -> server_error::Result<Vec<String>> {
        self.handle_query_metric_names(matchers, ctx)
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
        self.handle_query_label_values(metric, label_name, matchers, start, end, ctx)
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
            validate_param(&stmt.flow_name, query_ctx)?;
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

// Create a query ticket and slow query timer if the statement is a query or readonly statement.
fn should_capture_statement(stmt: Option<&Statement>) -> bool {
    if let Some(stmt) = stmt {
        matches!(stmt, Statement::Query(_)) || stmt.is_readonly()
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common_base::Plugins;
    use query::query_engine::options::QueryOptions;
    use session::context::QueryContext;
    use sql::dialect::GreptimeDbDialect;
    use strfmt::Format;

    use super::*;

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
        replace_test(sql, plugins, &query_ctx);
    }
}
