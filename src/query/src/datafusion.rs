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

//! Planner, QueryEngine implementations based on DataFusion.

mod error;
mod json_expr_planner;
mod pg_oid_alias_expr_planner;
mod planner;

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use catalog::kvbackend::KvBackendCatalogManager;
use common_base::Plugins;
use common_catalog::consts::is_readonly_table;
use common_error::ext::BoxedError;
use common_function::function::FunctionContext;
use common_function::function_factory::ScalarFunctionFactory;
use common_query::{Output, OutputData, OutputMeta};
use common_recordbatch::adapter::{RecordBatchStreamAdapter, RegionQueryStatCounters};
use common_recordbatch::{EmptyRecordBatchStream, SendableRecordBatchStream};
use common_telemetry::tracing;
use datafusion::catalog::TableFunction;
use datafusion::dataframe::DataFrame;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_common::ResolvedTableReference;
use datafusion_expr::{
    AggregateUDF, DmlStatement, LogicalPlan as DfLogicalPlan, LogicalPlan, WindowUDF, WriteOp,
};
use datatypes::prelude::VectorRef;
use datatypes::schema::Schema;
use futures_util::StreamExt;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt, ensure};
use sqlparser::ast::AnalyzeFormat;
use table::TableRef;
use table::requests::{DeleteRequest, InsertRequest};
use table::table::scan::{REGION_SCAN_EXEC_NAME, RegionScanExec};
use tracing::Span;

use crate::analyze::DistAnalyzeExec;
pub use crate::datafusion::planner::DfContextProviderAdapter;
use crate::dist_plan::{
    DIST_JOIN_PLANNER_RULE_NAME, DistJoinStats, DistPlannerOptions, MergeScanLogicalPlan,
    RemoteDynFilterReceiverInjectorRef, aggregate_region_stats, candidate_joins,
    expected_region_ids,
};
use crate::error::{
    CatalogSnafu, CreateRecordBatchSnafu, MissingTableMutationHandlerSnafu,
    MissingTimestampColumnSnafu, QueryExecutionSnafu, Result, TableMutationSnafu,
    TableNotFoundSnafu, TableReadOnlySnafu, UnsupportedExprSnafu,
};
use crate::executor::QueryExecutor;
use crate::metrics::{
    OnDone, QUERY_STAGE_ELAPSED, maybe_attach_region_watermark_metrics,
    should_collect_region_watermark_from_query_ctx,
};
use crate::options::ScheduledTimeExtension;
use crate::physical_wrapper::PhysicalPlanWrapperRef;
use crate::planner::{DfLogicalPlanner, LogicalPlanner};
use crate::query_engine::{DescribeResult, QueryEngineContext, QueryEngineState};
use crate::{QueryEngine, metrics};

/// Query parallelism hint key.
/// This hint can be set in the query context to control the parallelism of the query execution.
pub const QUERY_PARALLELISM_HINT: &str = "query_parallelism";

/// Whether to fallback to the original plan when failed to push down.
pub const QUERY_FALLBACK_HINT: &str = "query_fallback";

fn query_load_region_id(plan: &Arc<dyn ExecutionPlan>) -> Option<u64> {
    let mut region_id = None;
    let mut stack = vec![plan.clone()];

    while let Some(plan) = stack.pop() {
        if plan.name() == REGION_SCAN_EXEC_NAME
            && let Some(scan) = plan.downcast_ref::<RegionScanExec>()
            && let Some(scan_region_id) = scan.query_load_region_id()
        {
            match region_id {
                Some(region_id) if region_id != scan_region_id => return None,
                Some(_) => {}
                None => region_id = Some(scan_region_id),
            }
        }
        stack.extend(plan.children().into_iter().cloned());
    }

    region_id
}

// Finds the region-owned query statistic counters from the local datanode scan plan.
//
// Unlike the Prometheus read-load reporting in `MergeScanExec`, the heartbeat
// counters must be updated before metrics leave the datanode process. The
// `RecordBatchStreamAdapter` that resolves `RecordBatchMetrics` does not know
// the owning `MitoRegion`, so we extract the counters from `RegionScanExec` and
// pass them to the adapter. If a plan contains scans from different regions,
// return `None` to avoid charging the whole plan metrics to one region.
fn query_stat_counters(plan: &Arc<dyn ExecutionPlan>) -> Option<RegionQueryStatCounters> {
    let mut counters: Option<RegionQueryStatCounters> = None;
    let mut stack = vec![plan.clone()];

    while let Some(plan) = stack.pop() {
        if plan.name() == REGION_SCAN_EXEC_NAME
            && let Some(scan) = plan.downcast_ref::<RegionScanExec>()
            && let Some(scan_counters) = scan.query_stat_counters()
        {
            match &counters {
                Some(counters)
                    if !Arc::ptr_eq(&counters.query_cpu_time, &scan_counters.query_cpu_time)
                        || !Arc::ptr_eq(
                            &counters.query_scanned_bytes,
                            &scan_counters.query_scanned_bytes,
                        ) =>
                {
                    return None;
                }
                Some(_) => {}
                None => counters = Some(scan_counters),
            }
        }
        stack.extend(plan.children().into_iter().cloned());
    }

    counters
}

pub struct DatafusionQueryEngine {
    state: Arc<QueryEngineState>,
    plugins: Plugins,
}

impl DatafusionQueryEngine {
    pub fn new(state: Arc<QueryEngineState>, plugins: Plugins) -> Self {
        Self { state, plugins }
    }

    /// Statistics of the candidate tables of `logical_plan`, or `None` when the nested broadcast
    /// join rewrite has to keep the existing plan.
    ///
    /// Gated by the session opt-in and the registered distributed rules; the manual build table
    /// option selects the build side by name and never consults statistics.
    ///
    /// The candidate tables are resolved to their physical routes before the statistics of the
    /// query are fetched: the route must still describe the table the plan captured and at least
    /// one candidate pair must have usable routes, otherwise this is `None` without any
    /// `region_stats` call. The region sizes of the resolved tables then come from one
    /// `region_stats` call of the information extension, so the result belongs to this query only
    /// and never reaches the shared engine state. The rewrite is an optimization, so a table
    /// whose statistics are unusable (missing, duplicate, zero-sized or overflowing) is left out
    /// instead of failing the query.
    async fn dist_join_stats(
        &self,
        ctx: &QueryEngineContext,
        logical_plan: &LogicalPlan,
    ) -> Option<DistJoinStats> {
        if !ctx
            .query_ctx()
            .configuration_parameter()
            .experimental_dist_join()
        {
            return None;
        }

        let state = ctx.state();
        if !state
            .analyzer()
            .rules
            .iter()
            .any(|rule| rule.name() == DIST_JOIN_PLANNER_RULE_NAME)
        {
            return None;
        }
        if state
            .config()
            .options()
            .extensions
            .get::<DistPlannerOptions>()
            .and_then(|options| options.nested_broadcast_join_build_table.as_ref())
            .is_some()
        {
            return None;
        }

        let candidates = candidate_joins(logical_plan);
        if candidates.is_empty() {
            return None;
        }

        let catalog_manager = self
            .state
            .catalog_manager()
            .as_any()
            .downcast_ref::<KvBackendCatalogManager>()?;
        let partition_manager = catalog_manager.partition_manager();

        // Resolve the routes before fetching the statistics: a table whose route does not
        // describe the id the plan captured is not priced, and without one candidate pair whose
        // two sides both have usable routes the statistics cannot decide the rewrite.
        let mut routes = BTreeMap::new();
        for table_id in candidates
            .iter()
            .flat_map(|candidate| [candidate.probe, candidate.build])
            .collect::<BTreeSet<_>>()
        {
            let Ok((physical_table_id, route)) = partition_manager
                .find_physical_table_route_with_id(table_id)
                .await
            else {
                continue;
            };
            // A logical table resolves to the route of its physical table; pricing its regions
            // under the captured id would describe another table.
            if physical_table_id != table_id {
                continue;
            }
            let Some(regions) = expected_region_ids(physical_table_id, &route.region_routes) else {
                continue;
            };

            routes.insert(table_id, regions);
        }
        let has_usable_pair = candidates.iter().any(|candidate| {
            routes.contains_key(&candidate.probe) && routes.contains_key(&candidate.build)
        });
        if !has_usable_pair {
            return None;
        }

        let reports = match catalog_manager.information_extension().region_stats().await {
            Ok(reports) => reports,
            Err(err) => {
                common_telemetry::debug!(
                    err = %err,
                    "Failed to fetch the region statistics of the nested broadcast join, keeping the existing plan"
                );
                return None;
            }
        };

        let mut stats = DistJoinStats::default();
        for (table_id, regions) in routes {
            let Some(table_stats) = aggregate_region_stats(&regions, &reports) else {
                continue;
            };

            stats.tables.insert(table_id, table_stats);
        }

        (!stats.tables.is_empty()).then_some(stats)
    }

    #[tracing::instrument(skip_all)]
    async fn exec_query_plan(
        &self,
        plan: LogicalPlan,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let mut ctx = self.engine_context(query_ctx.clone());
        let plan = if let Some(receiver_injector) =
            self.plugins.get::<RemoteDynFilterReceiverInjectorRef>()
        {
            receiver_injector.maybe_inject(plan, query_ctx.clone())
        } else {
            plan
        };

        // `create_physical_plan` will optimize logical plan internally
        let physical_plan = self.create_physical_plan(&mut ctx, &plan).await?;
        let physical_plan = self.optimize_physical_plan(&mut ctx, physical_plan)?;
        let physical_plan = if let Some(wrapper) = self.plugins.get::<PhysicalPlanWrapperRef>() {
            wrapper.wrap(physical_plan, query_ctx)
        } else {
            physical_plan
        };

        let stream = self.execute_stream(&ctx, &physical_plan)?;

        Ok(Output::new(
            OutputData::Stream(stream),
            OutputMeta::new_with_plan(physical_plan),
        ))
    }

    #[tracing::instrument(skip_all)]
    async fn exec_dml_statement(
        &self,
        dml: DmlStatement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        ensure!(
            matches!(dml.op, WriteOp::Insert(_) | WriteOp::Delete),
            UnsupportedExprSnafu {
                name: format!("DML op {}", dml.op),
            }
        );

        let _timer = QUERY_STAGE_ELAPSED
            .with_label_values(&[dml.op.name()])
            .start_timer();

        let default_catalog = &query_ctx.current_catalog().to_owned();
        let default_schema = &query_ctx.current_schema();
        let table_name = dml.table_name.resolve(default_catalog, default_schema);
        let table = self.find_table(&table_name, &query_ctx).await?;

        let Output { data, meta } = self
            .exec_query_plan((*dml.input).clone(), query_ctx.clone())
            .await?;
        let mut stream = match data {
            OutputData::RecordBatches(batches) => batches.as_stream(),
            OutputData::Stream(stream) => stream,
            _ => unreachable!(),
        };

        let mut affected_rows = 0;
        let mut insert_cost = 0;

        match dml.op {
            WriteOp::Insert(_) => {
                while let Some(batch) = stream.next().await {
                    let batch = batch.context(CreateRecordBatchSnafu)?;
                    let column_vectors = batch
                        .column_vectors(&table_name.to_string(), table.schema())
                        .map_err(BoxedError::new)
                        .context(QueryExecutionSnafu)?;
                    // We ignore the insert op.
                    let output = self
                        .insert(&table_name, column_vectors, query_ctx.clone())
                        .await?;
                    let (rows, cost) = output.extract_rows_and_cost();
                    affected_rows += rows;
                    insert_cost += cost;
                }
            }
            WriteOp::Delete => {
                while let Some(batch) = stream.next().await {
                    let batch = batch.context(CreateRecordBatchSnafu)?;
                    let column_vectors = batch
                        .column_vectors(&table_name.to_string(), table.schema())
                        .map_err(BoxedError::new)
                        .context(QueryExecutionSnafu)?;
                    affected_rows += self
                        .delete(&table_name, &table, column_vectors, query_ctx.clone())
                        .await?;
                }
            }
            _ => unreachable!("guarded by the 'ensure!' at the beginning"),
        }
        Ok(Output::new(
            OutputData::AffectedRows(affected_rows),
            OutputMeta::new(meta.plan, insert_cost),
        ))
    }

    #[tracing::instrument(skip_all)]
    async fn delete(
        &self,
        table_name: &ResolvedTableReference,
        table: &TableRef,
        column_vectors: HashMap<String, VectorRef>,
        query_ctx: QueryContextRef,
    ) -> Result<usize> {
        let catalog_name = table_name.catalog.to_string();
        let schema_name = table_name.schema.to_string();
        let table_name = table_name.table.to_string();
        let table_schema = table.schema();

        ensure!(
            !is_readonly_table(&schema_name, &table_name),
            TableReadOnlySnafu { table: table_name }
        );

        let ts_column = table_schema
            .timestamp_column()
            .map(|x| &x.name)
            .with_context(|| MissingTimestampColumnSnafu {
                table_name: table_name.clone(),
            })?;

        let table_info = table.table_info();
        let rowkey_columns = table_info
            .meta
            .row_key_column_names()
            .collect::<Vec<&String>>();
        let column_vectors = column_vectors
            .into_iter()
            .filter(|x| &x.0 == ts_column || rowkey_columns.contains(&&x.0))
            .collect::<HashMap<_, _>>();

        let request = DeleteRequest {
            catalog_name,
            schema_name,
            table_name,
            key_column_values: column_vectors,
        };

        self.state
            .table_mutation_handler()
            .context(MissingTableMutationHandlerSnafu)?
            .delete(request, query_ctx)
            .await
            .context(TableMutationSnafu)
    }

    #[tracing::instrument(skip_all)]
    async fn insert(
        &self,
        table_name: &ResolvedTableReference,
        column_vectors: HashMap<String, VectorRef>,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let catalog_name = table_name.catalog.to_string();
        let schema_name = table_name.schema.to_string();
        let table_name = table_name.table.to_string();

        ensure!(
            !is_readonly_table(&schema_name, &table_name),
            TableReadOnlySnafu { table: table_name }
        );

        let request = InsertRequest {
            catalog_name,
            schema_name,
            table_name,
            columns_values: column_vectors,
            skip_wal: query_ctx.skip_wal(),
        };

        self.state
            .table_mutation_handler()
            .context(MissingTableMutationHandlerSnafu)?
            .insert(request, query_ctx)
            .await
            .context(TableMutationSnafu)
    }

    async fn find_table(
        &self,
        table_name: &ResolvedTableReference,
        query_context: &QueryContextRef,
    ) -> Result<TableRef> {
        let catalog_name = table_name.catalog.as_ref();
        let schema_name = table_name.schema.as_ref();
        let table_name = table_name.table.as_ref();

        self.state
            .catalog_manager()
            .table(catalog_name, schema_name, table_name, Some(query_context))
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu { table: table_name })
    }

    #[tracing::instrument(skip_all)]
    async fn create_physical_plan(
        &self,
        ctx: &mut QueryEngineContext,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        /// Only print context on panic, to avoid cluttering logs.
        ///
        /// TODO(discord9): remove this once we catch the bug
        #[derive(Debug)]
        struct PanicLogger<'a> {
            input_logical_plan: &'a LogicalPlan,
            after_analyze: Option<LogicalPlan>,
            after_optimize: Option<LogicalPlan>,
            phy_plan: Option<Arc<dyn ExecutionPlan>>,
        }
        impl Drop for PanicLogger<'_> {
            fn drop(&mut self) {
                if std::thread::panicking() {
                    common_telemetry::error!(
                        "Panic while creating physical plan, input logical plan: {:?}, after analyze: {:?}, after optimize: {:?}, final physical plan: {:?}",
                        self.input_logical_plan,
                        self.after_analyze,
                        self.after_optimize,
                        self.phy_plan
                    );
                }
            }
        }

        let mut logger = PanicLogger {
            input_logical_plan: logical_plan,
            after_analyze: None,
            after_optimize: None,
            phy_plan: None,
        };

        let _timer = metrics::CREATE_PHYSICAL_ELAPSED.start_timer();

        common_telemetry::debug!("Create physical plan, input plan: {logical_plan}");

        // The nested broadcast join rewrite reads its statistics from a query-local config
        // extension. This has to happen before the EXPLAIN branch below: the input of an EXPLAIN
        // plan is analyzed by the physical planner of DataFusion, i.e. outside this function.
        if let Some(stats) = self.dist_join_stats(ctx, logical_plan).await {
            ctx.state_mut()
                .config_mut()
                .options_mut()
                .extensions
                .insert(stats);
        }

        let state = ctx.state();

        // special handle EXPLAIN plan
        if matches!(logical_plan, DfLogicalPlan::Explain(_)) {
            return state
                .create_physical_plan(logical_plan)
                .await
                .map_err(Into::into);
        }

        // analyze first
        let analyzed_plan = state.analyzer().execute_and_check(
            logical_plan.clone(),
            state.config_options(),
            |_, _| {},
        )?;

        logger.after_analyze = Some(analyzed_plan.clone());

        common_telemetry::debug!("Create physical plan, analyzed plan: {analyzed_plan}");

        // skip optimize for MergeScan
        let optimized_plan = if let DfLogicalPlan::Extension(ext) = &analyzed_plan
            && ext.node.name() == MergeScanLogicalPlan::name()
        {
            analyzed_plan.clone()
        } else {
            state
                .optimizer()
                .optimize(analyzed_plan, state, |_, _| {})?
        };

        common_telemetry::debug!("Create physical plan, optimized plan: {optimized_plan}");
        logger.after_optimize = Some(optimized_plan.clone());

        let physical_plan = state
            .query_planner()
            .create_physical_plan(&optimized_plan, state)
            .await?;

        logger.phy_plan = Some(physical_plan.clone());
        drop(logger);
        Ok(physical_plan)
    }

    #[tracing::instrument(skip_all)]
    fn optimize_physical_plan(
        &self,
        ctx: &mut QueryEngineContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let _timer = metrics::OPTIMIZE_PHYSICAL_ELAPSED.start_timer();

        // TODO(ruihang): `self.create_physical_plan()` already optimize the plan, check
        // if we need to optimize it again here.
        // let state = ctx.state();
        // let config = state.config_options();

        // skip optimize AnalyzeExec plan
        let optimized_plan = if let Some(analyze_plan) = plan.downcast_ref::<AnalyzeExec>() {
            let format = if let Some(format) = ctx.query_ctx().explain_format()
                && format.to_lowercase() == "json"
            {
                AnalyzeFormat::JSON
            } else {
                AnalyzeFormat::TEXT
            };
            // Sets the verbose flag of the query context.
            // The MergeScanExec plan uses the verbose flag to determine whether to print the plan in verbose mode.
            ctx.query_ctx().set_explain_verbose(analyze_plan.verbose());

            Arc::new(DistAnalyzeExec::new(
                analyze_plan.input().clone(),
                analyze_plan.verbose(),
                format,
            ))
            // let mut new_plan = analyze_plan.input().clone();
            // for optimizer in state.physical_optimizers() {
            //     new_plan = optimizer
            //         .optimize(new_plan, config)
            //         .context(DataFusionSnafu)?;
            // }
            // Arc::new(DistAnalyzeExec::new(new_plan))
        } else {
            plan
            // let mut new_plan = plan;
            // for optimizer in state.physical_optimizers() {
            //     new_plan = optimizer
            //         .optimize(new_plan, config)
            //         .context(DataFusionSnafu)?;
            // }
            // new_plan
        };

        Ok(optimized_plan)
    }
}

#[async_trait]
impl QueryEngine for DatafusionQueryEngine {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn planner(&self) -> Arc<dyn LogicalPlanner> {
        Arc::new(DfLogicalPlanner::new(self.state.clone()))
    }

    fn name(&self) -> &str {
        "datafusion"
    }

    async fn describe(
        &self,
        plan: LogicalPlan,
        _query_ctx: QueryContextRef,
    ) -> Result<DescribeResult> {
        Ok(DescribeResult { logical_plan: plan })
    }

    async fn execute(&self, plan: LogicalPlan, query_ctx: QueryContextRef) -> Result<Output> {
        match plan {
            LogicalPlan::Dml(dml) => self.exec_dml_statement(dml, query_ctx).await,
            _ => self.exec_query_plan(plan, query_ctx).await,
        }
    }

    /// Note in SQL queries, aggregate names are looked up using
    /// lowercase unless the query uses quotes. For example,
    ///
    /// `SELECT MY_UDAF(x)...` will look for an aggregate named `"my_udaf"`
    /// `SELECT "my_UDAF"(x)` will look for an aggregate named `"my_UDAF"`
    ///
    /// So it's better to make UDAF name lowercase when creating one.
    fn register_aggregate_function(&self, func: AggregateUDF) {
        self.state.register_aggr_function(func);
    }

    /// Register an scalar function.
    /// Will override if the function with same name is already registered.
    fn register_scalar_function(&self, func: ScalarFunctionFactory) {
        self.state.register_scalar_function(func);
    }

    fn register_table_function(&self, func: Arc<TableFunction>) {
        self.state.register_table_function(func);
    }

    fn register_window_function(&self, func: WindowUDF) {
        self.state.register_window_function(func);
    }

    fn read_table(&self, table: TableRef) -> Result<DataFrame> {
        self.state.read_table(table).map_err(Into::into)
    }

    fn engine_context(&self, query_ctx: QueryContextRef) -> QueryEngineContext {
        let mut state = self.state.session_state();
        state.config_mut().set_extension(query_ctx.clone());
        state.config_mut().set_extension(self.state.clone());
        // note that hints in "x-greptime-hints" is automatically parsed
        // and set to query context's extension, so we can get it from query context.
        if let Some(parallelism) = query_ctx.extension(QUERY_PARALLELISM_HINT) {
            if let Ok(n) = parallelism.parse::<u64>() {
                if n > 0 {
                    let new_cfg = state.config().clone().with_target_partitions(n as usize);
                    *state.config_mut() = new_cfg;
                }
            } else {
                common_telemetry::warn!(
                    "Failed to parse query_parallelism: {}, using default value",
                    parallelism
                );
            }
        }

        // configure execution options
        state.config_mut().options_mut().execution.time_zone =
            Some(query_ctx.timezone().to_string());

        // usually it's impossible to have both `set variable` set by sql client and
        // hint in header by grpc client, so only need to deal with them separately.
        // Start from the options already configured on the session config (e.g. the
        // engine level `allow_query_fallback`) so that an insert below doesn't drop them.
        let mut dist_planner_options = state
            .config()
            .options()
            .extensions
            .get::<DistPlannerOptions>()
            .cloned()
            .unwrap_or_default();
        let mut has_dist_planner_options = false;
        if query_ctx.configuration_parameter().allow_query_fallback() {
            dist_planner_options.allow_query_fallback = true;
            has_dist_planner_options = true;
        } else if let Some(fallback) = query_ctx.extension(QUERY_FALLBACK_HINT) {
            // also check the query context for fallback hint
            // if it is set, we will enable the fallback
            if fallback.to_lowercase().parse::<bool>().unwrap_or(false) {
                dist_planner_options.allow_query_fallback = true;
                has_dist_planner_options = true;
            }
        }

        if has_dist_planner_options {
            state
                .config_mut()
                .options_mut()
                .extensions
                .insert(dist_planner_options);
        }

        state
            .config_mut()
            .options_mut()
            .extensions
            .insert(FunctionContext {
                query_ctx: query_ctx.clone(),
                state: self.engine_state().function_state(),
            });

        // Carry scheduled Flow time through ConfigOptions.extensions so that
        // the distributed plan analyzer can read it during expression
        // simplification (preventing wall-clock constant-folding of `now()`).
        state
            .config_mut()
            .options_mut()
            .extensions
            .insert(ScheduledTimeExtension {
                scheduled_time: crate::options::scheduled_time_from_ctx(&query_ctx),
            });

        let config_options = state.config_options().clone();
        let _ = state
            .execution_props_mut()
            .config_options
            .insert(config_options);

        // Apply scheduled time from query context if present, so that `now()` /
        // `current_timestamp()` functions evaluate against the logical scheduled time
        // rather than wall-clock.
        match crate::options::parse_scheduled_time_datetime(&query_ctx.extensions()) {
            Ok(Some(scheduled_rt)) => {
                state.execution_props_mut().query_execution_start_time = Some(scheduled_rt);
            }
            Ok(None) => {}
            Err(err) => {
                common_telemetry::warn!(err; "Ignoring invalid scheduled time query extension");
            }
        }

        QueryEngineContext::new(state, query_ctx)
    }

    fn engine_state(&self) -> &QueryEngineState {
        &self.state
    }
}

impl QueryExecutor for DatafusionQueryEngine {
    #[tracing::instrument(skip_all)]
    fn execute_stream(
        &self,
        ctx: &QueryEngineContext,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let query_ctx = ctx.query_ctx();
        let explain_verbose = query_ctx.explain_verbose();
        let should_collect_region_watermark =
            should_collect_region_watermark_from_query_ctx(&query_ctx)?;
        let output_partitions = plan.properties().output_partitioning().partition_count();
        if explain_verbose {
            common_telemetry::info!("Executing query plan, output_partitions: {output_partitions}");
        }

        let exec_timer = metrics::EXEC_PLAN_ELAPSED.start_timer();
        let task_ctx = ctx.build_task_ctx();
        let span = Span::current();

        match plan.properties().output_partitioning().partition_count() {
            0 => {
                let schema = Arc::new(
                    Schema::try_from(plan.schema())
                        .map_err(BoxedError::new)
                        .context(QueryExecutionSnafu)?,
                );
                Ok(Box::pin(EmptyRecordBatchStream::new(schema)))
            }
            1 => {
                let df_stream = plan.execute(0, task_ctx)?;
                let mut stream = RecordBatchStreamAdapter::try_new_with_span(df_stream, span)
                    .context(error::ConvertDfRecordBatchStreamSnafu)
                    .map_err(BoxedError::new)
                    .context(QueryExecutionSnafu)?;
                stream.set_metrics2(plan.clone());
                stream.set_query_load_region_id(query_load_region_id(plan));
                stream.set_query_stat_counters(query_stat_counters(plan));
                stream.set_explain_verbose(explain_verbose);
                let stream = OnDone::new(Box::pin(stream), move || {
                    let exec_cost = exec_timer.stop_and_record();
                    if explain_verbose {
                        common_telemetry::info!(
                            "DatafusionQueryEngine execute 1 stream, cost: {:?}s",
                            exec_cost,
                        );
                    }
                });
                Ok(maybe_attach_region_watermark_metrics(
                    Box::pin(stream),
                    plan.clone(),
                    should_collect_region_watermark,
                ))
            }
            _ => {
                // merge into a single partition
                let merged_plan = CoalescePartitionsExec::new(plan.clone());
                // CoalescePartitionsExec must produce a single partition
                assert_eq!(
                    1,
                    merged_plan
                        .properties()
                        .output_partitioning()
                        .partition_count()
                );
                let df_stream = merged_plan.execute(0, task_ctx)?;
                let mut stream = RecordBatchStreamAdapter::try_new_with_span(df_stream, span)
                    .context(error::ConvertDfRecordBatchStreamSnafu)
                    .map_err(BoxedError::new)
                    .context(QueryExecutionSnafu)?;
                stream.set_metrics2(plan.clone());
                stream.set_query_load_region_id(query_load_region_id(plan));
                stream.set_query_stat_counters(query_stat_counters(plan));
                stream.set_explain_verbose(explain_verbose);
                let stream = OnDone::new(Box::pin(stream), move || {
                    let exec_cost = exec_timer.stop_and_record();
                    if explain_verbose {
                        common_telemetry::info!(
                            "DatafusionQueryEngine execute {output_partitions} stream, cost: {:?}s",
                            exec_cost
                        );
                    }
                });
                Ok(maybe_attach_region_watermark_metrics(
                    Box::pin(stream),
                    plan.clone(),
                    should_collect_region_watermark,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use api::v1::SemanticType;
    use arrow::array::{ArrayRef, StringArray, UInt64Array};
    use arrow_schema::SortOptions;
    use async_trait::async_trait;
    use catalog::RegisterTableRequest;
    use catalog::information_schema::{
        DatanodeInspectRequest, InformationExtension, InformationExtensionRef,
    };
    use catalog::kvbackend::KvBackendCatalogManagerBuilder;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, NUMBERS_TABLE_ID};
    use common_error::ext::BoxedError;
    use common_meta::cache::{
        CacheRegistryBuilder, LayeredCacheRegistryBuilder, new_table_route_cache,
    };
    use common_meta::cluster::NodeInfo;
    use common_meta::datanode::{RegionManifestInfo, RegionStat};
    use common_meta::key::flow::flow_state::FlowStat;
    use common_meta::key::table_route::{TableRouteManager, TableRouteValue};
    use common_meta::kv_backend::TxnService;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_procedure::ProcedureInfo;
    use common_recordbatch::{
        EmptyRecordBatchStream, RecordBatch, SendableRecordBatchStream, util,
    };
    use datafusion::datasource::DefaultTableSource;
    use datafusion::physical_plan::display::{DisplayAs, DisplayFormatType};
    use datafusion::physical_plan::expressions::PhysicalSortExpr;
    use datafusion::physical_plan::joins::{HashJoinExec, JoinOn, PartitionMode};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
    use datafusion::prelude::{col, lit};
    use datafusion_common::{JoinType, NullEquality, ScalarValue};
    use datafusion_expr::LogicalPlanBuilder;
    use datafusion_physical_expr::expressions::Column;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
    use datatypes::vectors::{Helper, UInt32Vector, VectorRef};
    use partition::cache::new_partition_info_cache;
    use session::context::{QueryContext, QueryContextBuilder};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::region_engine::{
        PartitionRange, PrepareRequest, QueryScanContext, RegionRole, RegionScanner,
        ScannerProperties,
    };
    use store_api::storage::{RegionId, ScanRequest};
    use table::metadata::{TableId, TableInfoBuilder, TableMetaBuilder};
    use table::table::adapter::DfTableProviderAdapter;
    use table::table::numbers::{NUMBERS_TABLE_NAME, NumbersTable};
    use table::table::scan::RegionScanExec;
    use table::test_util::EmptyTable;
    use table::test_util::table_info::test_table_info;

    use super::*;
    use crate::options::QueryOptions;
    use crate::parser::{QueryLanguageParser, QueryStatement};
    use crate::part_sort::PartSortExec;
    use crate::query_engine::{QueryEngineFactory, QueryEngineRef};

    #[derive(Debug)]
    struct RecordingScanner {
        schema: SchemaRef,
        metadata: RegionMetadataRef,
        properties: ScannerProperties,
        update_calls: Arc<AtomicUsize>,
        last_filter_len: Arc<AtomicUsize>,
    }

    impl RecordingScanner {
        fn new(
            schema: SchemaRef,
            metadata: RegionMetadataRef,
            update_calls: Arc<AtomicUsize>,
            last_filter_len: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                schema,
                metadata,
                properties: ScannerProperties::default(),
                update_calls,
                last_filter_len,
            }
        }
    }

    impl RegionScanner for RecordingScanner {
        fn name(&self) -> &str {
            "RecordingScanner"
        }

        fn properties(&self) -> &ScannerProperties {
            &self.properties
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn metadata(&self) -> RegionMetadataRef {
            self.metadata.clone()
        }

        fn prepare(&mut self, request: PrepareRequest) -> std::result::Result<(), BoxedError> {
            self.properties.prepare(request);
            Ok(())
        }

        fn scan_partition(
            &self,
            _ctx: &QueryScanContext,
            _metrics_set: &ExecutionPlanMetricsSet,
            _partition: usize,
        ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
            Ok(Box::pin(EmptyRecordBatchStream::new(self.schema.clone())))
        }

        fn has_predicate_without_region(&self) -> bool {
            true
        }

        fn add_dyn_filter_to_predicate(
            &mut self,
            filter_exprs: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Vec<bool> {
            self.update_calls.fetch_add(1, Ordering::Relaxed);
            self.last_filter_len
                .store(filter_exprs.len(), Ordering::Relaxed);
            vec![true; filter_exprs.len()]
        }

        fn set_logical_region(&mut self, logical_region: bool) {
            self.properties.set_logical_region(logical_region);
        }

        fn set_query_load_region_id(&mut self, region_id: store_api::storage::RegionId) {
            self.properties.set_query_load_region_id(region_id);
        }
    }

    impl DisplayAs for RecordingScanner {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "RecordingScanner")
        }
    }

    fn build_query_load_region_scan(
        query_load_region_id: Option<RegionId>,
    ) -> Arc<dyn ExecutionPlan> {
        build_region_scan(query_load_region_id, None)
    }

    fn build_query_stat_counter_region_scan(
        counters: RegionQueryStatCounters,
    ) -> Arc<dyn ExecutionPlan> {
        build_region_scan(None, Some(counters))
    }

    fn build_region_scan(
        query_load_region_id: Option<RegionId>,
        query_stat_counters: Option<RegionQueryStatCounters>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(datatypes::schema::Schema::new(vec![ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )]));

        let mut metadata_builder = RegionMetadataBuilder::new(RegionId::new(1024, 1));
        metadata_builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .primary_key(vec![]);
        let metadata = Arc::new(metadata_builder.build().unwrap());
        let mut scanner = RecordingScanner::new(
            schema,
            metadata,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        if let Some(region_id) = query_load_region_id {
            scanner.set_query_load_region_id(region_id);
        }
        if let Some(counters) = query_stat_counters {
            scanner.properties.set_query_stat_counters(counters);
        }

        Arc::new(RegionScanExec::new(Box::new(scanner), ScanRequest::default(), None).unwrap())
    }

    fn query_stat_counters_for_test() -> RegionQueryStatCounters {
        RegionQueryStatCounters {
            query_cpu_time: Arc::new(AtomicU64::new(0)),
            query_scanned_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    #[test]
    fn query_load_region_id_ignores_scans_without_region_id() {
        let query_load_region_id = RegionId::new(1024, 42);
        let scan_without_region_id = build_query_load_region_scan(None);
        let scan_with_region_id = build_query_load_region_scan(Some(query_load_region_id));
        let on: JoinOn = vec![(
            Arc::new(Column::new("ts", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("ts", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                scan_without_region_id,
                scan_with_region_id,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNull,
                false,
            )
            .unwrap(),
        );

        assert_eq!(
            super::query_load_region_id(&plan),
            Some(query_load_region_id.as_u64())
        );
    }

    #[test]
    fn query_stat_counters_returns_shared_counter_for_multi_scan_plan() {
        let counters = query_stat_counters_for_test();
        let left = build_query_stat_counter_region_scan(counters.clone());
        let right = build_query_stat_counter_region_scan(counters.clone());
        let on: JoinOn = vec![(
            Arc::new(Column::new("ts", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("ts", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNull,
                false,
            )
            .unwrap(),
        );

        let actual = super::query_stat_counters(&plan).unwrap();
        assert!(Arc::ptr_eq(
            &actual.query_cpu_time,
            &counters.query_cpu_time
        ));
        assert!(Arc::ptr_eq(
            &actual.query_scanned_bytes,
            &counters.query_scanned_bytes
        ));
    }

    #[test]
    fn query_stat_counters_ignores_mixed_counter_plan() {
        let left = build_query_stat_counter_region_scan(query_stat_counters_for_test());
        let right = build_query_stat_counter_region_scan(query_stat_counters_for_test());
        let on: JoinOn = vec![(
            Arc::new(Column::new("ts", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("ts", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNull,
                false,
            )
            .unwrap(),
        );

        assert!(super::query_stat_counters(&plan).is_none());
    }

    async fn create_test_engine() -> QueryEngineRef {
        let catalog_manager = catalog::memory::new_memory_catalog_manager().unwrap();
        let req = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: NUMBERS_TABLE_NAME.to_string(),
            table_id: NUMBERS_TABLE_ID,
            table: NumbersTable::table(NUMBERS_TABLE_ID),
        };
        catalog_manager.register_table_sync(req).unwrap();

        QueryEngineFactory::new(
            catalog_manager,
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        )
        .query_engine()
    }

    #[tokio::test]
    async fn test_sql_to_plan() {
        let engine = create_test_engine().await;
        let sql = "select sum(number) from numbers limit 20";

        let stmt = QueryLanguageParser::parse_sql(sql, &QueryContext::arc()).unwrap();
        let plan = engine
            .planner()
            .plan(&stmt, QueryContext::arc())
            .await
            .unwrap();

        assert_eq!(
            plan.to_string(),
            r#"Limit: skip=0, fetch=20
  Projection: sum(numbers.number)
    Aggregate: groupBy=[[]], aggr=[[sum(numbers.number)]]
      TableScan: numbers"#
        );
    }

    #[tokio::test]
    async fn test_purge_table_is_not_available_to_select() {
        let engine = create_test_engine().await;
        let stmt =
            QueryLanguageParser::parse_sql("select purge_table('numbers')", &QueryContext::arc())
                .unwrap();

        assert!(
            engine
                .planner()
                .plan(&stmt, QueryContext::arc())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_execute() {
        let engine = create_test_engine().await;
        let sql = "select sum(number) from numbers limit 20";

        let stmt = QueryLanguageParser::parse_sql(sql, &QueryContext::arc()).unwrap();
        let plan = engine
            .planner()
            .plan(&stmt, QueryContext::arc())
            .await
            .unwrap();

        let output = engine.execute(plan, QueryContext::arc()).await.unwrap();

        match output.data {
            OutputData::Stream(recordbatch) => {
                let numbers = util::collect(recordbatch).await.unwrap();
                assert_eq!(1, numbers.len());
                assert_eq!(numbers[0].num_columns(), 1);
                assert_eq!(1, numbers[0].schema.num_columns());
                assert_eq!(
                    "sum(numbers.number)",
                    numbers[0].schema.column_schemas()[0].name
                );

                let batch = &numbers[0];
                assert_eq!(1, batch.num_columns());
                assert_eq!(batch.column(0).len(), 1);

                let expected = Arc::new(UInt64Array::from_iter_values([4950])) as ArrayRef;
                assert_eq!(batch.column(0), &expected);
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_read_table() {
        let engine = create_test_engine().await;

        let engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();
        let query_ctx = Arc::new(QueryContextBuilder::default().build());
        let table = engine
            .find_table(
                &ResolvedTableReference {
                    catalog: "greptime".into(),
                    schema: "public".into(),
                    table: "numbers".into(),
                },
                &query_ctx,
            )
            .await
            .unwrap();

        let df = engine.read_table(table).unwrap();
        let df = df
            .select_columns(&["number"])
            .unwrap()
            .filter(col("number").lt(lit(10)))
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(1, batches.len());
        let batch = &batches[0];

        assert_eq!(1, batch.num_columns());
        assert_eq!(batch.column(0).len(), 10);

        assert_eq!(
            Helper::try_into_vector(batch.column(0)).unwrap(),
            Arc::new(UInt32Vector::from_slice([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])) as VectorRef
        );
    }

    #[tokio::test]
    async fn test_describe() {
        let engine = create_test_engine().await;
        let sql = "select sum(number) from numbers limit 20";

        let stmt = QueryLanguageParser::parse_sql(sql, &QueryContext::arc()).unwrap();

        let plan = engine
            .planner()
            .plan(&stmt, QueryContext::arc())
            .await
            .unwrap();

        let DescribeResult { logical_plan } =
            engine.describe(plan, QueryContext::arc()).await.unwrap();

        let schema: Schema = logical_plan.schema().clone().try_into().unwrap();

        assert_eq!(
            schema.column_schemas()[0],
            ColumnSchema::new(
                "sum(numbers.number)",
                ConcreteDataType::uint64_datatype(),
                true
            )
        );
        assert_eq!(
            "Limit: skip=0, fetch=20\n  Projection: sum(numbers.number)\n    Aggregate: groupBy=[[]], aggr=[[sum(numbers.number)]]\n      TableScan: numbers",
            format!("{}", logical_plan.display_indent())
        );
    }

    #[tokio::test]
    async fn test_topk_dynamic_filter_pushdown_reaches_region_scan() {
        let engine = create_test_engine().await;
        let engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();
        let engine_ctx = engine.engine_context(QueryContext::arc());
        let state = engine_ctx.state();

        let schema = Arc::new(datatypes::schema::Schema::new(vec![ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )]));

        let mut metadata_builder = RegionMetadataBuilder::new(RegionId::new(1024, 1));
        metadata_builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .primary_key(vec![]);
        let metadata = Arc::new(metadata_builder.build().unwrap());

        let update_calls = Arc::new(AtomicUsize::new(0));
        let last_filter_len = Arc::new(AtomicUsize::new(0));
        let scanner = Box::new(RecordingScanner::new(
            schema,
            metadata,
            update_calls.clone(),
            last_filter_len.clone(),
        ));
        let scan = Arc::new(RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap());

        let sort_expr = PhysicalSortExpr {
            expr: Arc::new(Column::new("ts", 0)),
            options: SortOptions {
                descending: true,
                ..Default::default()
            },
        };
        let partition_ranges: Vec<Vec<PartitionRange>> = vec![vec![]];
        let mut plan: Arc<dyn ExecutionPlan> =
            Arc::new(PartSortExec::try_new(sort_expr, Some(3), partition_ranges, scan).unwrap());

        for optimizer in state.physical_optimizers() {
            plan = optimizer.optimize(plan, state.config_options()).unwrap();
        }

        assert!(update_calls.load(Ordering::Relaxed) > 0);
        assert!(last_filter_len.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn test_join_dynamic_filter_pushdown_reaches_region_scan() {
        let engine = create_test_engine().await;
        let engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();
        let engine_ctx = engine.engine_context(QueryContext::arc());
        let state = engine_ctx.state();

        assert!(
            state
                .config_options()
                .optimizer
                .enable_join_dynamic_filter_pushdown
        );

        let schema = Arc::new(datatypes::schema::Schema::new(vec![ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )]));

        let mut left_metadata_builder = RegionMetadataBuilder::new(RegionId::new(2048, 1));
        left_metadata_builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .primary_key(vec![]);
        let left_metadata = Arc::new(left_metadata_builder.build().unwrap());

        let mut right_metadata_builder = RegionMetadataBuilder::new(RegionId::new(2048, 2));
        right_metadata_builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .primary_key(vec![]);
        let right_metadata = Arc::new(right_metadata_builder.build().unwrap());

        let left_update_calls = Arc::new(AtomicUsize::new(0));
        let left_last_filter_len = Arc::new(AtomicUsize::new(0));
        let right_update_calls = Arc::new(AtomicUsize::new(0));
        let right_last_filter_len = Arc::new(AtomicUsize::new(0));

        let left_scan = Arc::new(
            RegionScanExec::new(
                Box::new(RecordingScanner::new(
                    schema.clone(),
                    left_metadata,
                    left_update_calls.clone(),
                    left_last_filter_len.clone(),
                )),
                ScanRequest::default(),
                None,
            )
            .unwrap(),
        );
        let right_scan = Arc::new(
            RegionScanExec::new(
                Box::new(RecordingScanner::new(
                    schema,
                    right_metadata,
                    right_update_calls.clone(),
                    right_last_filter_len.clone(),
                )),
                ScanRequest::default(),
                None,
            )
            .unwrap(),
        );

        let on: JoinOn = vec![(
            Arc::new(Column::new("ts", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("ts", 0)) as Arc<dyn PhysicalExpr>,
        )];

        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                left_scan,
                right_scan,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNull,
                false,
            )
            .unwrap(),
        );

        for optimizer in state.physical_optimizers() {
            plan = optimizer.optimize(plan, state.config_options()).unwrap();
        }

        assert!(left_update_calls.load(Ordering::Relaxed) > 0);
        assert_eq!(0, left_last_filter_len.load(Ordering::Relaxed));
        assert!(right_update_calls.load(Ordering::Relaxed) > 0);
        assert!(right_last_filter_len.load(Ordering::Relaxed) > 0);
    }
    #[derive(Default)]
    struct RecordingMutationHandler {
        inserts: std::sync::Mutex<Vec<table::requests::InsertRequest>>,
    }

    #[async_trait]
    impl common_function::handlers::TableMutationHandler for RecordingMutationHandler {
        async fn insert(
            &self,
            request: table::requests::InsertRequest,
            _ctx: session::context::QueryContextRef,
        ) -> common_query::error::Result<common_query::Output> {
            self.inserts.lock().unwrap().push(request);
            Ok(common_query::Output::new_with_affected_rows(1))
        }

        async fn delete(
            &self,
            _request: table::requests::DeleteRequest,
            _ctx: session::context::QueryContextRef,
        ) -> common_query::error::Result<common_base::AffectedRows> {
            unimplemented!("unexpected delete")
        }

        async fn flush(
            &self,
            _request: table::requests::FlushTableRequest,
            _ctx: session::context::QueryContextRef,
        ) -> common_query::error::Result<common_base::AffectedRows> {
            unimplemented!("unexpected flush")
        }

        async fn compact(
            &self,
            _request: table::requests::CompactTableRequest,
            _ctx: session::context::QueryContextRef,
        ) -> common_query::error::Result<common_base::AffectedRows> {
            unimplemented!("unexpected compact")
        }

        async fn build_index(
            &self,
            _request: table::requests::BuildIndexTableRequest,
            _ctx: session::context::QueryContextRef,
        ) -> common_query::error::Result<common_base::AffectedRows> {
            unimplemented!("unexpected build_index")
        }

        async fn flush_region(
            &self,
            _region_id: store_api::storage::RegionId,
            _ctx: session::context::QueryContextRef,
        ) -> common_query::error::Result<common_base::AffectedRows> {
            unimplemented!("unexpected flush_region")
        }

        async fn compact_region(
            &self,
            _region_id: store_api::storage::RegionId,
            _ctx: session::context::QueryContextRef,
        ) -> common_query::error::Result<common_base::AffectedRows> {
            unimplemented!("unexpected compact_region")
        }

        async fn discard_unflushed_data(
            &self,
            _region_id: store_api::storage::RegionId,
            _ctx: session::context::QueryContextRef,
        ) -> common_query::error::Result<common_base::AffectedRows> {
            unimplemented!("unexpected discard_unflushed_data")
        }

        async fn discard_unflushed_data_by_table(
            &self,
            _table_name: table::table_name::TableName,
            _ctx: session::context::QueryContextRef,
        ) -> common_query::error::Result<common_base::AffectedRows> {
            unimplemented!("unexpected discard_unflushed_data_by_table")
        }
    }

    fn native_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            ColumnSchema::new("dim", ConcreteDataType::date_datatype(), true),
            ColumnSchema::new("amount", ConcreteDataType::decimal128_datatype(30, 2), true),
            ColumnSchema::new(
                "elapsed",
                ConcreteDataType::duration_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(
                "updated_at",
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new("marker", ConcreteDataType::uint8_datatype(), true),
            ColumnSchema::new("payload", ConcreteDataType::binary_datatype(), true),
            ColumnSchema::new("epoch", ConcreteDataType::uint64_datatype(), true),
        ]))
    }

    fn register_native_tables(catalog: &catalog::memory::MemoryCatalogManager) {
        let schema = native_schema();
        let meta = TableMetaBuilder::empty()
            .schema(schema.clone())
            .primary_key_indices(vec![])
            .value_indices((0..schema.num_columns()).collect())
            .next_column_id(8)
            .build()
            .unwrap();
        let info = TableInfoBuilder::default()
            .name("native_regression")
            .table_id(9001)
            .table_version(0)
            .meta(meta)
            .build()
            .unwrap();
        catalog
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "native_regression".to_string(),
                table_id: 9001,
                table: table::test_util::EmptyTable::from_table_info(&info),
            })
            .unwrap();

        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("dim", ConcreteDataType::date_datatype(), true),
            ColumnSchema::new("amount", ConcreteDataType::decimal128_datatype(20, 2), true),
            ColumnSchema::new(
                "elapsed",
                ConcreteDataType::duration_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        ]));
        let rows = RecordBatch::new(
            schema,
            vec![
                Arc::new(datatypes::vectors::DateVector::from_slice([0, 2])) as VectorRef,
                Arc::new(
                    datatypes::vectors::Decimal128Vector::from_slice([10000, 20000])
                        .with_precision_and_scale(20, 2)
                        .unwrap(),
                ) as VectorRef,
                Arc::new(datatypes::vectors::DurationMillisecondVector::from_values(
                    [10, 20],
                )) as VectorRef,
                Arc::new(datatypes::vectors::TimestampMillisecondVector::from_slice(
                    [1, 2],
                )) as VectorRef,
            ],
        )
        .unwrap();
        catalog
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "native_aggregate".to_string(),
                table_id: 9002,
                table: table::test_util::MemTable::table("native_aggregate", rows),
            })
            .unwrap();
    }

    async fn run_sql(engine: &QueryEngineRef, sql: &str) -> Vec<RecordBatch> {
        let stmt = QueryLanguageParser::parse_sql(sql, &QueryContext::arc()).unwrap();
        let plan = engine
            .planner()
            .plan(&stmt, QueryContext::arc())
            .await
            .unwrap();
        match engine
            .execute(plan, QueryContext::arc())
            .await
            .unwrap()
            .data
        {
            OutputData::Stream(stream) => util::collect(stream).await.unwrap(),
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_native_executor_null_insert_and_aggregates() {
        let catalog = catalog::memory::new_memory_catalog_manager().unwrap();
        register_native_tables(&catalog);
        let handler = Arc::new(RecordingMutationHandler::default());
        let engine = QueryEngineFactory::new(
            catalog,
            None,
            Some(handler.clone()),
            None,
            None,
            false,
            QueryOptions::default(),
        )
        .query_engine();

        // The CASTs force Insert::can_extract_values false. This records the
        // QueryEngine DML path selected by operator/src/statement/dml.rs.
        let insert_sql = "INSERT INTO native_regression (dim, amount, elapsed, ts, updated_at, marker, payload, epoch) VALUES (NULL, NULL, NULL, CAST(-62135596799999 AS TIMESTAMP(3)), CAST(NULL AS TIMESTAMP(3)), CAST(1 AS UInt8), X'0102', CAST(1 AS UInt64))";
        let stmt = QueryLanguageParser::parse_sql(insert_sql, &QueryContext::arc()).unwrap();
        let QueryStatement::Sql(sql::statements::statement::Statement::Insert(insert)) = &stmt
        else {
            unreachable!()
        };
        assert!(!insert.can_extract_values());
        let plan = engine
            .planner()
            .plan(&stmt, QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(
            engine
                .execute(plan, QueryContext::arc())
                .await
                .unwrap()
                .data,
            OutputData::AffectedRows(1)
        ));

        let request = handler.inserts.lock().unwrap().pop().unwrap();
        for (name, data_type) in [
            ("dim", ConcreteDataType::date_datatype()),
            ("amount", ConcreteDataType::decimal128_datatype(30, 2)),
            ("elapsed", ConcreteDataType::duration_millisecond_datatype()),
            (
                "updated_at",
                ConcreteDataType::timestamp_millisecond_datatype(),
            ),
        ] {
            let vector = request.columns_values.get(name).unwrap();
            assert_eq!(vector.len(), 1, "{name}");
            assert_eq!(vector.data_type(), data_type, "{name}");
            assert!(vector.is_null(0), "{name}");
        }
        assert!(!request.columns_values["ts"].is_null(0));
        for (name, data_type) in [
            ("marker", ConcreteDataType::uint8_datatype()),
            ("payload", ConcreteDataType::binary_datatype()),
            ("epoch", ConcreteDataType::uint64_datatype()),
        ] {
            assert_eq!(
                request.columns_values[name].data_type(),
                data_type,
                "{name}"
            );
        }

        let batches = run_sql(
            &engine,
            "SELECT MIN(dim), MAX(dim), SUM(amount), SUM(elapsed) FROM native_aggregate",
        )
        .await;
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        for (index, (data_type, value)) in [
            (
                ConcreteDataType::date_datatype(),
                ScalarValue::Date32(Some(0)),
            ),
            (
                ConcreteDataType::date_datatype(),
                ScalarValue::Date32(Some(2)),
            ),
            (
                ConcreteDataType::decimal128_datatype(30, 2),
                ScalarValue::Decimal128(Some(30000), 30, 2),
            ),
            (
                ConcreteDataType::duration_millisecond_datatype(),
                ScalarValue::DurationMillisecond(Some(30)),
            ),
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(batch.schema.column_schemas()[index].data_type, data_type);
            assert_eq!(
                ScalarValue::try_from_array(batch.column(index).as_ref(), 0).unwrap(),
                value
            );
        }

        let batches = run_sql(
            &engine,
            "SELECT aggregate.amount_sum + aggregate.amount_sum AS amount_add, \
                    aggregate.elapsed_sum + aggregate.elapsed_sum AS elapsed_add, \
                    CASE WHEN aggregate.min_dim < CAST('1970-01-02' AS DATE) THEN true ELSE false END AS min_before, \
                    CASE WHEN aggregate.max_dim > CAST('1970-01-01' AS DATE) THEN true ELSE false END AS max_after \
             FROM (SELECT MIN(dim) AS min_dim, MAX(dim) AS max_dim, SUM(amount) AS amount_sum, SUM(elapsed) AS elapsed_sum \
                   FROM native_aggregate) AS aggregate",
        )
        .await;
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        for (index, (data_type, value)) in [
            (
                ConcreteDataType::decimal128_datatype(31, 2),
                ScalarValue::Decimal128(Some(60000), 31, 2),
            ),
            (
                ConcreteDataType::duration_millisecond_datatype(),
                ScalarValue::DurationMillisecond(Some(60)),
            ),
            (
                ConcreteDataType::boolean_datatype(),
                ScalarValue::Boolean(Some(true)),
            ),
            (
                ConcreteDataType::boolean_datatype(),
                ScalarValue::Boolean(Some(true)),
            ),
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(batch.schema.column_schemas()[index].data_type, data_type);
            assert_eq!(
                ScalarValue::try_from_array(batch.column(index).as_ref(), 0).unwrap(),
                value
            );
        }
    }

    /// The table ids of the two tables of the join of the automatic tests.
    const PROBE_TABLE_ID: TableId = 1024;
    const BUILD_TABLE_ID: TableId = 1025;

    /// The physical table id a logical candidate table resolves to in the route gating test.
    const LOGICAL_PHYSICAL_TABLE_ID: TableId = 1026;

    /// The table id of the table an aliased self-join joins with itself.
    const SELF_JOIN_TABLE_ID: TableId = 1027;

    /// An information extension with fixed region statistics and a counter of its `region_stats`
    /// calls.
    #[derive(Debug)]
    struct TestInformationExtension {
        reports: Vec<RegionStat>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl InformationExtension for TestInformationExtension {
        type Error = catalog::error::Error;

        async fn nodes(&self) -> std::result::Result<Vec<NodeInfo>, Self::Error> {
            Ok(vec![])
        }

        async fn procedures(
            &self,
        ) -> std::result::Result<Vec<(String, ProcedureInfo)>, Self::Error> {
            Ok(vec![])
        }

        async fn region_stats(&self) -> std::result::Result<Vec<RegionStat>, Self::Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.reports.clone())
        }

        async fn flow_stats(&self) -> std::result::Result<Option<FlowStat>, Self::Error> {
            Ok(None)
        }

        async fn inspect_datanode(
            &self,
            _request: DatanodeInspectRequest,
        ) -> std::result::Result<SendableRecordBatchStream, Self::Error> {
            Ok(common_recordbatch::RecordBatches::empty().as_stream())
        }
    }

    /// A region report with `bytes` approximate disk bytes and `role`.
    fn region_stat(region_id: RegionId, bytes: u64, role: RegionRole) -> RegionStat {
        RegionStat {
            id: region_id,
            rcus: 0,
            wcus: 0,
            approximate_bytes: bytes,
            engine: "mito".to_string(),
            role,
            num_rows: 0,
            memtable_size: 0,
            manifest_size: 0,
            sst_size: 0,
            sst_num: 0,
            index_size: 0,
            region_manifest: RegionManifestInfo::Mito {
                manifest_version: 0,
                flushed_entry_id: 0,
                file_removed_cnt: 0,
            },
            written_bytes: 0,
            query_cpu_time: 0,
            query_scanned_bytes: 0,
            data_topic_latest_entry_id: 0,
            metadata_topic_latest_entry_id: 0,
            min_timestamp: None,
            max_timestamp: None,
        }
    }

    /// The region statistics of a complete query: the probe table has two regions of 4_000 and
    /// 6_000 bytes, the build table one region of 1_000 bytes.
    fn dist_join_reports() -> Vec<RegionStat> {
        vec![
            region_stat(RegionId::new(PROBE_TABLE_ID, 1), 4_000, RegionRole::Leader),
            region_stat(RegionId::new(PROBE_TABLE_ID, 2), 6_000, RegionRole::Leader),
            region_stat(RegionId::new(BUILD_TABLE_ID, 1), 1_000, RegionRole::Leader),
        ]
    }

    /// Two-column schema (`number`, `host`) of the tables of the join of the tests.
    fn dist_join_test_schema() -> SchemaRef {
        let schema = SchemaBuilder::try_from_columns(vec![
            ColumnSchema::new("number", ConcreteDataType::uint32_datatype(), true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
        ])
        .unwrap()
        .build()
        .unwrap();
        Arc::new(schema)
    }

    /// Base table `name` with `table_id`, backed by an empty table provider.
    fn dist_join_test_table(table_id: TableId, name: &str) -> table::TableRef {
        let info = test_table_info(
            table_id,
            name,
            DEFAULT_SCHEMA_NAME,
            DEFAULT_CATALOG_NAME,
            dist_join_test_schema(),
        );
        EmptyTable::from_table_info(&info)
    }

    /// A scan of one of the tables of the join of the tests.
    fn dist_join_test_scan(alias: &str, table_id: TableId, name: &str) -> LogicalPlan {
        let source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(dist_join_test_table(table_id, name)),
        )));
        LogicalPlanBuilder::scan_with_filters(alias, source, None, vec![])
            .unwrap()
            .build()
            .unwrap()
    }

    /// `probe INNER JOIN build ON probe.number = build.number`: the shape `DistPlannerAnalyzer`
    /// wraps in `MergeScan` and `DistJoinPlanner` can then nest.
    fn dist_join_test_plan() -> LogicalPlan {
        LogicalPlanBuilder::from(dist_join_test_scan("probe", PROBE_TABLE_ID, "probe"))
            .join_on(
                dist_join_test_scan("build", BUILD_TABLE_ID, "build"),
                JoinType::Inner,
                vec![col("probe.number").eq(col("build.number"))],
            )
            .unwrap()
            .build()
            .unwrap()
    }

    /// The shape of [`dist_join_test_plan`] with the given tables under the aliases `a` and `b`:
    /// an aliased self-join passes the same table id twice.
    fn dist_join_aliased_test_plan(left: (TableId, &str), right: (TableId, &str)) -> LogicalPlan {
        LogicalPlanBuilder::from(dist_join_test_scan("a", left.0, left.1))
            .join_on(
                dist_join_test_scan("b", right.0, right.1),
                JoinType::Inner,
                vec![col("a.number").eq(col("b.number"))],
            )
            .unwrap()
            .build()
            .unwrap()
    }

    /// The statistics the engine derives from [`dist_join_reports`] and [`dist_join_test_routes`]:
    /// the probe table with 10_000 bytes in two regions and the build table with 1_000 bytes in
    /// one.
    fn dist_join_test_stats() -> BTreeMap<TableId, crate::dist_plan::DistJoinTableStats> {
        BTreeMap::from([
            (
                PROBE_TABLE_ID,
                crate::dist_plan::DistJoinTableStats {
                    total_bytes: 10_000,
                    region_count: 2,
                },
            ),
            (
                BUILD_TABLE_ID,
                crate::dist_plan::DistJoinTableStats {
                    total_bytes: 1_000,
                    region_count: 1,
                },
            ),
        ])
    }

    /// A query context with or without the session opt-in of the rewrite.
    fn dist_join_query_ctx(opt_in: bool) -> QueryContextRef {
        let query_ctx = QueryContextBuilder::default().build();
        query_ctx
            .configuration_parameter()
            .set_experimental_dist_join(opt_in);
        Arc::new(query_ctx)
    }

    /// The physical route of the table `table_id` with the given region numbers.
    fn dist_join_physical_route(
        table_id: TableId,
        region_numbers: &[u32],
    ) -> (TableId, TableRouteValue) {
        let routes = region_numbers
            .iter()
            .map(|region_number| RegionRoute {
                region: Region {
                    id: RegionId::new(table_id, *region_number),
                    ..Default::default()
                },
                ..Default::default()
            })
            .collect();
        (table_id, TableRouteValue::physical(routes))
    }

    /// The physical routes of the tables of the join of the tests: the probe table has the regions
    /// 1 and 2, the build table the region 1.
    fn dist_join_test_routes() -> Vec<(TableId, TableRouteValue)> {
        vec![
            dist_join_physical_route(PROBE_TABLE_ID, &[1, 2]),
            dist_join_physical_route(BUILD_TABLE_ID, &[1]),
        ]
    }

    /// A catalog manager whose KV backend holds the given table routes. Its region statistics are
    /// the given reports.
    async fn dist_join_catalog_manager(
        routes: Vec<(TableId, TableRouteValue)>,
        reports: Vec<RegionStat>,
        calls: Arc<AtomicUsize>,
    ) -> Arc<KvBackendCatalogManager> {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let table_route_manager = TableRouteManager::new(kv_backend.clone());
        for (table_id, table_route) in routes {
            let (txn, _) = table_route_manager
                .table_route_storage()
                .build_create_txn(table_id, &table_route)
                .unwrap();
            assert!(kv_backend.txn(txn).await.unwrap().succeeded);
        }

        let table_route_cache = Arc::new(new_table_route_cache(
            "test_table_route".to_string(),
            moka::future::Cache::new(16),
            kv_backend.clone(),
        ));
        let partition_info_cache = Arc::new(new_partition_info_cache(
            "test_partition_info".to_string(),
            moka::future::Cache::new(16),
            table_route_cache.clone(),
        ));
        let cache_registry = LayeredCacheRegistryBuilder::default()
            .add_cache_registry(
                CacheRegistryBuilder::default()
                    .add_cache(table_route_cache)
                    .add_cache(partition_info_cache)
                    .build(),
            )
            .build();
        let information_extension: InformationExtensionRef =
            Arc::new(TestInformationExtension { reports, calls });

        KvBackendCatalogManagerBuilder::new(
            information_extension,
            kv_backend,
            Arc::new(cache_registry),
        )
        .build()
    }

    /// A query engine over the catalog manager of the tests, with the distributed rules when
    /// `with_dist_planner` is set.
    async fn dist_join_engine(
        reports: Vec<RegionStat>,
        calls: Arc<AtomicUsize>,
        with_dist_planner: bool,
    ) -> QueryEngineRef {
        dist_join_engine_with_routes(dist_join_test_routes(), reports, calls, with_dist_planner)
            .await
    }

    /// A query engine over a catalog manager with the given table `routes`.
    async fn dist_join_engine_with_routes(
        routes: Vec<(TableId, TableRouteValue)>,
        reports: Vec<RegionStat>,
        calls: Arc<AtomicUsize>,
        with_dist_planner: bool,
    ) -> QueryEngineRef {
        let catalog_manager = dist_join_catalog_manager(routes, reports, calls).await;
        QueryEngineFactory::new_with_plugins(
            catalog_manager.clone(),
            Some(catalog_manager.partition_manager()),
            None,
            None,
            None,
            None,
            with_dist_planner,
            Plugins::new(),
            QueryOptions::default(),
        )
        .query_engine()
    }

    /// The optimized logical plan of the EXPLAIN output of `plan`, as the query engine produces
    /// it: `DatafusionQueryEngine::create_physical_plan` handles EXPLAIN, whose input plan is
    /// analyzed and optimized (i.e. the distributed rules including `DistJoinPlanner` run on it)
    /// before its stages are printed.
    async fn explain_logical_plan(
        engine: &DatafusionQueryEngine,
        ctx: &mut QueryEngineContext,
        plan: &LogicalPlan,
    ) -> String {
        let explain_plan = LogicalPlanBuilder::from(plan.clone())
            .explain(false, false)
            .unwrap()
            .build()
            .unwrap();
        let physical = engine
            .create_physical_plan(ctx, &explain_plan)
            .await
            .unwrap();
        let batches = datafusion::physical_plan::collect(physical, ctx.build_task_ctx())
            .await
            .unwrap();

        for batch in &batches {
            let plan_types = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let plans = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                if plan_types.value(row) == "logical_plan" {
                    return plans.value(row).to_string();
                }
            }
        }

        panic!("the EXPLAIN output has no logical plan: {batches:?}");
    }

    /// Whether an EXPLAIN of a join shows the nested rewrite: the join sits inside a `MergeScan`,
    /// so the enclosing boundary appears before the join in the plan text. The join of two
    /// boundaries, i.e. the plan without the rewrite, shows the join first.
    fn nests_join_in_merge_scan(plan: &str) -> bool {
        match (plan.find("Join:"), plan.find("MergeScan")) {
            (Some(join), Some(merge_scan)) => merge_scan < join,
            _ => false,
        }
    }

    /// The automatic path of the statistics of the nested broadcast join rewrite: the engine
    /// resolves the candidate tables to their physical routes, fetches the region statistics of
    /// the query once, and the EXPLAIN of the same query then shows the nested rewrite.
    #[tokio::test]
    async fn test_dist_join_stats_from_physical_routes() {
        common_telemetry::init_default_ut_logging();

        let calls = Arc::new(AtomicUsize::new(0));
        let engine = dist_join_engine(dist_join_reports(), calls.clone(), true).await;
        let query_engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();
        let mut ctx = query_engine.engine_context(dist_join_query_ctx(true));
        let plan = dist_join_test_plan();

        let stats = query_engine.dist_join_stats(&ctx, &plan).await.unwrap();
        assert_eq!(dist_join_test_stats(), stats.tables);
        assert!(stats.favors_right_build(PROBE_TABLE_ID, BUILD_TABLE_ID));
        assert_eq!(
            1,
            calls.load(Ordering::SeqCst),
            "the statistics of one query must come from one region stats call"
        );

        // The same query through the EXPLAIN path of the engine.
        let explained = explain_logical_plan(query_engine, &mut ctx, &plan).await;
        assert!(
            nests_join_in_merge_scan(&explained),
            "the EXPLAIN must show the nested rewrite, got:\n{explained}"
        );
        assert_eq!(2, calls.load(Ordering::SeqCst));
    }

    /// The gating of the automatic path: without the session opt-in, without the distributed
    /// rules, with the manual build table option or without a candidate join the engine does not
    /// fetch any statistics.
    #[tokio::test]
    async fn test_dist_join_stats_gating() {
        common_telemetry::init_default_ut_logging();

        // Without the session opt-in.
        let calls = Arc::new(AtomicUsize::new(0));
        let engine = dist_join_engine(dist_join_reports(), calls.clone(), true).await;
        let query_engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();
        let plan = dist_join_test_plan();
        let ctx = query_engine.engine_context(dist_join_query_ctx(false));
        assert!(query_engine.dist_join_stats(&ctx, &plan).await.is_none());
        assert_eq!(0, calls.load(Ordering::SeqCst));

        // Without the distributed rules.
        let calls = Arc::new(AtomicUsize::new(0));
        let engine = dist_join_engine(dist_join_reports(), calls.clone(), false).await;
        let query_engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();
        let ctx = query_engine.engine_context(dist_join_query_ctx(true));
        assert!(query_engine.dist_join_stats(&ctx, &plan).await.is_none());
        assert_eq!(0, calls.load(Ordering::SeqCst));

        // With the manual build table option, which selects the build side by name.
        let calls = Arc::new(AtomicUsize::new(0));
        let engine = dist_join_engine(dist_join_reports(), calls.clone(), true).await;
        let query_engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();
        let mut ctx = query_engine.engine_context(dist_join_query_ctx(true));
        ctx.state_mut()
            .config_mut()
            .options_mut()
            .extensions
            .insert(DistPlannerOptions {
                nested_broadcast_join_build_table: Some("build".to_string()),
                ..Default::default()
            });
        assert!(query_engine.dist_join_stats(&ctx, &plan).await.is_none());
        assert_eq!(0, calls.load(Ordering::SeqCst));

        // Without a candidate join.
        let ctx = query_engine.engine_context(dist_join_query_ctx(true));
        let scan_plan = dist_join_test_scan("probe", PROBE_TABLE_ID, "probe");
        assert!(
            query_engine
                .dist_join_stats(&ctx, &scan_plan)
                .await
                .is_none()
        );
        assert_eq!(0, calls.load(Ordering::SeqCst));

        // The same plan runs through the EXPLAIN path without the opt-in: the join keeps its two
        // boundaries.
        let mut ctx = query_engine.engine_context(dist_join_query_ctx(false));
        let explained = explain_logical_plan(query_engine, &mut ctx, &plan).await;
        assert!(
            !nests_join_in_merge_scan(&explained),
            "without the opt-in the join must keep its two MergeScan boundaries, got:\n{explained}"
        );
    }

    /// An incomplete description of a candidate table (here a follower report instead of the
    /// leader of the build table) leaves the table out of the statistics, so the EXPLAIN of the
    /// same query keeps the existing plan.
    #[tokio::test]
    async fn test_dist_join_stats_missing_description_keeps_plan() {
        common_telemetry::init_default_ut_logging();

        let calls = Arc::new(AtomicUsize::new(0));
        let mut reports = dist_join_reports();
        reports[2].role = RegionRole::Follower;
        let engine = dist_join_engine(reports, calls.clone(), true).await;
        let query_engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();
        let mut ctx = query_engine.engine_context(dist_join_query_ctx(true));
        let plan = dist_join_test_plan();

        let stats = query_engine.dist_join_stats(&ctx, &plan).await.unwrap();
        assert_eq!(1, stats.tables.len());
        assert!(!stats.favors_right_build(PROBE_TABLE_ID, BUILD_TABLE_ID));

        let explained = explain_logical_plan(query_engine, &mut ctx, &plan).await;
        assert!(
            !nests_join_in_merge_scan(&explained),
            "an incomplete description must keep the existing plan, got:\n{explained}"
        );
        assert_eq!(2, calls.load(Ordering::SeqCst));
    }

    /// A candidate table that resolves to another physical table id is not priced under the id the
    /// plan captured: the engine keeps the existing plan without fetching any statistics.
    #[tokio::test]
    async fn test_dist_join_stats_logical_route_keeps_plan() {
        common_telemetry::init_default_ut_logging();

        // The probe table is logical and resolves to the physical table LOGICAL_PHYSICAL_TABLE_ID,
        // whose regions the reports describe; the build table is a usable physical table. Pricing
        // the resolved regions under the captured probe id would make the pair usable, so the
        // engine must reject the resolution and fetch nothing.
        let calls = Arc::new(AtomicUsize::new(0));
        let routes = vec![
            (
                PROBE_TABLE_ID,
                TableRouteValue::logical(LOGICAL_PHYSICAL_TABLE_ID),
            ),
            dist_join_physical_route(LOGICAL_PHYSICAL_TABLE_ID, &[1, 2]),
            dist_join_physical_route(BUILD_TABLE_ID, &[1]),
        ];
        let reports = vec![
            region_stat(
                RegionId::new(LOGICAL_PHYSICAL_TABLE_ID, 1),
                4_000,
                RegionRole::Leader,
            ),
            region_stat(
                RegionId::new(LOGICAL_PHYSICAL_TABLE_ID, 2),
                6_000,
                RegionRole::Leader,
            ),
            region_stat(RegionId::new(BUILD_TABLE_ID, 1), 1_000, RegionRole::Leader),
        ];
        let engine = dist_join_engine_with_routes(routes, reports, calls.clone(), true).await;
        let query_engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();
        let ctx = query_engine.engine_context(dist_join_query_ctx(true));
        let plan = dist_join_test_plan();

        assert!(query_engine.dist_join_stats(&ctx, &plan).await.is_none());
        assert_eq!(0, calls.load(Ordering::SeqCst));

        // The logical probe is the only candidate table with a route: the pair is still unusable.
        let calls = Arc::new(AtomicUsize::new(0));
        let routes = vec![
            (
                PROBE_TABLE_ID,
                TableRouteValue::logical(LOGICAL_PHYSICAL_TABLE_ID),
            ),
            dist_join_physical_route(LOGICAL_PHYSICAL_TABLE_ID, &[1, 2]),
        ];
        let engine = dist_join_engine_with_routes(
            routes,
            vec![
                region_stat(
                    RegionId::new(LOGICAL_PHYSICAL_TABLE_ID, 1),
                    4_000,
                    RegionRole::Leader,
                ),
                region_stat(
                    RegionId::new(LOGICAL_PHYSICAL_TABLE_ID, 2),
                    6_000,
                    RegionRole::Leader,
                ),
            ],
            calls.clone(),
            true,
        )
        .await;
        let query_engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();

        assert!(query_engine.dist_join_stats(&ctx, &plan).await.is_none());
        assert_eq!(0, calls.load(Ordering::SeqCst));
    }

    /// A candidate pair with a missing or empty build route is left alone: the engine keeps the
    /// existing plan without fetching any statistics.
    #[tokio::test]
    async fn test_dist_join_stats_unusable_route_keeps_plan() {
        common_telemetry::init_default_ut_logging();

        // The build table has no route.
        let calls = Arc::new(AtomicUsize::new(0));
        let routes = vec![dist_join_physical_route(PROBE_TABLE_ID, &[1, 2])];
        let engine =
            dist_join_engine_with_routes(routes, dist_join_reports(), calls.clone(), true).await;
        let query_engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();
        let ctx = query_engine.engine_context(dist_join_query_ctx(true));
        let plan = dist_join_test_plan();

        assert!(query_engine.dist_join_stats(&ctx, &plan).await.is_none());
        assert_eq!(0, calls.load(Ordering::SeqCst));

        // The build table route has no region.
        let calls = Arc::new(AtomicUsize::new(0));
        let routes = vec![
            dist_join_physical_route(PROBE_TABLE_ID, &[1, 2]),
            dist_join_physical_route(BUILD_TABLE_ID, &[]),
        ];
        let engine =
            dist_join_engine_with_routes(routes, dist_join_reports(), calls.clone(), true).await;
        let query_engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();
        let ctx = query_engine.engine_context(dist_join_query_ctx(true));

        assert!(query_engine.dist_join_stats(&ctx, &plan).await.is_none());
        assert_eq!(0, calls.load(Ordering::SeqCst));
    }

    /// An aliased self-join prices one table against itself, which can never satisfy
    /// `N * B < B`: the engine resolves no route and fetches no statistics for it, and the
    /// EXPLAIN of the same query keeps the existing plan.
    #[tokio::test]
    async fn test_dist_join_stats_self_join_keeps_plan() {
        common_telemetry::init_default_ut_logging();

        let calls = Arc::new(AtomicUsize::new(0));
        let engine = dist_join_engine(dist_join_reports(), calls.clone(), true).await;
        let query_engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();
        let mut ctx = query_engine.engine_context(dist_join_query_ctx(true));
        let plan =
            dist_join_aliased_test_plan((PROBE_TABLE_ID, "probe"), (PROBE_TABLE_ID, "probe"));

        assert!(query_engine.dist_join_stats(&ctx, &plan).await.is_none());
        assert_eq!(
            0,
            calls.load(Ordering::SeqCst),
            "a self-join must not fetch any statistics"
        );

        let explained = explain_logical_plan(query_engine, &mut ctx, &plan).await;
        assert!(
            !nests_join_in_merge_scan(&explained),
            "the self-join must keep its two MergeScan boundaries, got:\n{explained}"
        );
        assert_eq!(0, calls.load(Ordering::SeqCst));
    }

    /// The aliases of the two sides do not change the candidate: the same shape over two tables is
    /// priced, the heuristic favors the build side, and the EXPLAIN shows the nested rewrite.
    #[tokio::test]
    async fn test_dist_join_stats_aliased_tables_stay_candidates() {
        common_telemetry::init_default_ut_logging();

        let calls = Arc::new(AtomicUsize::new(0));
        let engine = dist_join_engine(dist_join_reports(), calls.clone(), true).await;
        let query_engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();
        let mut ctx = query_engine.engine_context(dist_join_query_ctx(true));
        let plan =
            dist_join_aliased_test_plan((PROBE_TABLE_ID, "probe"), (BUILD_TABLE_ID, "build"));

        let stats = query_engine.dist_join_stats(&ctx, &plan).await.unwrap();
        assert_eq!(dist_join_test_stats(), stats.tables);
        assert!(stats.favors_right_build(PROBE_TABLE_ID, BUILD_TABLE_ID));
        assert_eq!(1, calls.load(Ordering::SeqCst));

        let explained = explain_logical_plan(query_engine, &mut ctx, &plan).await;
        assert!(
            nests_join_in_merge_scan(&explained),
            "the EXPLAIN must show the nested rewrite, got:\n{explained}"
        );
        assert_eq!(2, calls.load(Ordering::SeqCst));
    }

    /// A rejected self-join does not stop the traversal of the plan: the legitimate join next to
    /// it is still priced, the self-join table gets no statistics, and the query still comes from
    /// one `region_stats` call.
    #[tokio::test]
    async fn test_dist_join_stats_self_join_next_to_candidate() {
        common_telemetry::init_default_ut_logging();

        let calls = Arc::new(AtomicUsize::new(0));
        let mut routes = dist_join_test_routes();
        routes.push(dist_join_physical_route(SELF_JOIN_TABLE_ID, &[1]));
        let mut reports = dist_join_reports();
        reports.push(region_stat(
            RegionId::new(SELF_JOIN_TABLE_ID, 1),
            2_000,
            RegionRole::Leader,
        ));
        let engine = dist_join_engine_with_routes(routes, reports, calls.clone(), true).await;
        let query_engine = engine
            .as_any()
            .downcast_ref::<DatafusionQueryEngine>()
            .unwrap();
        let ctx = query_engine.engine_context(dist_join_query_ctx(true));
        let self_join =
            dist_join_aliased_test_plan((SELF_JOIN_TABLE_ID, "logs"), (SELF_JOIN_TABLE_ID, "logs"));
        let plan = LogicalPlanBuilder::from(self_join)
            .join_on(
                dist_join_test_plan(),
                JoinType::Inner,
                vec![col("a.number").eq(col("probe.number"))],
            )
            .unwrap()
            .build()
            .unwrap();

        let stats = query_engine.dist_join_stats(&ctx, &plan).await.unwrap();
        assert_eq!(
            dist_join_test_stats(),
            stats.tables,
            "the rejected self-join must not be priced"
        );
        assert_eq!(1, calls.load(Ordering::SeqCst));
    }
}
