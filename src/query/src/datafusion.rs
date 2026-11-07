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
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
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
use datafusion_common::tree_node::TreeNodeRecursion;
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
use crate::dist_plan::{MergeScanLogicalPlan, RemoteDynFilterReceiverInjectorRef};
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
use crate::optimizer::expr_depth;
use crate::options::ScheduledTimeExtension;
use crate::physical_wrapper::PhysicalPlanWrapperRef;
use crate::planner::{DfLogicalPlanner, LogicalPlanner};
use crate::query_engine::{DescribeResult, QueryEngineContext, QueryEngineState};
use crate::scan_sequence_binding::{RegionSeqs, ScanSequenceBinding, lower_sequence_bindings};
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

/// Lowers the [`ScanSequenceBinding`]s of `plan` into the scans they wrap.
///
/// The scan upper bound `H` of every region is the per-region snapshot the query
/// was planned on, so the lowered request reads exactly the rows the plan
/// position was bound to.
///
/// A plan without a binding is returned unchanged: a query that the query layer
/// did not sequence-bind keeps its exact current plan and pays nothing but one
/// plan walk here. A binding that cannot be lowered (no bound, no snapshot, a
/// non-scan child, a non-region source) fails the query instead of degrading to
/// a full scan, because a full scan would return rows the caller excluded.
fn lower_scan_sequence_bindings(
    plan: LogicalPlan,
    query_ctx: &QueryContextRef,
) -> Result<LogicalPlan> {
    if !contains_scan_sequence_binding(&plan) {
        return Ok(plan);
    }

    let snapshot_seqs = query_ctx.snapshots().into_iter().collect::<RegionSeqs>();
    Ok(lower_sequence_bindings(plan, snapshot_seqs)?)
}

/// Returns whether `plan` contains a [`ScanSequenceBinding`].
///
/// A plan position that is pushed down to a region is hidden in the remote input
/// of a [`MergeScanLogicalPlan`], which is not a child of the node holding it, so
/// such a position is not lowered here: it is lowered on the region server when
/// the decoded fragment is planned there.
fn contains_scan_sequence_binding(plan: &LogicalPlan) -> bool {
    let mut has_binding = false;
    // The visitor always succeeds, so the walk cannot fail either.
    let _ = plan.apply_with_subqueries(|plan| {
        if let LogicalPlan::Extension(extension) = plan
            && extension.node.as_any().is::<ScanSequenceBinding>()
        {
            has_binding = true;
            return Ok(TreeNodeRecursion::Stop);
        }

        Ok(TreeNodeRecursion::Continue)
    });

    has_binding
}

pub struct DatafusionQueryEngine {
    state: Arc<QueryEngineState>,
    plugins: Plugins,
}

impl DatafusionQueryEngine {
    pub fn new(state: Arc<QueryEngineState>, plugins: Plugins) -> Self {
        Self { state, plugins }
    }

    #[tracing::instrument(skip_all)]
    async fn exec_query_plan(
        &self,
        plan: LogicalPlan,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let mut ctx = self.engine_context(query_ctx)?;
        let query_ctx = ctx.query_ctx();
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
                // Read-only system tables may have no time index; reject DELETE before key lookup.
                ensure!(
                    !is_readonly_table(&table_name.schema, &table_name.table),
                    TableReadOnlySnafu {
                        table: table_name.table.to_string(),
                    }
                );
                let table_info = table.table_info();
                let table_schema = table.schema();
                let ts_column = table_schema.timestamp_column().with_context(|| {
                    MissingTimestampColumnSnafu {
                        table_name: table_info.name.clone(),
                    }
                })?;
                let key_indices = stream
                    .schema()
                    .column_schemas()
                    .iter()
                    .enumerate()
                    .filter(|(_, column)| {
                        ts_column.name == column.name
                            || table_info
                                .meta
                                .row_key_column_names()
                                .any(|name| name == &column.name)
                    })
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>();
                while let Some(batch) = stream.next().await {
                    // The input plan already evaluated WHERE; storage deletes need only keys/time.
                    // Project before vector conversion because JSON2's logical Struct is a placeholder.
                    let batch = batch
                        .context(CreateRecordBatchSnafu)?
                        .try_project(&key_indices)
                        .context(CreateRecordBatchSnafu)?;
                    let column_vectors = batch
                        .column_vectors(&table_name.to_string(), table.schema())
                        .map_err(BoxedError::new)
                        .context(QueryExecutionSnafu)?;
                    affected_rows += self
                        .delete(&table_name, column_vectors, query_ctx.clone())
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
        column_vectors: HashMap<String, VectorRef>,
        query_ctx: QueryContextRef,
    ) -> Result<usize> {
        let catalog_name = table_name.catalog.to_string();
        let schema_name = table_name.schema.to_string();
        let table_name = table_name.table.to_string();

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

        expr_depth::validate_plan(logical_plan)?;

        let mut logger = PanicLogger {
            input_logical_plan: logical_plan,
            after_analyze: None,
            after_optimize: None,
            phy_plan: None,
        };

        let _timer = metrics::CREATE_PHYSICAL_ELAPSED.start_timer();
        let query_ctx = ctx.query_ctx();
        let state = ctx.state();

        common_telemetry::debug!("Create physical plan, input plan: {logical_plan}");

        // special handle EXPLAIN plan
        if matches!(logical_plan, DfLogicalPlan::Explain(_)) {
            // `SessionState::create_physical_plan` optimizes the explained plan and
            // then plans it physically. Both steps run separately here so that the
            // scan sequence bindings of the explained plan are lowered in between,
            // at the same point as for a regular plan.
            let optimized_plan = state.optimize(logical_plan)?;
            let optimized_plan = lower_scan_sequence_bindings(optimized_plan, &query_ctx)?;

            return state
                .query_planner()
                .create_physical_plan(&optimized_plan, state)
                .await
                .map_err(Into::into);
        }

        // analyze first
        let analyzed_plan = state.analyzer().execute_and_check(
            logical_plan.clone(),
            state.config_options(),
            |_, _| {},
        )?;

        expr_depth::validate_plan(&analyzed_plan)?;
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

        expr_depth::validate_plan(&optimized_plan)?;

        // Lower the scan sequence bindings after the optimizer and before physical
        // planning. The optimizer must not see two positions that read one table
        // with different sequence bounds as two plain scans of that table, because
        // it may merge them; physical planning is the first step that turns a scan
        // into a scan request, so it is the last step that can still take the
        // binding.
        let optimized_plan = lower_scan_sequence_bindings(optimized_plan, &query_ctx)?;

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

    fn engine_context(&self, query_ctx: QueryContextRef) -> Result<QueryEngineContext> {
        let mut state = self.state.session_state();
        let query_ctx = self.state.apply_query_options(&mut state, query_ctx)?;
        state.config_mut().set_extension(query_ctx.clone());
        state.config_mut().set_extension(self.state.clone());
        // configure execution options
        state.config_mut().options_mut().execution.time_zone =
            Some(query_ctx.timezone().to_string());

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

        Ok(QueryEngineContext::new(state, query_ctx))
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
    use std::fmt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use api::v1::SemanticType;
    use arrow::array::{ArrayRef, UInt64Array};
    use arrow_schema::SortOptions;
    use async_trait::async_trait;
    use catalog::RegisterTableRequest;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, NUMBERS_TABLE_ID};
    use common_error::ext::BoxedError;
    use common_recordbatch::{
        EmptyRecordBatchStream, RecordBatch, SendableRecordBatchStream, util,
    };
    use datafusion::physical_plan::display::{DisplayAs, DisplayFormatType};
    use datafusion::physical_plan::expressions::PhysicalSortExpr;
    use datafusion::physical_plan::joins::{HashJoinExec, JoinOn, PartitionMode};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
    use datafusion::prelude::{col, lit};
    use datafusion_common::{JoinType, NullEquality, ScalarValue};
    use datafusion_physical_expr::expressions::Column;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaRef};
    use datatypes::vectors::{Helper, UInt32Vector, VectorRef};
    use session::context::{QueryContext, QueryContextBuilder};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::region_engine::{
        PartitionRange, PrepareRequest, QueryScanContext, RegionEngine, RegionEngineRef,
        RegionRole, RegionScanner, RegionScannerRef, RegionStatistic, RemapManifestsRequest,
        RemapManifestsResponse, ScannerProperties, SetRegionRoleStateResponse,
        SettableRegionRoleState, SyncRegionFromRequest, SyncRegionFromResponse,
    };
    use store_api::region_request::RegionRequest;
    use store_api::storage::{RegionId, ScanRequest, SequenceNumber};
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::table::numbers::{NUMBERS_TABLE_NAME, NumbersTable};
    use table::table::scan::RegionScanExec;

    use super::*;
    use crate::dummy_catalog::DummyTableProvider;
    use crate::error::Error;
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

    /// Mock region engine that records the scan requests it is asked to open and
    /// answers every scan with an empty stream.
    ///
    /// Recording the request is the only way to observe what the physical scan of
    /// a plan position was bound to: the request leaves the provider in
    /// [`TableProvider::scan`](datafusion::datasource::TableProvider::scan).
    struct RecordingRegionEngine {
        metadata: RegionMetadataRef,
        requests: std::sync::Mutex<Vec<ScanRequest>>,
    }

    impl RecordingRegionEngine {
        fn new(metadata: RegionMetadataRef) -> Self {
            Self {
                metadata,
                requests: std::sync::Mutex::new(Vec::new()),
            }
        }

        /// Returns the requests opened so far and forgets them.
        fn take_requests(&self) -> Vec<ScanRequest> {
            std::mem::take(&mut *self.requests.lock().unwrap())
        }
    }

    #[async_trait]
    impl RegionEngine for RecordingRegionEngine {
        fn name(&self) -> &str {
            "RecordingRegionEngine"
        }

        async fn handle_request(
            &self,
            _region_id: RegionId,
            _request: RegionRequest,
        ) -> std::result::Result<api::region::RegionResponse, BoxedError> {
            unimplemented!()
        }

        async fn get_committed_sequence(
            &self,
            _region_id: RegionId,
        ) -> std::result::Result<SequenceNumber, BoxedError> {
            Ok(store_api::storage::SequenceNumber::default())
        }

        async fn handle_query(
            &self,
            _region_id: RegionId,
            request: ScanRequest,
        ) -> std::result::Result<RegionScannerRef, BoxedError> {
            self.requests.lock().unwrap().push(request);

            let scanner = RecordingScanner::new(
                self.metadata.schema.clone(),
                self.metadata.clone(),
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
            );
            Ok(Box::new(scanner))
        }

        async fn get_metadata(
            &self,
            _region_id: RegionId,
        ) -> std::result::Result<RegionMetadataRef, BoxedError> {
            Ok(self.metadata.clone())
        }

        fn region_statistic(&self, _region_id: RegionId) -> Option<RegionStatistic> {
            None
        }

        async fn stop(&self) -> std::result::Result<(), BoxedError> {
            Ok(())
        }

        fn set_region_role(
            &self,
            _region_id: RegionId,
            _role: RegionRole,
        ) -> std::result::Result<(), BoxedError> {
            unimplemented!()
        }

        async fn sync_region(
            &self,
            _region_id: RegionId,
            _request: SyncRegionFromRequest,
        ) -> std::result::Result<SyncRegionFromResponse, BoxedError> {
            unimplemented!()
        }

        async fn remap_manifests(
            &self,
            _request: RemapManifestsRequest,
        ) -> std::result::Result<RemapManifestsResponse, BoxedError> {
            unimplemented!()
        }

        async fn set_region_role_state_gracefully(
            &self,
            _region_id: RegionId,
            _region_role_state: SettableRegionRoleState,
        ) -> std::result::Result<SetRegionRoleStateResponse, BoxedError> {
            unimplemented!()
        }

        fn role(&self, _region_id: RegionId) -> Option<RegionRole> {
            None
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    /// Returns the metadata of a test region: `k0: string tag, ts: timestamp
    /// time index, v0: float64 field`.
    fn sequence_binding_metadata(region_id: RegionId) -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(region_id);
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("k0", ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .primary_key(vec![1]);

        Arc::new(builder.build().unwrap())
    }

    /// Creates a region provider over a [`RecordingRegionEngine`].
    fn sequence_binding_provider(
        region_id: RegionId,
    ) -> (Arc<RecordingRegionEngine>, Arc<DummyTableProvider>) {
        let metadata = sequence_binding_metadata(region_id);
        let recorder = Arc::new(RecordingRegionEngine::new(metadata.clone()));
        let engine: RegionEngineRef = recorder.clone();
        let provider = Arc::new(DummyTableProvider::new(region_id, engine, metadata));

        (recorder, provider)
    }

    /// Returns a [`TableScan`](datafusion_expr::TableScan) plan node over
    /// `provider`.
    fn sequence_binding_scan(
        provider: Arc<DummyTableProvider>,
        name: &str,
    ) -> datafusion_expr::TableScan {
        let plan = datafusion_expr::LogicalPlanBuilder::scan(
            name,
            Arc::new(datafusion::datasource::DefaultTableSource::new(provider)),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        let datafusion_expr::LogicalPlan::TableScan(scan) = plan else {
            panic!("expects a TableScan");
        };

        scan
    }

    /// Returns the query context holding `snapshot` as the snapshot sequence of
    /// `region_id`.
    fn sequence_binding_query_ctx(region_id: RegionId, snapshot: Option<u64>) -> QueryContextRef {
        let mut snapshots = std::collections::HashMap::new();
        if let Some(snapshot) = snapshot {
            snapshots.insert(region_id.as_u64(), snapshot);
        }

        Arc::new(
            QueryContextBuilder::default()
                .snapshot_seqs(Arc::new(std::sync::RwLock::new(snapshots)))
                .build(),
        )
    }

    /// Runs the plan steps of `create_physical_plan` up to the physical plan and
    /// returns the analyzed and the optimized plan.
    fn analyze_and_optimize(
        engine: &QueryEngineRef,
        plan: LogicalPlan,
        query_ctx: QueryContextRef,
    ) -> (LogicalPlan, LogicalPlan) {
        let ctx = engine.engine_context(query_ctx).unwrap();
        let state = ctx.state().clone();
        let analyzed = state
            .analyzer()
            .execute_and_check(plan, state.config_options(), |_, _| {})
            .unwrap();
        let optimized = state
            .optimizer()
            .optimize(analyzed.clone(), &state, |_, _| {})
            .unwrap();

        (analyzed, optimized)
    }

    #[tokio::test]
    #[allow(clippy::print_stdout)]
    async fn test_lowering_removes_scan_sequence_bindings_after_optimize() {
        let engine = create_test_engine().await;
        let region_id = RegionId::new(1024, 1);
        let (recorder, provider) = sequence_binding_provider(region_id);
        let snapshot = 42;
        let query_ctx = sequence_binding_query_ctx(region_id, Some(snapshot));

        // Two plan positions read the same table after different sequences. The
        // wrapped scans are equal, so nothing but the binding tells them apart.
        let binding_a = ScanSequenceBinding::new(
            0,
            RegionSeqs::from([(region_id.as_u64(), 10)]),
            sequence_binding_scan(provider.clone(), "seq_lower"),
        );
        let binding_b = ScanSequenceBinding::new(
            1,
            RegionSeqs::from([(region_id.as_u64(), 20)]),
            sequence_binding_scan(provider, "seq_lower"),
        );
        let plan = datafusion_expr::LogicalPlanBuilder::from(binding_a.into_logical_plan())
            .union(
                datafusion_expr::LogicalPlanBuilder::from(binding_b.into_logical_plan())
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();

        // The wrapper survives the analyzer and the optimizer, and disappears in
        // the lowering step between the optimizer and physical planning. This is
        // the order `create_physical_plan` runs in.
        let (analyzed, optimized) = analyze_and_optimize(&engine, plan.clone(), query_ctx.clone());
        println!("analyzed plan:\n{}", analyzed.display_indent());
        println!("optimized plan:\n{}", optimized.display_indent());
        assert!(
            super::contains_scan_sequence_binding(&optimized),
            "the wrapper must still be there after the optimizer:\n{}",
            optimized.display_indent()
        );
        let lowered = super::lower_scan_sequence_bindings(optimized.clone(), &query_ctx).unwrap();
        println!("lowered plan:\n{}", lowered.display_indent());
        assert!(
            !super::contains_scan_sequence_binding(&lowered),
            "lowering must remove the wrapper:\n{}",
            lowered.display_indent()
        );

        // The same plan through the real execution path executes at all: a wrapper
        // reaching physical planning has no extension planner and fails, so a
        // successful plan proves the lowering ran before the physical planner.
        let output = engine.execute(plan, query_ctx.clone()).await.unwrap();
        if let Some(plan) = output.meta.plan.as_ref() {
            println!(
                "physical plan:\n{}",
                datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
            );
        }

        // Each position opened its own request, with its own id, lower bound and
        // the snapshot as upper bound.
        let mut requests = recorder.take_requests();
        requests.sort_by_key(|request| request.scan_id);
        println!("scan requests: {requests:?}");
        assert_eq!(2, requests.len(), "each position must open its own scan");
        for (request, after) in requests.iter().zip([10_u64, 20]) {
            assert_eq!(Some(after), request.memtable_min_sequence);
            assert_eq!(Some(snapshot), request.memtable_max_sequence);
            assert!(request.exact_sequence_range);
            assert_eq!(None, request.sst_min_sequence);
            assert!(!request.snapshot_on_scan);
        }
        assert_eq!(Some(0), requests[0].scan_id);
        assert_eq!(Some(1), requests[1].scan_id);
    }

    #[tokio::test]
    #[allow(clippy::print_stdout)]
    async fn test_lowering_keeps_unbound_plans_unchanged() {
        let engine = create_test_engine().await;
        let region_id = RegionId::new(1024, 1);
        let (recorder, provider) = sequence_binding_provider(region_id);
        // No snapshot at all: a plan without a binding must not need one.
        let query_ctx = sequence_binding_query_ctx(region_id, None);

        let plan = LogicalPlan::TableScan(sequence_binding_scan(provider, "seq_lower"));

        let (_, optimized) = analyze_and_optimize(&engine, plan.clone(), query_ctx.clone());
        assert!(!super::contains_scan_sequence_binding(&optimized));
        let unchanged = super::lower_scan_sequence_bindings(optimized.clone(), &query_ctx).unwrap();
        assert_eq!(optimized, unchanged);

        let output = engine.execute(plan, query_ctx.clone()).await.unwrap();
        if let Some(plan) = output.meta.plan.as_ref() {
            println!(
                "physical plan:\n{}",
                datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
            );
        }

        // The scan keeps the request it had before sequence bindings existed: no
        // position id, no explicit bounds, no exact range.
        let requests = recorder.take_requests();
        println!("scan requests: {requests:?}");
        assert_eq!(1, requests.len());
        let request = &requests[0];
        assert_eq!(None, request.scan_id);
        assert_eq!(None, request.memtable_min_sequence);
        assert_eq!(None, request.memtable_max_sequence);
        assert!(!request.exact_sequence_range);
        assert!(!request.snapshot_on_scan);
    }

    #[tokio::test]
    #[allow(clippy::print_stdout)]
    async fn test_lowering_fails_instead_of_scanning_everything() {
        let engine = create_test_engine().await;
        let region_id = RegionId::new(1024, 1);
        let (recorder, provider) = sequence_binding_provider(region_id);
        // The query has no snapshot sequence for the region, so the upper bound
        // `H` of the position cannot be resolved.
        let query_ctx = sequence_binding_query_ctx(region_id, None);

        let binding = ScanSequenceBinding::new(
            0,
            RegionSeqs::from([(region_id.as_u64(), 10)]),
            sequence_binding_scan(provider, "seq_lower"),
        );
        let plan = binding.into_logical_plan();

        let err = engine
            .execute(plan, query_ctx)
            .await
            .expect_err("a position without an upper bound must fail the query");
        println!("lowering failed with: {err}");
        assert!(matches!(err, Error::DataFusion { .. }), "unexpected: {err}");
        let message = err.to_string();
        assert!(
            message.contains("no snapshot sequence for region"),
            "unexpected message: {message}"
        );
        assert!(
            message.contains(&region_id.as_u64().to_string()),
            "the message must name the region: {message}"
        );

        // Nothing was scanned: the query failed instead of reading the whole
        // table.
        assert!(recorder.take_requests().is_empty());
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
        let engine_ctx = engine.engine_context(QueryContext::arc()).unwrap();
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
        let engine_ctx = engine.engine_context(QueryContext::arc()).unwrap();
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
}
