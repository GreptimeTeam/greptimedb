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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ahash::{HashMap, HashSet};
use arrow_schema::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, SortOptions};
use async_stream::stream;
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_plugins::GREPTIME_EXEC_READ_COST;
use common_query::request::QueryRequest;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_telemetry::tracing_context::TracingContext;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{Column as ColumnExpr, DataFusionError, Result};
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{Distribution, EquivalenceProperties, PhysicalSortExpr};
use futures_util::StreamExt;
use greptime_proto::v1::region::RegionRequestHeader;
use meter_core::data::ReadItem;
use meter_macros::read_meter;
use session::context::QueryContextRef;
use store_api::metrics::{REGION_QUERY_CPU_TIME, REGION_QUERY_SCANNED_BYTES};
use store_api::storage::RegionId;
use table::table::scan::REGION_SCAN_EXEC_NAME;
use table::table_name::TableName;
use tokio::time::Instant;
use tracing::{Instrument, Span};

use crate::dist_plan::analyzer::AliasMapping;
use crate::dist_plan::analyzer::utils::patch_batch_timezone;
use crate::dist_plan::dyn_filter_bridge::{
    CapturedDynFilter, capture_remote_dyn_filters_for_pushdown,
    query_context_with_initial_dyn_filter_regs, register_dyn_filters_for_region,
};
use crate::dist_plan::{RemoteDynFilterProducerId, RemoteDynFilterRegistryLease};
use crate::metrics::{MERGE_SCAN_ERRORS_TOTAL, MERGE_SCAN_POLL_ELAPSED, MERGE_SCAN_REGIONS};
use crate::options::{FlowQueryExtensions, remote_dyn_filter_pushdown_enabled_from_extensions};
use crate::query_engine::QueryEngineState;
use crate::region_query::RegionQueryHandlerRef;

fn query_engine_state_from_task_context(context: &TaskContext) -> Option<Arc<QueryEngineState>> {
    context.session_config().get_extension()
}

fn remote_dyn_filter_enabled(query_ctx: &QueryContextRef) -> Result<bool> {
    remote_dyn_filter_pushdown_enabled_from_extensions(&query_ctx.extensions())
        .map_err(|err| DataFusionError::External(Box::new(err)))
}

fn acquire_remote_dyn_filter_registry_lease(
    context: &TaskContext,
    query_ctx: &QueryContextRef,
    captured_dyn_filters: &[CapturedDynFilter],
) -> Option<RemoteDynFilterRegistryLease> {
    if captured_dyn_filters.is_empty() {
        return None;
    }

    let query_id = query_ctx.remote_query_id_value()?;
    let query_engine_state = query_engine_state_from_task_context(context)?;
    Some(
        query_engine_state
            .dyn_filter_registry_manager()
            .acquire_lease(query_id),
    )
}

fn query_context_for_remote_dyn_filter_region(
    query_ctx: &QueryContextRef,
    region_id: RegionId,
    remote_dyn_filter_registry_lease: Option<&RemoteDynFilterRegistryLease>,
    captured_dyn_filters: &[CapturedDynFilter],
) -> session::context::QueryContext {
    if let Some(remote_dyn_filter_registry_lease) = remote_dyn_filter_registry_lease {
        register_dyn_filters_for_region(
            remote_dyn_filter_registry_lease.registry(),
            region_id,
            captured_dyn_filters,
        );
    }

    query_context_with_initial_dyn_filter_regs(query_ctx, region_id, captured_dyn_filters)
}

#[derive(Debug, Hash, PartialOrd, PartialEq, Eq, Clone)]
pub struct MergeScanLogicalPlan {
    /// In logical plan phase it only contains one input
    input: LogicalPlan,
    /// If this plan is a placeholder
    is_placeholder: bool,
    partition_cols: AliasMapping,
    /// Assigned after dist-plan rewriting so rewriters only deal with plan shape.
    remote_dyn_filter_producer_id: Option<RemoteDynFilterProducerId>,
}

impl UserDefinedLogicalNodeCore for MergeScanLogicalPlan {
    fn name(&self) -> &str {
        Self::name()
    }

    // Prevent further optimization.
    // The input can be retrieved by `self.input()`
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        self.input.schema()
    }

    // Prevent further optimization
    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MergeScan [is_placeholder={}, remote_input=[\n{}\n]]",
            self.is_placeholder, self.input
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::prelude::Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(self.clone())
    }
}

impl MergeScanLogicalPlan {
    pub fn new(input: LogicalPlan, is_placeholder: bool, partition_cols: AliasMapping) -> Self {
        Self {
            input,
            is_placeholder,
            partition_cols,
            remote_dyn_filter_producer_id: None,
        }
    }

    pub(crate) fn with_remote_dyn_filter_producer_id(
        mut self,
        remote_dyn_filter_producer_id: RemoteDynFilterProducerId,
    ) -> Self {
        self.remote_dyn_filter_producer_id = Some(remote_dyn_filter_producer_id);
        self
    }

    pub fn name() -> &'static str {
        "MergeScan"
    }

    /// Create a [LogicalPlan::Extension] node from this merge scan plan
    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    pub fn is_placeholder(&self) -> bool {
        self.is_placeholder
    }

    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    pub fn partition_cols(&self) -> &AliasMapping {
        &self.partition_cols
    }

    pub fn remote_dyn_filter_producer_id(&self) -> Option<RemoteDynFilterProducerId> {
        self.remote_dyn_filter_producer_id
    }
}

#[derive(Clone)]
pub struct MergeScanExec {
    table: TableName,
    regions: Vec<RegionId>,
    plan: LogicalPlan,
    arrow_schema: ArrowSchemaRef,
    region_query_handler: RegionQueryHandlerRef,
    metric: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
    /// Metrics from sub stages
    sub_stage_metrics: Arc<Mutex<HashMap<RegionId, RecordBatchMetrics>>>,
    /// Metrics for each partition
    partition_metrics: Arc<Mutex<HashMap<usize, PartitionMetrics>>>,
    query_ctx: QueryContextRef,
    /// Optional because RDF must fail open: missing ids skip RDF but keep normal query execution.
    remote_dyn_filter_producer_id: Option<RemoteDynFilterProducerId>,
    captured_remote_dyn_filters: Arc<Mutex<Vec<CapturedDynFilter>>>,
    target_partition: usize,
    partition_cols: AliasMapping,
    enable_per_region_metrics: bool,
}

impl std::fmt::Debug for MergeScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergeScanExec")
            .field("table", &self.table)
            .field("regions", &self.regions)
            .field("plan", &self.plan)
            .finish()
    }
}

impl MergeScanExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session_state: &SessionState,
        table: TableName,
        regions: Vec<RegionId>,
        plan: LogicalPlan,
        arrow_schema: &ArrowSchema,
        region_query_handler: RegionQueryHandlerRef,
        query_ctx: QueryContextRef,
        target_partition: usize,
        partition_cols: AliasMapping,
        remote_dyn_filter_producer_id: Option<RemoteDynFilterProducerId>,
        enable_per_region_metrics: bool,
    ) -> Result<Self> {
        // TODO(CookiePieWw): Initially we removed the metadata from the schema in #2000, but we have to
        // keep it for #4619 to identify json type in src/datatypes/src/schema/column_schema.rs.
        // Reconsider if it's possible to remove it.
        let arrow_schema = Arc::new(arrow_schema.clone());

        // States the output ordering of the plan.
        //
        // When the input plan is a sort, we can use the sort ordering as the output ordering
        // if the target partition is greater than the number of regions, which means we won't
        // break the ordering on merging (of MergeScan).
        //
        // Otherwise, we need to use the default ordering.
        let eq_properties = if let LogicalPlan::Sort(sort) = &plan
            && target_partition >= regions.len()
        {
            let lex_ordering = sort
                .expr
                .iter()
                .map(|sort_expr| {
                    let physical_expr = session_state
                        .create_physical_expr(sort_expr.expr.clone(), plan.schema())?;
                    Ok(PhysicalSortExpr::new(
                        physical_expr,
                        SortOptions {
                            descending: !sort_expr.asc,
                            nulls_first: sort_expr.nulls_first,
                        },
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            EquivalenceProperties::new_with_orderings(arrow_schema.clone(), vec![lex_ordering])
        } else {
            EquivalenceProperties::new(arrow_schema.clone())
        };

        let partition_exprs = partition_cols
            .iter()
            .filter_map(|col| {
                if let Some(first_alias) = col.1.first() {
                    session_state
                        .create_physical_expr(
                            Expr::Column(ColumnExpr::new_unqualified(
                                first_alias.name().to_string(),
                            )),
                            plan.schema(),
                        )
                        .ok()
                } else {
                    None
                }
            })
            .collect();
        let partitioning = Partitioning::Hash(partition_exprs, target_partition);

        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            table,
            regions,
            plan,
            arrow_schema,
            region_query_handler,
            metric: ExecutionPlanMetricsSet::new(),
            sub_stage_metrics: Arc::default(),
            partition_metrics: Arc::default(),
            properties,
            query_ctx,
            remote_dyn_filter_producer_id,
            captured_remote_dyn_filters: Arc::default(),
            target_partition,
            partition_cols,
            enable_per_region_metrics,
        })
    }

    pub fn to_stream(
        &self,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> Result<SendableRecordBatchStream> {
        // prepare states to move
        let regions = self.regions.clone();
        let region_query_handler = self.region_query_handler.clone();
        let metric = MergeScanMetric::new(&self.metric);
        let arrow_schema = self.arrow_schema.clone();
        let query_ctx = self.query_ctx.clone();
        let sub_stage_metrics_moved = self.sub_stage_metrics.clone();
        let partition_metrics_moved = self.partition_metrics.clone();
        let plan = self.plan.clone();
        let target_partition = self.target_partition;
        let enable_per_region_metrics = self.enable_per_region_metrics;
        let remote_dyn_filter_enabled = remote_dyn_filter_enabled(&self.query_ctx)?;
        let captured_remote_dyn_filters = if remote_dyn_filter_enabled {
            self.captured_remote_dyn_filters()
        } else {
            Vec::new()
        };
        let dbname = context.task_id().unwrap_or_default();
        let tracing_context = TracingContext::from_json(context.session_id().as_str());
        let current_channel = self.query_ctx.channel();
        let read_preference = self.query_ctx.read_preference();
        let explain_verbose = self.query_ctx.explain_verbose();
        let remote_dyn_filter_registry_lease = acquire_remote_dyn_filter_registry_lease(
            context.as_ref(),
            &query_ctx,
            &captured_remote_dyn_filters,
        );

        let stream = Box::pin(stream!({
            let remote_dyn_filter_registry_lease = remote_dyn_filter_registry_lease;
            // only report metrics once for each MergeScan
            if partition == 0 {
                MERGE_SCAN_REGIONS.observe(regions.len() as f64);
            }

            let _finish_timer = metric.finish_time().timer();
            let mut ready_timer = metric.ready_time().timer();
            let mut first_consume_timer = Some(metric.first_consume_time().timer());

            // Per-partition timings, scoped to this partition's stream for `EXPLAIN VERBOSE`.
            let partition_start = Instant::now();
            let mut partition_ready_time: Option<Duration> = None;
            let mut partition_first_consume_time: Option<Duration> = None;

            for region_id in regions
                .iter()
                .skip(partition)
                .step_by(target_partition)
                .copied()
            {
                let region_span = tracing_context.attach(tracing::info_span!(
                    parent: &Span::current(),
                    "merge_scan_region",
                    region_id = %region_id,
                    partition = partition
                ));
                let region_query_ctx = query_context_for_remote_dyn_filter_region(
                    &query_ctx,
                    region_id,
                    remote_dyn_filter_registry_lease.as_ref(),
                    &captured_remote_dyn_filters,
                );
                let request = QueryRequest {
                    header: Some(RegionRequestHeader {
                        tracing_context: tracing_context.to_w3c(),
                        dbname: dbname.clone(),
                        query_context: Some((&region_query_ctx).into()),
                    }),
                    region_id,
                    plan: plan.clone(),
                };
                let region_start = Instant::now();
                let do_get_start = Instant::now();

                if explain_verbose {
                    common_telemetry::info!(
                        "Merge scan one region, partition: {}, region_id: {}",
                        partition,
                        region_id
                    );
                }

                let mut stream = region_query_handler
                    .do_get(read_preference, request)
                    .instrument(region_span.clone())
                    .await
                    .map_err(|e| {
                        MERGE_SCAN_ERRORS_TOTAL.inc();
                        DataFusionError::External(Box::new(e))
                    })?;
                let do_get_cost = do_get_start.elapsed();

                if let Some(remote_dyn_filter_registry_lease) =
                    remote_dyn_filter_registry_lease.as_ref()
                {
                    remote_dyn_filter_registry_lease
                        .ensure_fanout_task(region_query_handler.clone());
                }

                ready_timer.stop();
                if partition_ready_time.is_none() {
                    partition_ready_time = Some(partition_start.elapsed());
                }

                let mut poll_duration = Duration::ZERO;
                let mut poll_timer = Instant::now();
                while let Some(batch) = stream.next().instrument(region_span.clone()).await {
                    let poll_elapsed = poll_timer.elapsed();
                    poll_duration += poll_elapsed;

                    let batch = batch.map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let batch = patch_batch_timezone(
                        arrow_schema.clone(),
                        batch.into_df_record_batch().columns().to_vec(),
                    )?;
                    metric.record_output_batch_rows(batch.num_rows());
                    if let Some(mut first_consume_timer) = first_consume_timer.take() {
                        first_consume_timer.stop();
                        partition_first_consume_time = Some(partition_start.elapsed());
                    }

                    if let Some(metrics) = stream.metrics() {
                        let mut sub_stage_metrics = sub_stage_metrics_moved.lock().unwrap();
                        sub_stage_metrics.insert(region_id, metrics);
                    }

                    yield Ok(batch);
                    // reset poll timer
                    poll_timer = Instant::now();
                }
                // Also stop on an exhausted stream that yielded no batch. The `take()`
                // guard ensures it only records once, on the first such region.
                if let Some(mut first_consume_timer) = first_consume_timer.take() {
                    first_consume_timer.stop();
                    partition_first_consume_time = Some(partition_start.elapsed());
                }
                let total_cost = region_start.elapsed();

                // Record region metrics and push to global partition_metrics
                let region_metrics = RegionMetrics {
                    region_id,
                    poll_duration,
                    do_get_cost,
                    total_cost,
                };

                // Push RegionMetrics to global partition_metrics immediately after scanning this region
                {
                    let mut partition_metrics_guard = partition_metrics_moved.lock().unwrap();
                    let partition_metrics = partition_metrics_guard
                        .entry(partition)
                        .or_insert_with(|| PartitionMetrics::new(partition, explain_verbose));
                    partition_metrics.add_region_metrics(region_metrics);
                }

                if explain_verbose {
                    common_telemetry::info!(
                        "Merge scan finish one region, partition: {}, region_id: {}, poll_duration: {:?}, first_consume: {}, do_get_cost: {:?}",
                        partition,
                        region_id,
                        poll_duration,
                        metric.first_consume_time(),
                        do_get_cost
                    );
                }

                // process metrics after all data is drained.
                if let Some(metrics) = stream.metrics() {
                    let load = region_scan_load(&metrics);
                    report_region_query_load(enable_per_region_metrics, region_id, &metrics);
                    let (c, s) = parse_catalog_and_schema_from_db_string(&dbname);
                    let value = read_meter!(c, s, load, current_channel as u8);
                    metric.record_greptime_exec_cost(value as usize);

                    // record metrics from sub sgates
                    let mut sub_stage_metrics = sub_stage_metrics_moved.lock().unwrap();
                    sub_stage_metrics.insert(region_id, metrics);
                }

                MERGE_SCAN_POLL_ELAPSED.observe(poll_duration.as_secs_f64());
            }

            // Stop the global timers for partitions with no region, otherwise they keep
            // running until drop and inflate the shared metrics. No-op otherwise.
            ready_timer.stop();
            if let Some(mut first_consume_timer) = first_consume_timer.take() {
                first_consume_timer.stop();
            }

            // Finish partition metrics and log results
            let partition_finish_time = partition_start.elapsed();
            {
                let mut partition_metrics_guard = partition_metrics_moved.lock().unwrap();
                if let Some(partition_metrics) = partition_metrics_guard.get_mut(&partition) {
                    partition_metrics.set_timings(
                        partition_ready_time.unwrap_or_default(),
                        partition_first_consume_time.unwrap_or_default(),
                        partition_finish_time,
                    );
                    partition_metrics.finish();
                }
            }
        }));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.arrow_schema.clone(),
            stream,
        )))
    }

    pub fn try_with_new_distribution(&self, distribution: Distribution) -> Option<Self> {
        let Distribution::HashPartitioned(hash_exprs) = distribution else {
            // not applicable
            return None;
        };

        if let Partitioning::Hash(curr_dist, _) = &self.properties.partitioning
            && curr_dist == &hash_exprs
        {
            // No need to change the distribution
            return None;
        }

        let all_partition_col_aliases: HashSet<_> = self
            .partition_cols
            .values()
            .flat_map(|aliases| aliases.iter().map(|c| c.name()))
            .collect();
        let overlaps: Vec<_> = hash_exprs
            .iter()
            .filter(|expr| {
                expr.as_any()
                    .downcast_ref::<Column>()
                    .is_some_and(|col_expr| all_partition_col_aliases.contains(col_expr.name()))
            })
            .cloned()
            .collect();

        if overlaps.is_empty() {
            return None;
        }

        Some(Self {
            table: self.table.clone(),
            regions: self.regions.clone(),
            plan: self.plan.clone(),
            arrow_schema: self.arrow_schema.clone(),
            region_query_handler: self.region_query_handler.clone(),
            metric: self.metric.clone(),
            properties: Arc::new(PlanProperties::new(
                self.properties.eq_properties.clone(),
                Partitioning::Hash(overlaps, self.target_partition),
                self.properties.emission_type,
                self.properties.boundedness,
            )),
            sub_stage_metrics: self.sub_stage_metrics.clone(),
            partition_metrics: self.partition_metrics.clone(),
            query_ctx: self.query_ctx.clone(),
            remote_dyn_filter_producer_id: self.remote_dyn_filter_producer_id,
            captured_remote_dyn_filters: self.captured_remote_dyn_filters.clone(),
            target_partition: self.target_partition,
            partition_cols: self.partition_cols.clone(),
            enable_per_region_metrics: self.enable_per_region_metrics,
        })
    }

    fn captured_remote_dyn_filters(&self) -> Vec<CapturedDynFilter> {
        self.captured_remote_dyn_filters.lock().unwrap().clone()
    }

    pub fn sub_stage_metrics(&self) -> Vec<RecordBatchMetrics> {
        self.sub_stage_metrics
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    pub fn regions(&self) -> &[RegionId] {
        &self.regions
    }

    pub fn is_flow_sink_scan(&self) -> bool {
        let Some(sink_table_id) =
            FlowQueryExtensions::parse_flow_extensions(&self.query_ctx.extensions())
                .ok()
                .flatten()
                .and_then(|extensions| extensions.sink_table_id)
        else {
            return false;
        };

        !self.regions.is_empty()
            && self
                .regions
                .iter()
                .all(|region_id| region_id.table_id() == sink_table_id)
    }

    pub fn partition_count(&self) -> usize {
        self.target_partition
    }

    pub fn region_count(&self) -> usize {
        self.regions.len()
    }

    fn partition_metrics(&self) -> Vec<PartitionMetrics> {
        self.partition_metrics
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
impl MergeScanExec {
    fn remote_dyn_filter_producer_id(&self) -> Option<RemoteDynFilterProducerId> {
        self.remote_dyn_filter_producer_id
    }
}

/// Metrics for a region of a partition.
#[derive(Debug, Clone)]
struct RegionMetrics {
    region_id: RegionId,
    poll_duration: Duration,
    do_get_cost: Duration,
    /// Total cost to scan the region.
    total_cost: Duration,
}

/// Metrics for a partition of a MergeScanExec.
#[derive(Debug, Clone)]
struct PartitionMetrics {
    partition: usize,
    region_metrics: Vec<RegionMetrics>,
    total_poll_duration: Duration,
    total_do_get_cost: Duration,
    total_regions: usize,
    /// Time until this partition's scan is ready to emit data.
    ready_time: Duration,
    /// Time until this partition's first stream poll resolves (a batch or exhausted).
    first_consume_time: Duration,
    /// Time until this partition's scan finishes execution.
    finish_time: Duration,
    explain_verbose: bool,
    finished: bool,
}

impl PartitionMetrics {
    fn new(partition: usize, explain_verbose: bool) -> Self {
        Self {
            partition,
            region_metrics: Vec::new(),
            total_poll_duration: Duration::ZERO,
            total_do_get_cost: Duration::ZERO,
            total_regions: 0,
            ready_time: Duration::ZERO,
            first_consume_time: Duration::ZERO,
            finish_time: Duration::ZERO,
            explain_verbose,
            finished: false,
        }
    }

    fn add_region_metrics(&mut self, region_metrics: RegionMetrics) {
        self.total_poll_duration += region_metrics.poll_duration;
        self.total_do_get_cost += region_metrics.do_get_cost;
        self.total_regions += 1;
        self.region_metrics.push(region_metrics);
    }

    /// Set the per-partition timings captured during streaming.
    fn set_timings(
        &mut self,
        ready_time: Duration,
        first_consume_time: Duration,
        finish_time: Duration,
    ) {
        self.ready_time = ready_time;
        self.first_consume_time = first_consume_time;
        self.finish_time = finish_time;
    }

    /// Finish the partition metrics and log the results.
    fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.log_metrics();
    }

    /// Log partition metrics based on explain_verbose level.
    fn log_metrics(&self) {
        if self.explain_verbose {
            common_telemetry::info!(
                "MergeScan partition {} finished: {} regions, total_poll_duration: {:?}, total_do_get_cost: {:?}, ready_time: {:?}, first_consume_time: {:?}, finish_time: {:?}",
                self.partition,
                self.total_regions,
                self.total_poll_duration,
                self.total_do_get_cost,
                self.ready_time,
                self.first_consume_time,
                self.finish_time
            );
        } else {
            common_telemetry::debug!(
                "MergeScan partition {} finished: {} regions, total_poll_duration: {:?}, total_do_get_cost: {:?}, ready_time: {:?}, first_consume_time: {:?}, finish_time: {:?}",
                self.partition,
                self.total_regions,
                self.total_poll_duration,
                self.total_do_get_cost,
                self.ready_time,
                self.first_consume_time,
                self.finish_time
            );
        }
    }
}

impl Drop for PartitionMetrics {
    fn drop(&mut self) {
        if !self.finished {
            self.log_metrics();
        }
    }
}

impl ExecutionPlan for MergeScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    // DataFusion will swap children unconditionally.
    // But since this node is leaf node, it's safe to just return self.
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self.clone())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let parent_filters = child_pushdown_result
            .parent_filters
            .into_iter()
            .map(|filter| filter.filter)
            .collect::<Vec<_>>();

        if !remote_dyn_filter_enabled(&self.query_ctx)? {
            // Reject remote pushdown instead of pretending success: this keeps
            // DataFusion/local dynamic filter semantics intact while disabling
            // only FE -> DN remote dynamic filter propagation.
            self.captured_remote_dyn_filters.lock().unwrap().clear();
            let new_self = Arc::new(self.clone());

            return Ok(FilterPushdownPropagation {
                filters: parent_filters.into_iter().map(|_| PushedDown::No).collect(),
                updated_node: Some(new_self),
            });
        }

        let Some(remote_dyn_filter_producer_id) = self.remote_dyn_filter_producer_id else {
            // Missing RDF identity disables only RDF, not normal execution.
            common_telemetry::warn!(
                "MergeScan remote dynamic filter producer id is not assigned; skipping remote dynamic filter pushdown"
            );
            self.captured_remote_dyn_filters.lock().unwrap().clear();
            let new_self = Arc::new(self.clone());

            return Ok(FilterPushdownPropagation {
                filters: parent_filters.into_iter().map(|_| PushedDown::No).collect(),
                updated_node: Some(new_self),
            });
        };
        let remote_dyn_filter_pushdown =
            capture_remote_dyn_filters_for_pushdown(remote_dyn_filter_producer_id, parent_filters);
        *self.captured_remote_dyn_filters.lock().unwrap() =
            remote_dyn_filter_pushdown.captured_dyn_filters;
        let new_self = Arc::new(self.clone());

        Ok(FilterPushdownPropagation {
            filters: remote_dyn_filter_pushdown
                .pushed_down
                .into_iter()
                .map(|pushdown_ready| {
                    if pushdown_ready {
                        PushedDown::Yes
                    } else {
                        PushedDown::No
                    }
                })
                .collect(),
            updated_node: Some(new_self),
        })
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.to_stream(context, partition)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn name(&self) -> &str {
        "MergeScanExec"
    }
}

impl DisplayAs for MergeScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MergeScanExec: peers=[")?;
        for region_id in self.regions.iter() {
            write!(f, "{}, ", region_id)?;
        }
        write!(f, "]")?;

        if matches!(t, DisplayFormatType::Verbose) {
            let partition_metrics = self.partition_metrics();
            if !partition_metrics.is_empty() {
                write!(f, ", metrics={{")?;
                for (i, pm) in partition_metrics.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(
                        f,
                        "\"partition_{}\":{{\"regions\":{},\"total_poll_duration\":\"{:?}\",\"total_do_get_cost\":\"{:?}\",\"ready_time\":\"{:?}\",\"first_consume_time\":\"{:?}\",\"finish_time\":\"{:?}\",\"region_metrics\":[",
                        pm.partition,
                        pm.total_regions,
                        pm.total_poll_duration,
                        pm.total_do_get_cost,
                        pm.ready_time,
                        pm.first_consume_time,
                        pm.finish_time
                    )?;
                    for (j, rm) in pm.region_metrics.iter().enumerate() {
                        if j > 0 {
                            write!(f, ",")?;
                        }
                        write!(
                            f,
                            "{{\"region_id\":\"{}\",\"poll_duration\":\"{:?}\",\"do_get_cost\":\"{:?}\",\"total_cost\":\"{:?}\"}}",
                            rm.region_id, rm.poll_duration, rm.do_get_cost, rm.total_cost
                        )?;
                    }
                    write!(f, "]}}")?;
                }
                write!(f, "}}")?;
            }
        }

        Ok(())
    }
}

/// Extract total `output_bytes` from [`RegionScanExec`] plan nodes.
fn scan_output_bytes(metrics: &RecordBatchMetrics) -> usize {
    metrics
        .plan_metrics
        .iter()
        .filter(|pm| pm.plan_name == REGION_SCAN_EXEC_NAME)
        .flat_map(|pm| &pm.metrics)
        .filter_map(|(name, value)| (name == "output_bytes").then_some(*value))
        .sum()
}

fn region_scan_load(metrics: &RecordBatchMetrics) -> ReadItem {
    ReadItem {
        cpu_time: metrics.elapsed_compute as u64,
        table_scan: scan_output_bytes(metrics) as u64,
    }
}

fn report_region_query_load(
    enable_per_region_metrics: bool,
    region_id: RegionId,
    metrics: &RecordBatchMetrics,
) {
    if !enable_per_region_metrics {
        return;
    }

    let region_id = region_id.to_string();
    let load = region_scan_load(metrics);
    REGION_QUERY_CPU_TIME
        .with_label_values(&[&region_id])
        .add(load.cpu_time as i64);
    REGION_QUERY_SCANNED_BYTES
        .with_label_values(&[&region_id])
        .add(load.table_scan as i64);
}

#[derive(Debug, Clone)]
struct MergeScanMetric {
    /// Nanosecond elapsed till the scan operator is ready to emit data
    ready_time: Time,
    /// Nanosecond elapsed till the first record batch emitted from the scan operator gets consumed
    first_consume_time: Time,
    /// Nanosecond elapsed till the scan operator finished execution
    finish_time: Time,
    /// Count of rows fetched from remote
    output_rows: Count,

    /// Gauge for greptime plan execution cost metrics for output
    greptime_exec_cost: Gauge,
}

impl MergeScanMetric {
    pub fn new(metric: &ExecutionPlanMetricsSet) -> Self {
        Self {
            ready_time: MetricBuilder::new(metric).subset_time("ready_time", 1),
            first_consume_time: MetricBuilder::new(metric).subset_time("first_consume_time", 1),
            finish_time: MetricBuilder::new(metric).subset_time("finish_time", 1),
            output_rows: MetricBuilder::new(metric).output_rows(1),
            greptime_exec_cost: MetricBuilder::new(metric).gauge(GREPTIME_EXEC_READ_COST, 1),
        }
    }

    pub fn ready_time(&self) -> &Time {
        &self.ready_time
    }

    pub fn first_consume_time(&self) -> &Time {
        &self.first_consume_time
    }

    pub fn finish_time(&self) -> &Time {
        &self.finish_time
    }

    pub fn record_output_batch_rows(&self, num_rows: usize) {
        self.output_rows.add(num_rows);
    }

    pub fn record_greptime_exec_cost(&self, metrics: usize) {
        self.greptime_exec_cost.add(metrics);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use async_trait::async_trait;
    use common_query::request::INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY;
    use common_recordbatch::adapter::{PlanMetrics, RecordBatchMetrics};
    use datafusion::config::ConfigOptions;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::filter_pushdown::ChildFilterPushdownResult;
    use datafusion_common::TableReference;
    use datafusion_expr::{LogicalPlanBuilder, lit};
    use datafusion_physical_expr::Distribution;
    use datafusion_physical_expr::expressions::{
        Column, DynamicFilterPhysicalExpr, lit as physical_lit,
    };
    use session::ReadPreference;
    use session::context::QueryContext;
    use session::query_id::QueryId;
    use table::table_name::TableName;
    use uuid::Uuid;

    use super::*;
    use crate::dist_plan::{DynFilterRegistryManager, Subscriber};
    use crate::region_query::RegionQueryHandler;

    fn test_query_id(value: u128) -> QueryId {
        QueryId::from(Uuid::from_u128(value))
    }

    #[test]
    fn remote_dyn_filter_region_query_context_registers_before_do_get() {
        let registry_manager = Arc::new(DynFilterRegistryManager::default());
        let query_ctx = QueryContext::arc();
        let query_id = query_ctx
            .remote_query_id_value()
            .expect("query context must have remote query id");
        let lease = registry_manager.acquire_lease(query_id);
        let region_id = RegionId::new(1024, 7);
        let dyn_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("host", 0)) as Arc<_>],
            physical_lit(true) as _,
        )) as Arc<dyn datafusion_physical_expr::PhysicalExpr>;
        let captured = capture_remote_dyn_filters_for_pushdown(
            RemoteDynFilterProducerId::new(42),
            vec![dyn_filter],
        );
        assert_eq!(captured.captured_dyn_filters.len(), 1);

        let region_query_ctx = query_context_for_remote_dyn_filter_region(
            &query_ctx,
            region_id,
            Some(&lease),
            &captured.captured_dyn_filters,
        );

        let entries = lease.registry().entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].subscribers(), vec![Subscriber::new(region_id)]);
        assert!(
            !entries[0].fanout_started_for_test(),
            "fanout must start only after do_get succeeds"
        );
        assert!(
            region_query_ctx
                .extension(INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY)
                .is_some(),
            "initial RDF registrations must be present in the do_get query context"
        );
    }

    #[test]
    fn remote_dyn_filter_registry_cleanup_waits_for_last_query_scoped_stream_drop() {
        let registry_manager = Arc::new(DynFilterRegistryManager::default());
        let query_id = test_query_id(1);

        let first = registry_manager.acquire_lease(query_id);
        let second = registry_manager.acquire_lease(query_id);

        drop(first);
        assert_eq!(registry_manager.registry_count(), 1);

        drop(second);
        assert_eq!(registry_manager.registry_count(), 0);
    }

    #[test]
    fn remote_dyn_filter_registry_cleanup_shares_query_scope_across_independent_leases() {
        let registry_manager = Arc::new(DynFilterRegistryManager::default());
        let query_id = test_query_id(1);

        let first_exec_like_lease = registry_manager.acquire_lease(query_id);
        let second_exec_like_lease = registry_manager.acquire_lease(query_id);

        drop(first_exec_like_lease);
        assert_eq!(registry_manager.registry_count(), 1);

        drop(second_exec_like_lease);
        assert_eq!(registry_manager.registry_count(), 0);
    }

    struct TestRegionQueryHandler;

    #[async_trait]
    impl RegionQueryHandler for TestRegionQueryHandler {
        async fn do_get(
            &self,
            _read_preference: ReadPreference,
            _request: common_query::request::QueryRequest,
        ) -> crate::error::Result<common_recordbatch::SendableRecordBatchStream> {
            unimplemented!("test only")
        }

        async fn handle_remote_dyn_filter_update(
            &self,
            _region_id: RegionId,
            _query_id: String,
            _update: api::v1::region::RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            unimplemented!("test only")
        }

        async fn handle_remote_dyn_filter_unregister(
            &self,
            _region_id: RegionId,
            _query_id: String,
            _unregister: api::v1::region::RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            unimplemented!("test only")
        }
    }

    #[test]
    fn try_with_new_distribution_preserves_remote_dyn_filter_producer_id() {
        let remote_dyn_filter_producer_id = RemoteDynFilterProducerId::new(42);

        // Build a plan whose schema contains "col1"
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i32).alias("col1")])
            .unwrap()
            .build()
            .unwrap();

        let schema = plan.schema().as_arrow().clone();
        let table = TableName::new("catalog", "schema", "table");
        let regions = vec![RegionId::new(1024, 1)];
        let query_ctx = QueryContext::arc();

        // Non-empty partition_cols so try_with_new_distribution can detect an overlap
        let mut partition_cols = AliasMapping::new();
        partition_cols.insert(
            "col1".to_string(),
            BTreeSet::from([ColumnExpr::new(Some(TableReference::bare("table")), "col1")]),
        );

        let session_state = SessionStateBuilder::new().build();

        let handler = Arc::new(TestRegionQueryHandler);
        let target_partition = 2;

        let exec = MergeScanExec::new(
            &session_state,
            table,
            regions,
            plan,
            &schema,
            handler,
            query_ctx,
            target_partition,
            partition_cols,
            Some(remote_dyn_filter_producer_id),
            false,
        )
        .unwrap();

        assert_eq!(
            exec.remote_dyn_filter_producer_id(),
            Some(remote_dyn_filter_producer_id)
        );

        // A distribution that differs from the current partitioning but shares a
        // column name present in partition_cols, so try_with_new_distribution
        // produces a clone instead of returning None.
        let new_dist = Distribution::HashPartitioned(vec![
            Arc::new(Column::new("col1", 0)),
            Arc::new(Column::new("col2", 1)),
        ]);

        let cloned = exec
            .try_with_new_distribution(new_dist)
            .expect("expected a cloned exec with overlapping partition col");

        assert_eq!(
            cloned.remote_dyn_filter_producer_id(),
            Some(remote_dyn_filter_producer_id),
            "try_with_new_distribution must preserve remote dynamic filter producer id"
        );
    }

    #[test]
    fn remote_dyn_filter_preflight_removes_parent_filter_after_dn_runtime_is_ready() {
        let remote_dyn_filter_producer_id = RemoteDynFilterProducerId::new(42);
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i32).alias("col1")])
            .unwrap()
            .build()
            .unwrap();

        let schema = plan.schema().as_arrow().clone();
        let table = TableName::new("catalog", "schema", "table");
        let regions = vec![RegionId::new(1024, 1)];
        let query_ctx = QueryContext::arc();
        let session_state = SessionStateBuilder::new().build();
        let handler = Arc::new(TestRegionQueryHandler);
        let exec = MergeScanExec::new(
            &session_state,
            table,
            regions,
            plan,
            &schema,
            handler,
            query_ctx,
            1,
            AliasMapping::new(),
            Some(remote_dyn_filter_producer_id),
            false,
        )
        .unwrap();
        let dyn_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("host", 0)) as Arc<_>],
            physical_lit(true) as _,
        )) as Arc<dyn datafusion_physical_expr::PhysicalExpr>;

        let propagation = exec
            .handle_child_pushdown_result(
                FilterPushdownPhase::Post,
                ChildPushdownResult {
                    parent_filters: vec![ChildFilterPushdownResult {
                        filter: dyn_filter,
                        child_results: vec![PushedDown::Yes],
                    }],
                    self_filters: Vec::new(),
                },
                &ConfigOptions::new(),
            )
            .unwrap();

        assert_eq!(exec.captured_remote_dyn_filters().len(), 1);
        assert!(matches!(propagation.filters.as_slice(), [PushedDown::Yes]));
    }

    #[test]
    fn scan_output_bytes_uses_plan_name() {
        let metrics = RecordBatchMetrics {
            plan_metrics: vec![PlanMetrics {
                plan: "SeqScan: region=1".to_string(),
                plan_name: REGION_SCAN_EXEC_NAME.to_string(),
                level: 0,
                metrics: vec![("output_bytes".to_string(), 42)],
            }],
            ..Default::default()
        };

        assert_eq!(scan_output_bytes(&metrics), 42);
    }

    #[test]
    fn scan_output_bytes_defaults_to_zero_without_region_scan() {
        let metrics = RecordBatchMetrics {
            plan_metrics: vec![PlanMetrics {
                plan: "ProjectionExec".to_string(),
                plan_name: "ProjectionExec".to_string(),
                level: 0,
                metrics: vec![("output_bytes".to_string(), 42)],
            }],
            ..Default::default()
        };

        assert_eq!(scan_output_bytes(&metrics), 0);
    }

    #[test]
    fn scan_output_bytes_sums_multiple_region_scans() {
        let metrics = RecordBatchMetrics {
            plan_metrics: vec![
                PlanMetrics {
                    plan: "RegionScanExec: region=1".to_string(),
                    plan_name: REGION_SCAN_EXEC_NAME.to_string(),
                    level: 0,
                    metrics: vec![("output_bytes".to_string(), 42)],
                },
                PlanMetrics {
                    plan: "RegionScanExec: region=2".to_string(),
                    plan_name: REGION_SCAN_EXEC_NAME.to_string(),
                    level: 0,
                    metrics: vec![("output_bytes".to_string(), 18)],
                },
            ],
            ..Default::default()
        };

        assert_eq!(scan_output_bytes(&metrics), 60);
    }

    #[test]
    fn report_region_query_load_updates_prometheus_metrics_when_enabled() {
        use store_api::metrics::{REGION_QUERY_CPU_TIME, REGION_QUERY_SCANNED_BYTES};

        let region_id = RegionId::new(1024, 10001);
        let region_id_label = region_id.to_string();
        let labels = [&region_id_label];
        let _ = REGION_QUERY_CPU_TIME.remove_label_values(&labels);
        let _ = REGION_QUERY_SCANNED_BYTES.remove_label_values(&labels);

        let metrics = RecordBatchMetrics {
            elapsed_compute: 42,
            plan_metrics: vec![PlanMetrics {
                plan: "RegionScanExec: region=1".to_string(),
                plan_name: REGION_SCAN_EXEC_NAME.to_string(),
                level: 0,
                metrics: vec![("output_bytes".to_string(), 24)],
            }],
            ..Default::default()
        };

        report_region_query_load(true, region_id, &metrics);

        assert_eq!(REGION_QUERY_CPU_TIME.with_label_values(&labels).get(), 42);
        assert_eq!(
            REGION_QUERY_SCANNED_BYTES.with_label_values(&labels).get(),
            24
        );

        let _ = REGION_QUERY_CPU_TIME.remove_label_values(&labels);
        let _ = REGION_QUERY_SCANNED_BYTES.remove_label_values(&labels);
    }
}
