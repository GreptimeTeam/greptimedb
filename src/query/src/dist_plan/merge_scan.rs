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

#[cfg(test)]
use std::cell::Cell;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ahash::{HashMap, HashSet};
use arrow_schema::{
    ArrowError, DataType, DataType as ArrowDataType, Field, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef, SortOptions,
};
use async_stream::stream;
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_plugins::GREPTIME_EXEC_READ_COST;
use common_query::request::QueryRequest;
use common_recordbatch::adapter::{RecordBatchMetrics, region_scan_output_bytes};
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
use datafusion_common::stats::Precision;
use datafusion_common::{Column as ColumnExpr, DataFusionError, Result, Statistics};
use datafusion_expr::{Expr, Extension, FetchType, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{Distribution, EquivalenceProperties, PhysicalSortExpr};
use datatypes::extension::json::{
    Json2ExtensionType, is_any_json_extension_type, is_json2_extension_type,
    is_legacy_json2_extension_type,
};
use futures_util::StreamExt;
use greptime_proto::v1::region::RegionRequestHeader;
use meter_core::data::ReadItem;
use meter_macros::read_meter;
use session::context::{
    FLIGHT_METRICS_HEARTBEAT_INTERVAL, QueryContextRef,
    SUPPORT_FLIGHT_METRICS_BEFORE_BATCH_EXTENSION_KEY,
};
use store_api::metrics::{REGION_QUERY_CPU_TIME, REGION_QUERY_SCANNED_BYTES};
use store_api::storage::RegionId;
use table::table_name::TableName;
use tokio::time;
use tokio::time::Instant;
use tracing::{Instrument, Span};

use crate::dist_plan::analyzer::AliasMapping;
use crate::dist_plan::analyzer::utils::patch_batch_timezone;
use crate::dist_plan::dyn_filter_bridge::{
    CapturedDynFilter, capture_remote_dyn_filters_for_pushdown,
    query_context_with_refreshed_initial_dyn_filter_regs,
    register_dyn_filter_subscribers_for_region, register_remote_dyn_filters,
};
use crate::dist_plan::{
    FilterId, RemoteDynFilterProducerId, RemoteDynFilterRegistryLease, Subscriber,
};
use crate::metrics::{MERGE_SCAN_ERRORS_TOTAL, MERGE_SCAN_POLL_ELAPSED, MERGE_SCAN_REGIONS};
use crate::options::{FlowQueryExtensions, remote_dyn_filter_pushdown_enabled_from_extensions};
use crate::query_engine::QueryEngineState;
use crate::region_query::RegionQueryHandlerRef;

fn query_engine_state_from_task_context(context: &TaskContext) -> Option<Arc<QueryEngineState>> {
    context.session_config().get_extension()
}

/// Returns a deterministic upper bound on rows emitted by the remote plan for
/// one region.
///
/// Only explicit caps and row-non-increasing wrappers are followed. Nodes that
/// may multiply rows fail open, so the result remains a sound upper bound.
fn remote_plan_row_bound(plan: &LogicalPlan) -> Option<usize> {
    match plan {
        LogicalPlan::Limit(limit) => {
            let input_bound = remote_plan_row_bound(&limit.input);
            match limit.get_fetch_type() {
                Ok(FetchType::Literal(Some(fetch))) => {
                    Some(input_bound.map_or(fetch, |bound| bound.min(fetch)))
                }
                _ => input_bound,
            }
        }
        LogicalPlan::Sort(sort) => {
            let input_bound = remote_plan_row_bound(&sort.input);
            sort.fetch
                .map(|fetch| input_bound.map_or(fetch, |bound| bound.min(fetch)))
                .or(input_bound)
        }
        LogicalPlan::Projection(projection) => remote_plan_row_bound(&projection.input),
        LogicalPlan::Filter(filter) => remote_plan_row_bound(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => remote_plan_row_bound(&alias.input),
        LogicalPlan::Window(window) => remote_plan_row_bound(&window.input),
        LogicalPlan::Repartition(repartition) => remote_plan_row_bound(&repartition.input),
        LogicalPlan::Distinct(distinct) => remote_plan_row_bound(distinct.input()),
        LogicalPlan::Aggregate(aggregate) => {
            if aggregate
                .group_expr
                .iter()
                .any(|expr| matches!(expr, Expr::GroupingSet(_)))
            {
                None
            } else if aggregate.group_expr.is_empty() {
                Some(1)
            } else {
                remote_plan_row_bound(&aggregate.input)
            }
        }
        _ => None,
    }
}

fn remote_dyn_filter_enabled(query_ctx: &QueryContextRef) -> Result<bool> {
    remote_dyn_filter_pushdown_enabled_from_extensions(&query_ctx.extensions())
        .map_err(|err| DataFusionError::External(Box::new(err)))
}

fn remote_schema_mismatch(message: impl Into<String>) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(ArrowError::SchemaError(message.into())), None)
}

fn record_merge_scan_schema_error() {
    MERGE_SCAN_ERRORS_TOTAL.inc();

    #[cfg(test)]
    TEST_MERGE_SCAN_SCHEMA_ERRORS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
thread_local! {
    // Prometheus counters are process-global and tests run concurrently. This
    // companion counter is incremented at the exact production increment site
    // and is scoped to the polling test thread, making delta assertions stable.
    static TEST_MERGE_SCAN_SCHEMA_ERRORS: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
fn merge_scan_schema_error_count_for_test() -> u64 {
    TEST_MERGE_SCAN_SCHEMA_ERRORS.with(Cell::get)
}

/// Returns true when the field is a JSON column, identified either by its
/// Arrow extension type (`greptime.json` / `greptime.json2`) or by its
/// `greptime:type=Json` marker.
fn is_json_field(field: &Field) -> bool {
    is_any_json_extension_type(field)
        || field
            .metadata()
            .get(datatypes::schema::TYPE_KEY)
            .map(String::as_ref)
            == Some("Json")
}

/// Returns true when two fields describe the same column semantics: same
/// name, same data type, and same nullability.
///
/// Arrow `Field` metadata is auxiliary information (indexing, encoding,
/// compression hints, ...) and does not participate in the comparison. This
/// applies to every field, JSON columns included: JSON identity keys
/// (`greptime:type`, `ARROW:extension:metadata`) are never compared here.
/// The wire/decoded physical-type difference of JSON columns is exempted
/// separately by [`json_fields_compatible`].
fn fields_semantically_equal(expected_field: &Field, actual_field: &Field) -> bool {
    expected_field.name() == actual_field.name()
        && expected_field.data_type() == actual_field.data_type()
        && expected_field.is_nullable() == actual_field.is_nullable()
}

/// Returns true when two fields are semantically equivalent JSON columns.
///
/// A JSON column is carried on the wire in its binary-encoded form (e.g.
/// `Binary` + `ARROW:extension:name=greptime.json` / `greptime:type=Json`) and,
/// after decoding on the remote side, in a concretized structured form (e.g.
/// `Struct(...)` / `List(...)` carrying the same extension metadata). Both forms
/// describe the same column, so the raw arrow data type must not be compared
/// directly. The JSON extension identity (`greptime:type`) and the JSON2
/// settings (`ARROW:extension:metadata`, e.g. type hints) are the semantic
/// parts of the field and must match.
fn json_fields_compatible(expected_field: &Field, actual_field: &Field) -> bool {
    is_json_field(expected_field)
        && is_json_field(actual_field)
        && expected_field.name() == actual_field.name()
        && expected_field.is_nullable() == actual_field.is_nullable()
        // Both must carry the same JSON marker.
        && expected_field.metadata().get(datatypes::schema::TYPE_KEY)
            == actual_field.metadata().get(datatypes::schema::TYPE_KEY)
        // JSON2 settings (type hints etc.) must match.
        && expected_field
            .metadata()
            .get(arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY)
            == actual_field
                .metadata()
                .get(arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY)
}

/// Validates the remote schema before positional column handling.
///
/// Field metadata (indexing/encoding/compression hints) is non-semantic at
/// this boundary and is ignored: two fields are equal when their name, data
/// type, and nullability match (see [`fields_semantically_equal`]). A
/// timestamp timezone difference is the only intentional exception, accepted
/// when the fields share the same timestamp unit, distinct timezones, and
/// equal name and nullability. Top-level Arrow schema metadata is
/// non-semantic at this boundary as well. JSON columns are compared
/// semantically (see [`json_fields_compatible`]) because their wire and
/// decoded representations use different physical arrow types.
fn validate_remote_schema(
    expected: &ArrowSchema,
    actual: &ArrowSchema,
    source: &str,
) -> Result<()> {
    if expected.fields().len() != actual.fields().len() {
        return Err(remote_schema_mismatch(format!(
            "MergeScan {source} schema field count mismatch: expected {}, actual {}",
            expected.fields().len(),
            actual.fields().len()
        )));
    }

    for (index, (expected_field, actual_field)) in expected
        .fields()
        .iter()
        .zip(actual.fields().iter())
        .enumerate()
    {
        // Field metadata does not participate in the semantic comparison.
        if fields_semantically_equal(expected_field, actual_field) {
            continue;
        }

        // JSON columns are equivalent in their binary wire form and their
        // decoded structured form; compare them semantically instead of
        // comparing the raw arrow data type.
        if json_fields_compatible(expected_field, actual_field) {
            continue;
        }

        // Intentionally mirrors Arrow Field equality properties, except timezone.
        let timezone_only_difference = matches!(
            (expected_field.data_type(), actual_field.data_type()),
            (
                ArrowDataType::Timestamp(expected_unit, expected_timezone),
                ArrowDataType::Timestamp(actual_unit, actual_timezone),
            ) if expected_unit == actual_unit
                && expected_timezone != actual_timezone
                && expected_field.name() == actual_field.name()
                && expected_field.is_nullable() == actual_field.is_nullable()
        );
        if !timezone_only_difference {
            return Err(remote_schema_mismatch(format!(
                "MergeScan {source} schema field mismatch at position {index}: expected {:?}, actual {:?}",
                expected_field, actual_field
            )));
        }
    }

    Ok(())
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

fn register_remote_dyn_filters_for_region(
    remote_dyn_filter_registry_lease: Option<&RemoteDynFilterRegistryLease>,
    captured_dyn_filters: &[CapturedDynFilter],
) {
    if let Some(remote_dyn_filter_registry_lease) = remote_dyn_filter_registry_lease {
        register_remote_dyn_filters(
            remote_dyn_filter_registry_lease.registry(),
            captured_dyn_filters,
        );
    }
}

struct SubscriberRollbackGuard<'a> {
    registry: &'a crate::dist_plan::QueryDynFilterRegistry,
    added: Vec<(FilterId, Subscriber)>,
}

impl<'a> SubscriberRollbackGuard<'a> {
    fn new(
        registry: &'a crate::dist_plan::QueryDynFilterRegistry,
        added: Vec<(FilterId, Subscriber)>,
    ) -> Self {
        Self { registry, added }
    }

    fn disarm(&mut self) {
        self.added.clear();
    }
}

impl Drop for SubscriberRollbackGuard<'_> {
    fn drop(&mut self) {
        for (filter_id, subscriber) in &self.added {
            self.registry.remove_subscriber(filter_id, subscriber);
        }
    }
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
        let arrow_schema = maybe_amend_json2_field(arrow_schema);
        let output_partition_count = Self::output_partition_count(regions.len(), target_partition);

        // States the output ordering of the plan.
        //
        // When the input plan is a sort, we can use the sort ordering as the output ordering
        // if the target partition is greater than the number of regions, which means we won't
        // break the ordering on merging (of MergeScan).
        //
        // Otherwise, we need to use the default ordering.
        let eq_properties = if let LogicalPlan::Sort(sort) = &plan
            && output_partition_count >= regions.len()
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
        let partitioning = Partitioning::Hash(partition_exprs, output_partition_count);

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

    /// Conservative row-count upper bound for all selected regions.
    ///
    /// This is not an expected cardinality: DataFusion receives it as an
    /// inexact estimate only because `Statistics` has no upper-bound precision.
    fn estimated_num_rows(&self) -> Precision<usize> {
        if self.regions.is_empty() {
            return Precision::Inexact(0);
        }

        let Some(rows_per_region) = remote_plan_row_bound(&self.plan) else {
            return Precision::Absent;
        };
        rows_per_region
            .checked_mul(self.regions.len())
            .map_or(Precision::Absent, Precision::Inexact)
    }

    /// Number of partitions populated by the region striping in [`Self::to_stream`].
    fn output_partition_count(num_regions: usize, target_partition: usize) -> usize {
        num_regions.max(1).min(target_partition.max(1))
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
        let target_partition =
            Self::output_partition_count(self.regions.len(), self.target_partition);
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
        let live_analyze_metrics = explain_verbose && self.query_ctx.live_analyze_metrics_enabled();
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
                let region_start = Instant::now();
                register_remote_dyn_filters_for_region(
                    remote_dyn_filter_registry_lease.as_ref(),
                    &captured_remote_dyn_filters,
                );
                let select_target_start = Instant::now();
                let target = region_query_handler
                    .select_target(read_preference, region_id)
                    .instrument(region_span.clone())
                    .await
                    .map_err(|e| {
                        MERGE_SCAN_ERRORS_TOTAL.inc();
                        DataFusionError::External(Box::new(e))
                    })?;
                let select_target_cost = select_target_start.elapsed();
                let mut subscriber_rollback =
                    remote_dyn_filter_registry_lease.as_ref().map(|lease| {
                        SubscriberRollbackGuard::new(
                            lease.registry(),
                            register_dyn_filter_subscribers_for_region(
                                lease.registry(),
                                region_id,
                                target.clone(),
                                &captured_remote_dyn_filters,
                            ),
                        )
                    });
                let mut region_query_ctx = query_context_with_refreshed_initial_dyn_filter_regs(
                    &query_ctx,
                    region_id,
                    &captured_remote_dyn_filters,
                );
                if live_analyze_metrics {
                    let remote_query_id = region_query_ctx.remote_query_id().map(str::to_string);
                    if let Some(remote_query_id) = remote_query_id {
                        region_query_ctx.set_extension(
                            SUPPORT_FLIGHT_METRICS_BEFORE_BATCH_EXTENSION_KEY,
                            remote_query_id,
                        );
                    }
                }
                let request = QueryRequest {
                    header: Some(RegionRequestHeader {
                        tracing_context: tracing_context.to_w3c(),
                        dbname: dbname.clone(),
                        query_context: Some((&region_query_ctx).into()),
                    }),
                    region_id,
                    plan: plan.clone(),
                };
                if explain_verbose {
                    common_telemetry::info!(
                        "Merge scan one region, partition: {}, region_id: {}",
                        partition,
                        region_id
                    );
                }

                let do_get_start = Instant::now();
                let do_get_result = region_query_handler
                    .do_get(&target, request)
                    .instrument(region_span.clone())
                    .await;
                if do_get_result.is_err() {
                    drop(subscriber_rollback.take());
                }
                let mut stream = do_get_result.map_err(|e| {
                    MERGE_SCAN_ERRORS_TOTAL.inc();
                    DataFusionError::External(Box::new(e))
                })?;

                if let Some(subscriber_rollback) = subscriber_rollback.as_mut() {
                    subscriber_rollback.disarm();
                }
                let mut advertised_schema = stream.schema().arrow_schema().clone();
                validate_remote_schema(
                    arrow_schema.as_ref(),
                    advertised_schema.as_ref(),
                    "advertised remote stream",
                )
                .inspect_err(|_| record_merge_scan_schema_error())?;
                let do_get_cost = select_target_cost + do_get_start.elapsed();

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
                loop {
                    let batch = if live_analyze_metrics {
                        match time::timeout(
                            FLIGHT_METRICS_HEARTBEAT_INTERVAL,
                            stream.next().instrument(region_span.clone()),
                        )
                        .await
                        {
                            Ok(batch) => batch,
                            Err(_) => {
                                if let Some(metrics) = stream.metrics() {
                                    let mut sub_stage_metrics =
                                        sub_stage_metrics_moved.lock().unwrap();
                                    sub_stage_metrics.insert(region_id, metrics);
                                }
                                continue;
                            }
                        }
                    } else {
                        stream.next().instrument(region_span.clone()).await
                    };
                    let Some(batch) = batch else {
                        break;
                    };
                    let poll_elapsed = poll_timer.elapsed();
                    poll_duration += poll_elapsed;

                    let batch = batch.map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let df_batch = batch.into_df_record_batch();
                    if !Arc::ptr_eq(&advertised_schema, df_batch.schema_ref()) {
                        validate_remote_schema(
                            arrow_schema.as_ref(),
                            df_batch.schema_ref().as_ref(),
                            "remote record batch",
                        )
                        .inspect_err(|_| record_merge_scan_schema_error())?;
                        advertised_schema = df_batch.schema_ref().clone();
                    }
                    let batch =
                        patch_batch_timezone(arrow_schema.clone(), df_batch.columns().to_vec())?;
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

        let hash_expr_col_names: HashSet<_> = hash_exprs
            .iter()
            .filter_map(|expr| {
                expr.downcast_ref::<Column>()
                    .map(|col_expr| col_expr.name())
            })
            .collect();

        let covers_all_partition_cols = self.partition_cols.values().all(|aliases| {
            aliases
                .iter()
                .any(|col| hash_expr_col_names.contains(col.name()))
        });
        if !covers_all_partition_cols {
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
                expr.downcast_ref::<Column>()
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
                Partitioning::Hash(overlaps, self.partition_count()),
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
        let sub_stage_metrics = self.sub_stage_metrics.lock().unwrap();
        let mut metrics: Vec<_> = sub_stage_metrics.iter().collect();
        metrics.sort_unstable_by_key(|(region_id, _)| **region_id);
        metrics
            .into_iter()
            .map(|(_, metrics)| metrics.clone())
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
        Self::output_partition_count(self.regions.len(), self.target_partition)
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

// If the schema has JSON2 field, AND the field is of empty Struct datatype, amend it with Binary
// datatype.
// This is a very hacky way to make it possible to query the whole JSON2 column. Because when
// querying a whole JSON2 column, like in the SQL `select * from ...`, we can't concretize the JSON2
// datatype from the query. Hence, the JSON2 datatype remains what in the column schema, i.e., empty
// Struct. An empty Struct is not alignable like any other concretized JSON2 datatypes, so to make
// the query work, we amend(rewrite) it to Binary datatype.
// Why the Binary datatype? Because underlying the scan and projection stage, the JSON2 data are
// variant shape, will be all converted to bytes.
// Anyway, this is not clean nor elegant. TODO(LFC) Maybe make it into some plan analyzer rule?
fn maybe_amend_json2_field(schema: &ArrowSchema) -> ArrowSchemaRef {
    let schema = schema.clone();
    let mut new_fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields().iter() {
        let new_field = if is_json2_extension_type(field)
            && matches!(field.data_type(), DataType::Struct(fields) if fields.is_empty())
        {
            let is_legacy_json2 = is_legacy_json2_extension_type(field);
            let mut new_field = field.as_ref().clone();
            new_field.set_data_type(DataType::Binary);
            if is_legacy_json2 {
                // Pre-type-hint JSON2 is identified partly by its Struct data type. Promote the
                // ephemeral field before rewriting it to Binary so later checks retain its JSON2
                // identity.
                new_field = new_field.with_extension_type(Json2ExtensionType::default());
            }
            Arc::new(new_field)
        } else {
            field.clone()
        };
        new_fields.push(new_field);
    }
    Arc::new(ArrowSchema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ))
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

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        if partition.is_some() {
            return Ok(Arc::new(Statistics::new_unknown(&self.arrow_schema)));
        }

        let mut statistics = Statistics::new_unknown(&self.arrow_schema);
        statistics.num_rows = self.estimated_num_rows();
        Ok(Arc::new(statistics))
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

fn region_scan_load(metrics: &RecordBatchMetrics) -> ReadItem {
    ReadItem {
        cpu_time: metrics.elapsed_compute as u64,
        table_scan: region_scan_output_bytes(metrics) as u64,
    }
}

fn report_region_query_load(region_id: RegionId, load: &ReadItem) {
    let region_id = region_id.to_string();
    REGION_QUERY_CPU_TIME
        .with_label_values(&[&region_id])
        .inc_by(load.cpu_time);
    REGION_QUERY_SCANNED_BYTES
        .with_label_values(&[&region_id])
        .inc_by(load.table_scan);
}

fn query_load_region_id(default_region_id: RegionId, metrics: &RecordBatchMetrics) -> RegionId {
    metrics
        .query_load_region_id
        .map(RegionId::from_u64)
        .unwrap_or(default_region_id)
}

impl Drop for MergeScanExec {
    fn drop(&mut self) {
        // Per-region Prometheus metrics can have high cardinality, so they are
        // controlled by `enable_per_region_metrics`. Region-owned counters for
        // heartbeat reporting are updated on datanodes when query metrics resolve.
        if !self.enable_per_region_metrics {
            return;
        }

        let metrics = self.sub_stage_metrics.lock().unwrap();
        for (region_id, metrics) in metrics.iter() {
            let load = region_scan_load(metrics);
            report_region_query_load(query_load_region_id(*region_id, metrics), &load);
        }
    }
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
    use std::collections::{BTreeSet, HashMap as StdHashMap};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use arrow::array::{Int64Array, TimestampMillisecondArray};
    use arrow_schema::extension::{
        EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, ExtensionType,
    };
    use arrow_schema::{DataType as TestArrowDataType, Field, Fields, TimeUnit};
    use async_trait::async_trait;
    use common_base::Plugins;
    use common_meta::peer::Peer;
    use common_query::request::{
        INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY, InitialDynFilterRegs,
    };
    use common_recordbatch::adapter::{PlanMetrics, RecordBatchMetrics};
    use common_recordbatch::{
        DfRecordBatch, EmptyRecordBatchStream, RecordBatch, RecordBatchStream,
    };
    use datafusion::config::ConfigOptions;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::filter_pushdown::ChildFilterPushdownResult;
    use datafusion_common::TableReference;
    use datafusion_expr::{LogicalPlanBuilder, col, lit};
    use datafusion_physical_expr::Distribution;
    use datafusion_physical_expr::expressions::{
        Column, DynamicFilterPhysicalExpr, lit as physical_lit,
    };
    use datatypes::extension::json::JsonExtensionType;
    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{Int64Vector, StringVector, TimestampMillisecondVector};
    use futures_util::{Stream, TryStreamExt};
    use session::ReadPreference;
    use session::context::QueryContext;
    use session::query_id::QueryId;
    use table::table::scan::REGION_SCAN_EXEC_NAME;
    use table::table_name::TableName;
    use tokio::sync::{Notify, oneshot};
    use uuid::Uuid;

    use super::*;
    use crate::dist_plan::DynFilterRegistryManager;
    use crate::options::QueryOptions;
    use crate::query_engine::{QueryEngineContext, QueryEngineState};
    use crate::region_query::RegionQueryHandler;

    fn test_target(id: u64) -> crate::region_query::RegionQueryTarget {
        crate::region_query::RegionQueryTarget::new(Peer {
            id,
            addr: format!("127.0.0.1:{id}"),
        })
    }

    fn test_query_id(value: u128) -> QueryId {
        QueryId::from(Uuid::from_u128(value))
    }

    #[test]
    fn test_amend_legacy_json2_field_preserves_json2_identity() {
        let field = Field::new("j", DataType::Struct(Fields::empty()), true).with_metadata(
            StdHashMap::from([
                (
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    JsonExtensionType::NAME.to_string(),
                ),
                (
                    EXTENSION_TYPE_METADATA_KEY.to_string(),
                    serde_json::json!({
                        "json_structure_settings": { "Structured": null }
                    })
                    .to_string(),
                ),
            ]),
        );

        let schema = maybe_amend_json2_field(&ArrowSchema::new(vec![field]));
        let field = schema.field(0);
        assert_eq!(&DataType::Binary, field.data_type());
        assert_eq!(Some(Json2ExtensionType::NAME), field.extension_type_name());
        assert!(is_json2_extension_type(field));
    }

    fn merge_scan_exec_with_sorted_input(
        region_count: u64,
        target_partition: usize,
    ) -> MergeScanExec {
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("ts")])
            .unwrap()
            .sort(vec![col("ts").sort(false, true)])
            .unwrap()
            .build()
            .unwrap();
        let regions = (0..region_count)
            .map(|region_number| RegionId::new(1024, region_number as u32))
            .collect();

        merge_scan_exec_with_plan(regions, plan, target_partition)
    }

    fn merge_scan_exec_with_plan(
        regions: Vec<RegionId>,
        plan: LogicalPlan,
        target_partition: usize,
    ) -> MergeScanExec {
        let session_state = SessionStateBuilder::new().build();
        let schema = plan.schema().as_arrow().clone();

        MergeScanExec::new(
            &session_state,
            // The table name is not relevant to these ordering metadata tests;
            // `MergeScanExec::new` requires one to model the production plan.
            TableName::new("catalog", "schema", "table"),
            regions,
            plan,
            &schema,
            Arc::new(TestRegionQueryHandler::default()),
            QueryContext::arc(),
            target_partition,
            AliasMapping::new(),
            None,
            false,
        )
        .unwrap()
    }

    fn task_context_with_engine_state(
        state: Arc<QueryEngineState>,
        query_ctx: QueryContextRef,
    ) -> Arc<TaskContext> {
        let mut session_state = state.session_state();
        session_state.config_mut().set_extension(state);
        QueryEngineContext::new(session_state, query_ctx).build_task_ctx()
    }

    fn remote_dyn_filter_test_exec(
        handler: crate::region_query::RegionQueryHandlerRef,
        query_ctx: QueryContextRef,
    ) -> MergeScanExec {
        let session_state = SessionStateBuilder::new().build();
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i32).alias("col1")])
            .unwrap()
            .build()
            .unwrap();
        let schema = plan.schema().as_arrow().clone();

        MergeScanExec::new(
            &session_state,
            TableName::new("catalog", "schema", "table"),
            vec![RegionId::new(1024, 1)],
            plan,
            &schema,
            handler,
            query_ctx,
            1,
            AliasMapping::new(),
            Some(RemoteDynFilterProducerId::new(42)),
            false,
        )
        .unwrap()
    }

    fn install_remote_dyn_filter(exec: &MergeScanExec) -> Arc<DynamicFilterPhysicalExpr> {
        let dyn_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("host", 0)) as Arc<_>],
            physical_lit(true) as _,
        ));
        exec.handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            ChildPushdownResult {
                parent_filters: vec![ChildFilterPushdownResult {
                    filter: dyn_filter.clone() as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
                    child_results: vec![PushedDown::Yes],
                }],
                self_filters: Vec::new(),
            },
            &ConfigOptions::new(),
        )
        .unwrap();
        dyn_filter
    }

    fn query_engine_state(
        handler: crate::region_query::RegionQueryHandlerRef,
    ) -> Arc<QueryEngineState> {
        Arc::new(QueryEngineState::new(
            catalog::memory::new_memory_catalog_manager().unwrap(),
            None,
            Some(handler),
            None,
            None,
            None,
            false,
            Plugins::default(),
            QueryOptions::default(),
        ))
    }

    fn empty_record_batch_stream(
        request: &common_query::request::QueryRequest,
    ) -> common_recordbatch::SendableRecordBatchStream {
        let arrow_schema = request.plan.schema().as_arrow().clone();
        Box::pin(EmptyRecordBatchStream::new(Arc::new(
            datatypes::schema::Schema::try_from(Arc::new(arrow_schema)).unwrap(),
        )))
    }

    fn pending_record_batch_stream(
        request: &common_query::request::QueryRequest,
    ) -> common_recordbatch::SendableRecordBatchStream {
        let stream = futures_util::stream::pending::<
            datafusion_common::Result<datafusion::arrow::record_batch::RecordBatch>,
        >();
        let arrow_schema = request.plan.schema().as_arrow().clone();
        let stream = RecordBatchStreamAdapter::new(Arc::new(arrow_schema), stream);
        Box::pin(
            common_recordbatch::adapter::RecordBatchStreamAdapter::try_new(Box::pin(stream))
                .unwrap(),
        )
    }

    #[test]
    fn merge_scan_does_not_advertise_ordering_when_partition_may_merge_regions() {
        let exec = merge_scan_exec_with_sorted_input(3, 2);

        assert!(
            exec.properties().output_ordering().is_none(),
            "target_partition < region_count means one output partition may concatenate multiple sorted region streams"
        );
    }

    #[test]
    fn merge_scan_advertises_ordering_when_each_partition_reads_at_most_one_region() {
        let exec = merge_scan_exec_with_sorted_input(3, 3);

        assert!(exec.properties().output_ordering().is_some());
    }

    #[test]
    fn merge_scan_advertises_ordering_when_partitions_exceed_regions() {
        let exec = merge_scan_exec_with_sorted_input(3, 4);

        assert!(exec.properties().output_ordering().is_some());
    }

    #[test]
    fn merge_scan_reports_populated_partition_count() {
        let cases = [(0, 10, 1), (1, 10, 1), (3, 2, 2), (5, 10, 5), (3, 0, 1)];

        for (region_count, target, expected) in cases {
            let exec = merge_scan_exec_with_sorted_input(region_count, target);
            assert_eq!(exec.partition_count(), expected);
            assert_eq!(
                exec.properties().output_partitioning().partition_count(),
                expected
            );
        }
    }

    #[test]
    fn merge_scan_reports_only_deterministic_plan_bounds() {
        use datafusion::functions_aggregate::expr_fn::count;
        use datafusion_expr::GroupingSet;

        let regions = vec![RegionId::new(1024, 1), RegionId::new(1024, 2)];
        let limited = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            merge_scan_exec_with_plan(regions.clone(), limited, 10)
                .partition_statistics(None)
                .unwrap()
                .num_rows,
            Precision::Inexact(100)
        );

        let large_bound = i32::MAX as usize + 1;
        let large_limit = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(large_bound))
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            merge_scan_exec_with_plan(vec![RegionId::new(1024, 1)], large_limit, 10)
                .partition_statistics(None)
                .unwrap()
                .num_rows,
            Precision::Inexact(large_bound)
        );

        let uncapped = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            merge_scan_exec_with_plan(regions.clone(), uncapped.clone(), 10)
                .partition_statistics(None)
                .unwrap()
                .num_rows,
            Precision::Absent
        );
        assert_eq!(
            merge_scan_exec_with_plan(Vec::new(), uncapped, 10)
                .partition_statistics(None)
                .unwrap()
                .num_rows,
            Precision::Inexact(0)
        );

        let global_aggregate = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(0))
            .unwrap()
            .aggregate(Vec::<Expr>::new(), vec![count(lit(1))])
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            merge_scan_exec_with_plan(regions.clone(), global_aggregate, 10)
                .partition_statistics(None)
                .unwrap()
                .num_rows,
            Precision::Inexact(2)
        );

        let grouping_sets = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .aggregate(
                vec![Expr::GroupingSet(GroupingSet::GroupingSets(vec![
                    vec![],
                    vec![col("col")],
                ]))],
                Vec::<Expr>::new(),
            )
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            merge_scan_exec_with_plan(regions, grouping_sets, 10)
                .partition_statistics(None)
                .unwrap()
                .num_rows,
            Precision::Absent
        );
    }

    #[test]
    fn sub_stage_metrics_are_sorted_by_region_id() {
        let exec = merge_scan_exec_with_sorted_input(0, 1);
        let higher_region = RegionId::new(1024, 2);
        let lower_region = RegionId::new(1024, 1);
        let higher_metrics = RecordBatchMetrics {
            plan_metrics: vec![PlanMetrics {
                plan: "higher region".to_string(),
                plan_name: "higher region".to_string(),
                level: 0,
                metrics: Vec::new(),
            }],
            ..Default::default()
        };
        let lower_metrics = RecordBatchMetrics {
            plan_metrics: vec![PlanMetrics {
                plan: "lower region".to_string(),
                plan_name: "lower region".to_string(),
                level: 0,
                metrics: Vec::new(),
            }],
            ..Default::default()
        };

        let mut sub_stage_metrics = exec.sub_stage_metrics.lock().unwrap();
        sub_stage_metrics.insert(higher_region, higher_metrics);
        sub_stage_metrics.insert(lower_region, lower_metrics);
        drop(sub_stage_metrics);

        let metrics = exec.sub_stage_metrics();
        let plans: Vec<_> = metrics
            .iter()
            .map(|metrics| metrics.plan_metrics[0].plan.as_str())
            .collect();

        assert_eq!(plans, ["lower region", "higher region"]);
    }

    #[test]
    fn remote_dyn_filter_producer_registration_defers_subscriber_registration() {
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

        register_remote_dyn_filters_for_region(Some(&lease), &captured.captured_dyn_filters);
        let region_query_ctx = query_context_with_refreshed_initial_dyn_filter_regs(
            &query_ctx,
            region_id,
            &captured.captured_dyn_filters,
        );

        let entries = lease.registry().entries();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].subscribers().is_empty());
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

    #[tokio::test]
    async fn failed_do_get_rolls_back_new_subscriber_without_starting_fanout() {
        let handler = Arc::new(FailingRegionQueryHandler::default());
        let query_ctx = QueryContext::arc();
        let state = Arc::new(QueryEngineState::new(
            catalog::memory::new_memory_catalog_manager().unwrap(),
            None,
            Some(handler.clone()),
            None,
            None,
            None,
            false,
            Plugins::default(),
            QueryOptions::default(),
        ));
        let lease = state
            .acquire_remote_dyn_filter_registry_lease(&query_ctx)
            .unwrap();
        handler.set_registry(lease.registry_arc_for_test());
        let task_ctx = task_context_with_engine_state(state, query_ctx.clone());
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i32).alias("col1")])
            .unwrap()
            .build()
            .unwrap();
        let schema = plan.schema().as_arrow().clone();
        let exec = MergeScanExec::new(
            &SessionStateBuilder::new().build(),
            TableName::new("catalog", "schema", "table"),
            vec![RegionId::new(1024, 1)],
            plan,
            &schema,
            handler.clone(),
            query_ctx,
            1,
            AliasMapping::new(),
            Some(RemoteDynFilterProducerId::new(42)),
            false,
        )
        .unwrap();
        let dyn_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("host", 0)) as Arc<_>],
            physical_lit(true) as _,
        )) as Arc<dyn datafusion_physical_expr::PhysicalExpr>;
        exec.handle_child_pushdown_result(
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

        let mut stream = exec.to_stream(task_ctx, 0).unwrap();
        assert!(stream.next().await.unwrap().is_err());
        assert_eq!(handler.do_get_calls.load(Ordering::SeqCst), 1);
        assert!(handler.saw_subscriber.load(Ordering::SeqCst));

        let entries = lease.registry().entries();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].subscribers().is_empty());
        assert!(!entries[0].fanout_started_for_test());
    }

    #[tokio::test]
    async fn aborting_pending_do_get_poll_rolls_back_subscriber_without_starting_fanout() {
        let handler = Arc::new(PendingDoGetHandler::default());
        let query_ctx = QueryContext::arc();
        let state = query_engine_state(handler.clone());
        let lease = state
            .acquire_remote_dyn_filter_registry_lease(&query_ctx)
            .unwrap();
        let exec = remote_dyn_filter_test_exec(handler.clone(), query_ctx.clone());
        install_remote_dyn_filter(&exec);
        let stream = exec
            .to_stream(task_context_with_engine_state(state, query_ctx), 0)
            .unwrap();

        let poll = tokio::spawn(async move {
            let mut stream = stream;
            stream.next().await
        });
        handler.do_get_entered.notified().await;
        let entries = lease.registry().entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].subscribers().len(), 1);
        assert!(!entries[0].fanout_started_for_test());

        poll.abort();
        assert!(poll.await.unwrap_err().is_cancelled());

        assert!(entries[0].subscribers().is_empty());
        assert!(!entries[0].fanout_started_for_test());
    }

    #[tokio::test]
    async fn failed_do_get_preserves_preexisting_duplicate_subscriber() {
        let handler = Arc::new(FailingRegionQueryHandler::default());
        let query_ctx = QueryContext::arc();
        let state = query_engine_state(handler.clone());
        let lease = state
            .acquire_remote_dyn_filter_registry_lease(&query_ctx)
            .unwrap();
        let exec = remote_dyn_filter_test_exec(handler.clone(), query_ctx.clone());
        install_remote_dyn_filter(&exec);

        register_remote_dyn_filters_for_region(Some(&lease), &exec.captured_remote_dyn_filters());
        let entry = lease.registry().entries().pop().unwrap();
        let subscriber = Subscriber::new(RegionId::new(1024, 1), test_target(1));
        assert!(matches!(
            lease
                .registry()
                .register_subscriber(entry.filter_id(), subscriber.clone()),
            crate::dist_plan::SubscriberRegistration::Added
        ));

        let mut stream = exec
            .to_stream(task_context_with_engine_state(state, query_ctx), 0)
            .unwrap();
        assert!(stream.next().await.unwrap().is_err());

        assert_eq!(entry.subscribers(), vec![subscriber]);
        assert!(!entry.fanout_started_for_test());
    }

    #[tokio::test]
    async fn failed_target_selection_registers_no_subscriber_or_fanout() {
        let handler = Arc::new(SelectTargetErrorHandler::default());
        let query_ctx = QueryContext::arc();
        let state = query_engine_state(handler.clone());
        let lease = state
            .acquire_remote_dyn_filter_registry_lease(&query_ctx)
            .unwrap();
        let exec = remote_dyn_filter_test_exec(handler.clone(), query_ctx.clone());
        install_remote_dyn_filter(&exec);

        let mut stream = exec
            .to_stream(task_context_with_engine_state(state, query_ctx), 0)
            .unwrap();
        assert!(stream.next().await.unwrap().is_err());

        assert_eq!(handler.select_target_calls.load(Ordering::SeqCst), 1);
        assert_eq!(handler.do_get_calls.load(Ordering::SeqCst), 0);
        let entries = lease.registry().entries();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].subscribers().is_empty());
        assert!(!entries[0].fanout_started_for_test());
    }

    #[tokio::test]
    async fn frozen_target_is_reused_for_do_get_update_and_unregister() {
        let (first_update_entered_tx, first_update_entered_rx) = oneshot::channel();
        let (release_first_update_tx, release_first_update_rx) = oneshot::channel();
        let (second_update_entered_tx, second_update_entered_rx) = oneshot::channel();
        let (release_second_update_tx, release_second_update_rx) = oneshot::channel();
        let (unregister_tx, unregister_rx) = oneshot::channel();
        let handler = Arc::new(RoutingRegionQueryHandler::new(
            first_update_entered_tx,
            release_first_update_rx,
            second_update_entered_tx,
            release_second_update_rx,
            unregister_tx,
        ));
        let query_ctx = QueryContext::arc();
        let state = query_engine_state(handler.clone());
        let lease = state
            .acquire_remote_dyn_filter_registry_lease(&query_ctx)
            .unwrap();
        let exec = remote_dyn_filter_test_exec(handler.clone(), query_ctx.clone());
        let dyn_filter = install_remote_dyn_filter(&exec);

        let stream = exec
            .to_stream(task_context_with_engine_state(state, query_ctx), 0)
            .unwrap();
        let poll = tokio::spawn(async move {
            let mut stream = stream;
            stream.next().await
        });
        handler.do_get_entered.notified().await;
        assert_eq!(handler.do_get_targets(), vec![1]);

        first_update_entered_rx.await.unwrap();
        dyn_filter.update(physical_lit(false) as _).unwrap();
        release_first_update_tx.send(()).unwrap();
        second_update_entered_rx.await.unwrap();
        assert_eq!(handler.update_targets(), vec![1, 1]);

        poll.abort();
        assert!(poll.await.unwrap_err().is_cancelled());
        drop(exec);
        drop(dyn_filter);
        release_second_update_tx.send(()).unwrap();
        unregister_rx.await.unwrap();
        assert_eq!(handler.unregister_targets(), vec![1]);
        drop(lease);
    }

    #[tokio::test]
    async fn immediate_eof_do_get_receives_refreshed_remote_dyn_filter_snapshot() {
        let handler = Arc::new(ImmediateEofRegionQueryHandler::default());
        let query_ctx = QueryContext::arc();
        let state = query_engine_state(handler.clone());
        let exec = remote_dyn_filter_test_exec(handler.clone(), query_ctx.clone());
        let dyn_filter = install_remote_dyn_filter(&exec);
        dyn_filter.update(physical_lit(false) as _).unwrap();

        let mut stream = exec
            .to_stream(task_context_with_engine_state(state, query_ctx), 0)
            .unwrap();
        assert!(stream.next().await.is_none());

        let registrations = handler.registrations();
        assert_eq!(registrations.regs.len(), 1);
        let snapshot = registrations.regs[0].initial_snapshot.as_ref().unwrap();
        assert!(snapshot.generation > 0);
        assert!(!snapshot.is_complete);
        assert_eq!(
            snapshot
                .payload
                .decode_datafusion_expr(
                    &TaskContext::default(),
                    &ArrowSchema::new(vec![arrow_schema::Field::new(
                        "host",
                        arrow_schema::DataType::Boolean,
                        false,
                    )]),
                    common_query::request::REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
                )
                .unwrap()
                .to_string(),
            "false"
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

    #[derive(Clone)]
    struct TestRegionResponse {
        advertised_schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    }

    #[derive(Default)]
    struct TestRegionQueryHandler {
        responses: HashMap<RegionId, TestRegionResponse>,
    }

    impl TestRegionQueryHandler {
        fn new(responses: impl IntoIterator<Item = (RegionId, RecordBatch)>) -> Self {
            let responses = responses
                .into_iter()
                .map(|(region_id, batch)| {
                    (
                        region_id,
                        TestRegionResponse {
                            advertised_schema: batch.schema.clone(),
                            batches: vec![batch],
                        },
                    )
                })
                .collect();
            Self { responses }
        }

        fn with_responses(
            responses: impl IntoIterator<Item = (RegionId, Arc<Schema>, Vec<RecordBatch>)>,
        ) -> Self {
            let responses = responses
                .into_iter()
                .map(|(region_id, advertised_schema, batches)| {
                    (
                        region_id,
                        TestRegionResponse {
                            advertised_schema,
                            batches,
                        },
                    )
                })
                .collect();
            Self { responses }
        }
    }

    struct TestRecordBatchStream {
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
        index: usize,
    }

    impl Stream for TestRecordBatchStream {
        type Item = common_recordbatch::error::Result<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Some(batch) = self.batches.get(self.index).cloned() {
                self.index += 1;
                Poll::Ready(Some(Ok(batch)))
            } else {
                Poll::Ready(None)
            }
        }
    }

    impl RecordBatchStream for TestRecordBatchStream {
        fn schema(&self) -> Arc<Schema> {
            self.schema.clone()
        }

        fn output_ordering(&self) -> Option<&[common_recordbatch::OrderOption]> {
            None
        }

        fn metrics(&self) -> Option<RecordBatchMetrics> {
            None
        }
    }

    #[derive(Default)]
    struct FailingRegionQueryHandler {
        do_get_calls: AtomicUsize,
        saw_subscriber: std::sync::atomic::AtomicBool,
        registry: std::sync::Mutex<Option<Arc<crate::dist_plan::QueryDynFilterRegistry>>>,
    }

    impl FailingRegionQueryHandler {
        fn set_registry(&self, registry: Arc<crate::dist_plan::QueryDynFilterRegistry>) {
            *self.registry.lock().unwrap() = Some(registry);
        }
    }

    #[async_trait]
    impl RegionQueryHandler for FailingRegionQueryHandler {
        async fn select_target(
            &self,
            _read_preference: ReadPreference,
            _region_id: RegionId,
        ) -> crate::error::Result<crate::region_query::RegionQueryTarget> {
            Ok(test_target(1))
        }

        async fn do_get(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            _request: common_query::request::QueryRequest,
        ) -> crate::error::Result<common_recordbatch::SendableRecordBatchStream> {
            self.do_get_calls.fetch_add(1, Ordering::SeqCst);
            self.saw_subscriber.store(
                self.registry
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|registry| {
                        registry
                            .entries()
                            .iter()
                            .any(|entry| !entry.subscribers().is_empty())
                    }),
                Ordering::SeqCst,
            );
            crate::error::UnimplementedSnafu {
                operation: "test do_get failure",
            }
            .fail()
        }

        async fn handle_remote_dyn_filter_update(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            _query_id: String,
            _update: api::v1::region::RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            unimplemented!("test only")
        }

        async fn handle_remote_dyn_filter_unregister(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            _query_id: String,
            _unregister: api::v1::region::RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            unimplemented!("test only")
        }
    }

    #[derive(Default)]
    struct PendingDoGetHandler {
        do_get_entered: Notify,
        never_complete: Notify,
    }

    #[async_trait]
    impl RegionQueryHandler for PendingDoGetHandler {
        async fn select_target(
            &self,
            _read_preference: ReadPreference,
            _region_id: RegionId,
        ) -> crate::error::Result<crate::region_query::RegionQueryTarget> {
            Ok(test_target(1))
        }

        async fn do_get(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            _request: common_query::request::QueryRequest,
        ) -> crate::error::Result<common_recordbatch::SendableRecordBatchStream> {
            self.do_get_entered.notify_one();
            self.never_complete.notified().await;
            unreachable!("the test aborts the pending do_get future")
        }

        async fn handle_remote_dyn_filter_update(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            _query_id: String,
            _update: api::v1::region::RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            unreachable!("fanout must not start while do_get is pending")
        }

        async fn handle_remote_dyn_filter_unregister(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            _query_id: String,
            _unregister: api::v1::region::RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            unreachable!("fanout must not start while do_get is pending")
        }
    }

    #[derive(Default)]
    struct SelectTargetErrorHandler {
        select_target_calls: AtomicUsize,
        do_get_calls: AtomicUsize,
    }

    #[async_trait]
    impl RegionQueryHandler for SelectTargetErrorHandler {
        async fn select_target(
            &self,
            _read_preference: ReadPreference,
            _region_id: RegionId,
        ) -> crate::error::Result<crate::region_query::RegionQueryTarget> {
            self.select_target_calls.fetch_add(1, Ordering::SeqCst);
            crate::error::UnimplementedSnafu {
                operation: "test target selection failure",
            }
            .fail()
        }

        async fn do_get(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            _request: common_query::request::QueryRequest,
        ) -> crate::error::Result<common_recordbatch::SendableRecordBatchStream> {
            self.do_get_calls.fetch_add(1, Ordering::SeqCst);
            unreachable!("do_get must not run after select_target fails")
        }

        async fn handle_remote_dyn_filter_update(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            _query_id: String,
            _update: api::v1::region::RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            unreachable!("fanout must not start after select_target fails")
        }

        async fn handle_remote_dyn_filter_unregister(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            _query_id: String,
            _unregister: api::v1::region::RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            unreachable!("fanout must not start after select_target fails")
        }
    }

    struct RoutingRegionQueryHandler {
        route: Mutex<crate::region_query::RegionQueryTarget>,
        do_get_entered: Notify,
        do_get_targets: Mutex<Vec<u64>>,
        update_targets: Mutex<Vec<u64>>,
        unregister_targets: Mutex<Vec<u64>>,
        update_calls: AtomicUsize,
        first_update_entered_tx: Mutex<Option<oneshot::Sender<()>>>,
        release_first_update_rx: Mutex<Option<oneshot::Receiver<()>>>,
        second_update_entered_tx: Mutex<Option<oneshot::Sender<()>>>,
        release_second_update_rx: Mutex<Option<oneshot::Receiver<()>>>,
        unregister_tx: Mutex<Option<oneshot::Sender<()>>>,
    }

    impl RoutingRegionQueryHandler {
        fn new(
            first_update_entered_tx: oneshot::Sender<()>,
            release_first_update_rx: oneshot::Receiver<()>,
            second_update_entered_tx: oneshot::Sender<()>,
            release_second_update_rx: oneshot::Receiver<()>,
            unregister_tx: oneshot::Sender<()>,
        ) -> Self {
            Self {
                route: Mutex::new(test_target(1)),
                do_get_entered: Notify::new(),
                do_get_targets: Mutex::new(Vec::new()),
                update_targets: Mutex::new(Vec::new()),
                unregister_targets: Mutex::new(Vec::new()),
                update_calls: AtomicUsize::new(0),
                first_update_entered_tx: Mutex::new(Some(first_update_entered_tx)),
                release_first_update_rx: Mutex::new(Some(release_first_update_rx)),
                second_update_entered_tx: Mutex::new(Some(second_update_entered_tx)),
                release_second_update_rx: Mutex::new(Some(release_second_update_rx)),
                unregister_tx: Mutex::new(Some(unregister_tx)),
            }
        }

        fn do_get_targets(&self) -> Vec<u64> {
            self.do_get_targets.lock().unwrap().clone()
        }

        fn update_targets(&self) -> Vec<u64> {
            self.update_targets.lock().unwrap().clone()
        }

        fn unregister_targets(&self) -> Vec<u64> {
            self.unregister_targets.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl RegionQueryHandler for RoutingRegionQueryHandler {
        async fn select_target(
            &self,
            _read_preference: ReadPreference,
            _region_id: RegionId,
        ) -> crate::error::Result<crate::region_query::RegionQueryTarget> {
            let mut route = self.route.lock().unwrap();
            let target = route.clone();
            *route = test_target(2);
            Ok(target)
        }

        async fn do_get(
            &self,
            target: &crate::region_query::RegionQueryTarget,
            request: common_query::request::QueryRequest,
        ) -> crate::error::Result<common_recordbatch::SendableRecordBatchStream> {
            self.do_get_targets.lock().unwrap().push(target.peer().id);
            self.do_get_entered.notify_one();
            Ok(pending_record_batch_stream(&request))
        }

        async fn handle_remote_dyn_filter_update(
            &self,
            target: &crate::region_query::RegionQueryTarget,
            _query_id: String,
            _update: api::v1::region::RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            self.update_targets.lock().unwrap().push(target.peer().id);
            match self.update_calls.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    if let Some(tx) = self.first_update_entered_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    let release = { self.release_first_update_rx.lock().unwrap().take() };
                    if let Some(release) = release {
                        let _ = release.await;
                    }
                }
                1 => {
                    if let Some(tx) = self.second_update_entered_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    let release = { self.release_second_update_rx.lock().unwrap().take() };
                    if let Some(release) = release {
                        let _ = release.await;
                    }
                }
                _ => {}
            }
            Ok(())
        }

        async fn handle_remote_dyn_filter_unregister(
            &self,
            target: &crate::region_query::RegionQueryTarget,
            _query_id: String,
            _unregister: api::v1::region::RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            self.unregister_targets
                .lock()
                .unwrap()
                .push(target.peer().id);
            if let Some(tx) = self.unregister_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct ImmediateEofRegionQueryHandler {
        registrations: Mutex<Option<InitialDynFilterRegs>>,
    }

    impl ImmediateEofRegionQueryHandler {
        fn registrations(&self) -> InitialDynFilterRegs {
            self.registrations.lock().unwrap().clone().unwrap()
        }
    }

    #[async_trait]
    impl RegionQueryHandler for ImmediateEofRegionQueryHandler {
        async fn select_target(
            &self,
            _read_preference: ReadPreference,
            _region_id: RegionId,
        ) -> crate::error::Result<crate::region_query::RegionQueryTarget> {
            Ok(test_target(1))
        }

        async fn do_get(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            request: common_query::request::QueryRequest,
        ) -> crate::error::Result<common_recordbatch::SendableRecordBatchStream> {
            let registrations = request
                .header
                .clone()
                .and_then(|header| header.query_context)
                .and_then(|query_context| {
                    query_context
                        .extensions
                        .get(INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY)
                        .cloned()
                })
                .map(|serialized| InitialDynFilterRegs::from_extension_value(&serialized).unwrap())
                .unwrap();
            *self.registrations.lock().unwrap() = Some(registrations);
            Ok(empty_record_batch_stream(&request))
        }

        async fn handle_remote_dyn_filter_update(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            _query_id: String,
            _update: api::v1::region::RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            Ok(())
        }

        async fn handle_remote_dyn_filter_unregister(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            _query_id: String,
            _unregister: api::v1::region::RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl RegionQueryHandler for TestRegionQueryHandler {
        async fn select_target(
            &self,
            _read_preference: ReadPreference,
            _region_id: RegionId,
        ) -> crate::error::Result<crate::region_query::RegionQueryTarget> {
            Ok(test_target(1))
        }

        async fn do_get(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            request: common_query::request::QueryRequest,
        ) -> crate::error::Result<common_recordbatch::SendableRecordBatchStream> {
            let response = self
                .responses
                .get(&request.region_id)
                .expect("test handler needs a response for every requested region");
            Ok(Box::pin(TestRecordBatchStream {
                schema: response.advertised_schema.clone(),
                batches: response.batches.clone(),
                index: 0,
            }))
        }

        async fn handle_remote_dyn_filter_update(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            _query_id: String,
            _update: api::v1::region::RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            unimplemented!("test only")
        }

        async fn handle_remote_dyn_filter_unregister(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            _query_id: String,
            _unregister: api::v1::region::RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            unimplemented!("test only")
        }
    }

    fn int64_schema(columns: &[&str]) -> Arc<Schema> {
        Arc::new(Schema::new(
            columns
                .iter()
                .map(|name| ColumnSchema::new(*name, ConcreteDataType::int64_datatype(), false))
                .collect(),
        ))
    }

    fn record_batch(schema: Arc<Schema>, columns: Vec<VectorRef>) -> RecordBatch {
        RecordBatch::new(schema, columns).expect("test record batch must match its schema")
    }

    fn expected_int64_schema() -> ArrowSchema {
        int64_schema(&["a", "b"]).arrow_schema().as_ref().clone()
    }

    fn merge_scan_exec(
        responses: Vec<(RegionId, RecordBatch)>,
        expected_schema: ArrowSchema,
        target_partition: usize,
    ) -> MergeScanExec {
        let regions = responses.iter().map(|(region_id, _)| *region_id).collect();
        merge_scan_exec_with_handler(
            regions,
            expected_schema,
            Arc::new(TestRegionQueryHandler::new(responses)),
            target_partition,
        )
    }

    fn merge_scan_exec_with_handler(
        regions: Vec<RegionId>,
        expected_schema: ArrowSchema,
        handler: Arc<TestRegionQueryHandler>,
        target_partition: usize,
    ) -> MergeScanExec {
        let plan = LogicalPlanBuilder::empty(true).build().unwrap();
        MergeScanExec::new(
            &SessionStateBuilder::new().build(),
            TableName::new("catalog", "schema", "table"),
            regions,
            plan,
            &expected_schema,
            handler,
            QueryContext::arc(),
            target_partition,
            AliasMapping::new(),
            None,
            false,
        )
        .unwrap()
    }

    async fn collect_merge_scan(
        exec: MergeScanExec,
    ) -> datafusion_common::Result<Vec<DfRecordBatch>> {
        exec.execute(0, Arc::new(TaskContext::default()))?
            .try_collect()
            .await
    }

    fn assert_int64_batch(batch: &DfRecordBatch, values: (i64, i64)) {
        assert_eq!(batch.schema().as_ref(), &expected_int64_schema());
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!((a.value(0), b.value(0)), values);
    }

    #[tokio::test]
    async fn qbs_merge_scan_remote_schema_identity_canonical_single_region() {
        let batch = record_batch(
            int64_schema(&["a", "b"]),
            vec![
                Arc::new(Int64Vector::from_slice([11])) as _,
                Arc::new(Int64Vector::from_slice([12])) as _,
            ],
        );
        let batches = collect_merge_scan(merge_scan_exec(
            vec![(RegionId::new(1024, 1), batch)],
            expected_int64_schema(),
            1,
        ))
        .await
        .unwrap();
        assert_eq!(batches.len(), 1);
        assert_int64_batch(&batches[0], (11, 12));
    }

    #[tokio::test]
    async fn qbs_merge_scan_remote_schema_identity_canonical_two_regions() {
        let batch = || {
            record_batch(
                int64_schema(&["a", "b"]),
                vec![
                    Arc::new(Int64Vector::from_slice([11])) as _,
                    Arc::new(Int64Vector::from_slice([12])) as _,
                ],
            )
        };
        let batches = collect_merge_scan(merge_scan_exec(
            vec![
                (RegionId::new(1024, 1), batch()),
                (RegionId::new(1024, 2), batch()),
            ],
            expected_int64_schema(),
            1,
        ))
        .await
        .unwrap();
        assert_eq!(batches.len(), 2);
        for batch in &batches {
            assert_int64_batch(batch, (11, 12));
        }
    }

    #[tokio::test]
    async fn qbs_merge_scan_remote_schema_identity_swapped_columns_never_relabels_positionally() {
        let batch = record_batch(
            int64_schema(&["b", "a"]),
            vec![
                Arc::new(Int64Vector::from_slice([2002])) as _,
                Arc::new(Int64Vector::from_slice([1002])) as _,
            ],
        );
        assert!(
            collect_merge_scan(merge_scan_exec(
                vec![(RegionId::new(1024, 1), batch)],
                expected_int64_schema(),
                1,
            ))
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn qbs_merge_scan_remote_schema_identity_allows_timestamp_timezone_only_patch() {
        let remote_arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "ts",
            TestArrowDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        )]));
        let remote_schema = Arc::new(Schema::try_from(remote_arrow_schema).unwrap());
        let timestamp_array: Arc<dyn arrow::array::Array> =
            Arc::new(TimestampMillisecondArray::from(vec![1002]).with_timezone("UTC"));
        let timestamp = TimestampMillisecondVector::try_from_arrow_array(timestamp_array).unwrap();
        let batch = record_batch(remote_schema, vec![Arc::new(timestamp) as _]);
        let expected_schema = ArrowSchema::new(vec![Field::new(
            "ts",
            TestArrowDataType::Timestamp(TimeUnit::Millisecond, Some("Asia/Shanghai".into())),
            false,
        )]);
        let batches = collect_merge_scan(merge_scan_exec(
            vec![(RegionId::new(1024, 1), batch)],
            expected_schema.clone(),
            1,
        ))
        .await
        .unwrap();
        assert_eq!(batches[0].schema().as_ref(), &expected_schema);
    }

    #[tokio::test]
    async fn qbs_merge_scan_remote_schema_identity_rejects_incompatible_type() {
        let batch = record_batch(
            Arc::new(Schema::new(vec![
                ColumnSchema::new("a", ConcreteDataType::string_datatype(), false),
                ColumnSchema::new("b", ConcreteDataType::int64_datatype(), false),
            ])),
            vec![
                Arc::new(StringVector::from_slice(&["not-an-int"])) as _,
                Arc::new(Int64Vector::from_slice([12])) as _,
            ],
        );
        assert!(
            collect_merge_scan(merge_scan_exec(
                vec![(RegionId::new(1024, 1), batch)],
                expected_int64_schema(),
                1,
            ))
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn qbs_merge_scan_remote_schema_identity_rejects_too_few_columns() {
        let batch = record_batch(
            int64_schema(&["a"]),
            vec![Arc::new(Int64Vector::from_slice([11])) as _],
        );
        assert!(
            collect_merge_scan(merge_scan_exec(
                vec![(RegionId::new(1024, 1), batch)],
                expected_int64_schema(),
                1,
            ))
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn qbs_merge_scan_remote_schema_identity_rejects_too_many_columns() {
        let batch = record_batch(
            int64_schema(&["a", "b", "extra"]),
            vec![
                Arc::new(Int64Vector::from_slice([11])) as _,
                Arc::new(Int64Vector::from_slice([12])) as _,
                Arc::new(Int64Vector::from_slice([13])) as _,
            ],
        );
        assert!(
            collect_merge_scan(merge_scan_exec(
                vec![(RegionId::new(1024, 1), batch)],
                expected_int64_schema(),
                1,
            ))
            .await
            .is_err()
        );
    }

    #[test]
    fn merge_scan_remote_schema_identity_allows_top_level_metadata_mismatch() {
        let fields = expected_int64_schema().fields().clone();
        let expected = ArrowSchema::new_with_metadata(
            fields.clone(),
            StdHashMap::from([("greptime:version".to_string(), "1".to_string())]),
        );
        let actual = ArrowSchema::new_with_metadata(
            fields,
            StdHashMap::from([("greptime:version".to_string(), "0".to_string())]),
        );
        assert!(validate_remote_schema(&expected, &actual, "test").is_ok());
    }

    /// Builds the metadata of a JSON column field. `wire_form` mirrors the
    /// binary-encoded representation (adds `ARROW:extension:name`), while the
    /// decoded structured form only carries the semantic keys.
    fn json_field_metadata(wire_form: bool, json_settings: &str) -> StdHashMap<String, String> {
        let mut metadata = StdHashMap::from([
            (datatypes::schema::TYPE_KEY.to_string(), "Json".to_string()),
            (
                arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY.to_string(),
                json_settings.to_string(),
            ),
        ]);
        if wire_form {
            metadata.insert(
                arrow_schema::extension::EXTENSION_TYPE_NAME_KEY.to_string(),
                "greptime.json".to_string(),
            );
        }
        metadata
    }

    #[test]
    fn merge_scan_remote_schema_identity_accepts_json_wire_binary_vs_decoded_struct() {
        // The merge-scan (expected) side carries a JSON2 column in its binary
        // wire form (Binary + `ARROW:extension:name`); the remote decoded
        // stream carries the concretized structured form (Struct + the same
        // semantic extension metadata, without the arrow extension name). This
        // mirrors the failing `json2_limit` case: the two representations must
        // validate as equal.
        let json_settings = r#"{"json_settings":{"type_hints":[]}}"#;
        let expected = ArrowSchema::new(vec![
            Field::new("j", TestArrowDataType::Binary, true)
                .with_metadata(json_field_metadata(true, json_settings)),
        ]);
        let actual =
            ArrowSchema::new(vec![
                Field::new(
                    "j",
                    TestArrowDataType::Struct(arrow_schema::Fields::from(vec![Arc::new(
                        Field::new("a", TestArrowDataType::Utf8View, true),
                    )])),
                    true,
                )
                .with_metadata(json_field_metadata(false, json_settings)),
            ]);
        assert!(validate_remote_schema(&expected, &actual, "test").is_ok());
    }

    #[test]
    fn merge_scan_remote_schema_identity_accepts_json_decoded_struct_vs_wire_binary() {
        // The reverse direction: expected side is the decoded structured form
        // while the actual remote stream advertises the binary wire form.
        let json_settings = r#"{"json_settings":{"type_hints":[]}}"#;
        let expected =
            ArrowSchema::new(vec![
                Field::new(
                    "j",
                    TestArrowDataType::Struct(arrow_schema::Fields::from(vec![Arc::new(
                        Field::new("a", TestArrowDataType::Utf8View, true),
                    )])),
                    true,
                )
                .with_metadata(json_field_metadata(false, json_settings)),
            ]);
        let actual = ArrowSchema::new(vec![
            Field::new("j", TestArrowDataType::Binary, true)
                .with_metadata(json_field_metadata(true, json_settings)),
        ]);
        assert!(validate_remote_schema(&expected, &actual, "test").is_ok());
    }

    #[test]
    fn merge_scan_remote_schema_identity_rejects_json_fields_with_different_json_settings() {
        // JSON2 settings (type hints) are semantic: two JSON columns with
        // different settings describe different logical structures and must be
        // rejected.
        let expected = ArrowSchema::new(vec![
            Field::new("j", TestArrowDataType::Binary, true).with_metadata(json_field_metadata(
                true,
                r#"{"json_settings":{"type_hints":[["a",{"JsonType":"Int64"}]]}}"#,
            )),
        ]);
        let actual =
            ArrowSchema::new(vec![
                Field::new(
                    "j",
                    TestArrowDataType::Struct(arrow_schema::Fields::from(vec![Arc::new(
                        Field::new("a", TestArrowDataType::Utf8View, true),
                    )])),
                    true,
                )
                .with_metadata(json_field_metadata(
                    false,
                    r#"{"json_settings":{"type_hints":[["a",{"JsonType":"String"}]]}}"#,
                )),
            ]);
        assert!(validate_remote_schema(&expected, &actual, "test").is_err());
    }

    #[test]
    fn merge_scan_remote_schema_identity_ignores_json_extension_metadata_when_physical_type_matches()
     {
        // Field metadata (including the JSON extension identity) is not part
        // of the semantic field identity: a JSON column in its binary wire
        // form and a plain Binary column share the same name, physical type,
        // and nullability, so they validate as equal.
        let expected = ArrowSchema::new(vec![
            Field::new("j", TestArrowDataType::Binary, true).with_metadata(json_field_metadata(
                true,
                r#"{"json_settings":{"type_hints":[]}}"#,
            )),
        ]);
        let actual = ArrowSchema::new(vec![Field::new("j", TestArrowDataType::Binary, true)]);
        assert!(validate_remote_schema(&expected, &actual, "test").is_ok());

        // The JSON compatibility exemption is scoped to JSON columns only: a
        // pair of non-JSON fields with different physical types is still
        // rejected even though one side is Binary.
        let plain_binary = ArrowSchema::new(vec![Field::new("v", TestArrowDataType::Binary, true)]);
        let plain_struct = ArrowSchema::new(vec![Field::new(
            "v",
            TestArrowDataType::Struct(arrow_schema::Fields::from(vec![Arc::new(Field::new(
                "a",
                TestArrowDataType::Utf8View,
                true,
            ))])),
            true,
        )]);
        assert!(validate_remote_schema(&plain_binary, &plain_struct, "test").is_err());
    }

    #[test]
    fn merge_scan_remote_schema_identity_ignores_field_metadata_but_rejects_nullability_mismatch() {
        let expected = expected_int64_schema();
        let metadata_mismatch = ArrowSchema::new_with_metadata(
            vec![
                expected
                    .field(0)
                    .as_ref()
                    .clone()
                    .with_metadata(StdHashMap::from([(
                        "remote".to_string(),
                        "different".to_string(),
                    )])),
                expected.field(1).as_ref().clone(),
            ],
            expected.metadata().clone(),
        );
        let nullability_mismatch = ArrowSchema::new_with_metadata(
            vec![
                expected.field(0).as_ref().clone().with_nullable(true),
                expected.field(1).as_ref().clone(),
            ],
            expected.metadata().clone(),
        );
        // Field metadata is auxiliary and does not participate in the
        // semantic field comparison.
        assert!(validate_remote_schema(&expected, &metadata_mismatch, "test").is_ok());
        assert!(validate_remote_schema(&expected, &nullability_mismatch, "test").is_err());
    }

    #[test]
    fn merge_scan_remote_schema_identity_ignores_skipping_index_field_metadata() {
        // Regression: a remote stream may advertise auxiliary field metadata
        // (e.g. `greptime:skipping_index`) that the expected schema does not
        // carry. Field metadata is not part of the semantic field identity
        // (name + data type + nullability) and must not fail validation.
        let skipping_index_metadata = StdHashMap::from([(
            "greptime:skipping_index".to_string(),
            r#"{"granularity":1,"false-positive-rate-in-10000":100,"index-type":"BloomFilter"}"#
                .to_string(),
        )]);
        let expected =
            ArrowSchema::new(vec![Field::new("value", TestArrowDataType::Float64, true)]);
        let actual_with_metadata = ArrowSchema::new(vec![
            Field::new("value", TestArrowDataType::Float64, true)
                .with_metadata(skipping_index_metadata),
        ]);
        assert!(validate_remote_schema(&expected, &actual_with_metadata, "test").is_ok());

        // A real data type mismatch is still rejected.
        let wrong_type =
            ArrowSchema::new(vec![Field::new("value", TestArrowDataType::Int64, true)]);
        assert!(validate_remote_schema(&expected, &wrong_type, "test").is_err());

        // A name mismatch is still rejected.
        let wrong_name =
            ArrowSchema::new(vec![Field::new("other", TestArrowDataType::Float64, true)]);
        assert!(validate_remote_schema(&expected, &wrong_name, "test").is_err());
    }

    #[test]
    fn merge_scan_remote_schema_identity_rejects_timestamp_timezone_plus_field_mismatches() {
        let expected = ArrowSchema::new(vec![Field::new(
            "ts",
            TestArrowDataType::Timestamp(TimeUnit::Millisecond, Some("Asia/Shanghai".into())),
            false,
        )]);
        let different_unit = ArrowSchema::new(vec![Field::new(
            "ts",
            TestArrowDataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            false,
        )]);
        let different_name = ArrowSchema::new(vec![Field::new(
            "other",
            TestArrowDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        )]);
        let nullability = ArrowSchema::new(vec![Field::new(
            "ts",
            TestArrowDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        )]);
        let metadata = ArrowSchema::new(vec![
            Field::new(
                "ts",
                TestArrowDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            )
            .with_metadata(StdHashMap::from([(
                "remote".to_string(),
                "different".to_string(),
            )])),
        ]);
        for actual in [&different_unit, &different_name, &nullability] {
            assert!(validate_remote_schema(&expected, actual, "test").is_err());
        }
        // Field metadata is non-semantic and is ignored even for timezone
        // pairs: a metadata difference on an otherwise timezone-only
        // difference is accepted.
        assert!(validate_remote_schema(&expected, &metadata, "test").is_ok());
    }

    #[test]
    fn merge_scan_remote_schema_identity_returns_arrow_schema_error() {
        let expected = expected_int64_schema();
        let actual = ArrowSchema::new(vec![Field::new("other", TestArrowDataType::Int64, false)]);
        match validate_remote_schema(&expected, &actual, "test").unwrap_err() {
            DataFusionError::ArrowError(error, None) => match error.as_ref() {
                ArrowError::SchemaError(message) => {
                    assert!(message.contains("field count mismatch"))
                }
                error => panic!("expected ArrowError::SchemaError, got {error:?}"),
            },
            error => panic!("expected DataFusionError::ArrowError(_, None), got {error:?}"),
        }
    }

    #[tokio::test]
    async fn merge_scan_remote_schema_identity_rejects_incompatible_empty_advertised_schema() {
        let region_id = RegionId::new(1024, 1);
        let exec = merge_scan_exec_with_handler(
            vec![region_id],
            expected_int64_schema(),
            Arc::new(TestRegionQueryHandler::with_responses(vec![(
                region_id,
                int64_schema(&["a"]),
                vec![],
            )])),
            1,
        );
        let errors_before = merge_scan_schema_error_count_for_test();
        assert!(collect_merge_scan(exec).await.is_err());
        assert_eq!(merge_scan_schema_error_count_for_test(), errors_before + 1);
    }

    #[tokio::test]
    async fn merge_scan_remote_schema_identity_allows_top_level_metadata_version_mismatch() {
        let fields = expected_int64_schema().fields().clone();
        let expected = ArrowSchema::new_with_metadata(
            fields.clone(),
            StdHashMap::from([("greptime:version".to_string(), "1".to_string())]),
        );
        let remote_schema = Arc::new(
            Schema::try_from(Arc::new(ArrowSchema::new_with_metadata(
                fields,
                StdHashMap::from([("greptime:version".to_string(), "0".to_string())]),
            )))
            .unwrap(),
        );
        let batch = record_batch(
            remote_schema.clone(),
            vec![
                Arc::new(Int64Vector::from_slice([11])) as _,
                Arc::new(Int64Vector::from_slice([12])) as _,
            ],
        );
        let batches = collect_merge_scan(merge_scan_exec_with_handler(
            vec![RegionId::new(1024, 1)],
            expected.clone(),
            Arc::new(TestRegionQueryHandler::with_responses(vec![(
                RegionId::new(1024, 1),
                remote_schema,
                vec![batch],
            )])),
            1,
        ))
        .await
        .unwrap();
        assert_eq!(batches[0].schema().as_ref(), &expected);
    }

    #[tokio::test]
    async fn merge_scan_remote_schema_identity_rejects_advertised_schema_inner_batch_mismatch() {
        let region_id = RegionId::new(1024, 1);
        let advertised_schema = int64_schema(&["a", "b"]);
        let inner_batch = record_batch(
            int64_schema(&["b", "a"]),
            vec![
                Arc::new(Int64Vector::from_slice([12])) as _,
                Arc::new(Int64Vector::from_slice([11])) as _,
            ],
        )
        .into_df_record_batch();
        let inner_schema = inner_batch.schema_ref().clone();
        let batch = RecordBatch::from_df_record_batch(advertised_schema.clone(), inner_batch);
        assert!(Arc::ptr_eq(
            advertised_schema.arrow_schema(),
            batch.schema.arrow_schema()
        ));
        assert!(!Arc::ptr_eq(
            advertised_schema.arrow_schema(),
            &inner_schema
        ));
        let exec = merge_scan_exec_with_handler(
            vec![region_id],
            expected_int64_schema(),
            Arc::new(TestRegionQueryHandler::with_responses(vec![(
                region_id,
                advertised_schema,
                vec![batch],
            )])),
            1,
        );
        let errors_before = merge_scan_schema_error_count_for_test();
        assert!(collect_merge_scan(exec).await.is_err());
        assert_eq!(merge_scan_schema_error_count_for_test(), errors_before + 1);
    }

    #[tokio::test]
    async fn merge_scan_remote_schema_identity_rejects_unchecked_inner_extra_column() {
        let region_id = RegionId::new(1024, 1);
        let advertised_schema = int64_schema(&["a", "b"]);
        let inner_batch = record_batch(
            int64_schema(&["a", "b", "extra"]),
            vec![
                Arc::new(Int64Vector::from_slice([11])) as _,
                Arc::new(Int64Vector::from_slice([12])) as _,
                Arc::new(Int64Vector::from_slice([13])) as _,
            ],
        )
        .into_df_record_batch();
        assert!(!Arc::ptr_eq(
            advertised_schema.arrow_schema(),
            inner_batch.schema_ref()
        ));
        let batch = RecordBatch::from_df_record_batch(advertised_schema.clone(), inner_batch);
        let exec = merge_scan_exec_with_handler(
            vec![region_id],
            expected_int64_schema(),
            Arc::new(TestRegionQueryHandler::with_responses(vec![(
                region_id,
                advertised_schema,
                vec![batch],
            )])),
            1,
        );
        assert!(collect_merge_scan(exec).await.is_err());
    }

    #[tokio::test]
    async fn merge_scan_remote_schema_identity_validates_structurally_equal_distinct_batch_schema()
    {
        let region_id = RegionId::new(1024, 1);
        let advertised_schema = int64_schema(&["a", "b"]);
        let inner_schema = Arc::new(
            Schema::try_from(Arc::new(advertised_schema.arrow_schema().as_ref().clone())).unwrap(),
        );
        let inner_batch = record_batch(
            inner_schema,
            vec![
                Arc::new(Int64Vector::from_slice([11])) as _,
                Arc::new(Int64Vector::from_slice([12])) as _,
            ],
        )
        .into_df_record_batch();
        assert_eq!(advertised_schema.arrow_schema(), inner_batch.schema_ref());
        assert!(!Arc::ptr_eq(
            advertised_schema.arrow_schema(),
            inner_batch.schema_ref()
        ));
        let batch = RecordBatch::from_df_record_batch(advertised_schema.clone(), inner_batch);
        let batches = collect_merge_scan(merge_scan_exec_with_handler(
            vec![region_id],
            expected_int64_schema(),
            Arc::new(TestRegionQueryHandler::with_responses(vec![(
                region_id,
                advertised_schema,
                vec![batch],
            )])),
            1,
        ))
        .await
        .unwrap();
        assert_eq!(batches.len(), 1);
        assert_int64_batch(&batches[0], (11, 12));
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

        let handler = Arc::new(TestRegionQueryHandler::default());
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
        let handler = Arc::new(TestRegionQueryHandler::default());
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

        assert_eq!(region_scan_output_bytes(&metrics), 42);
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

        assert_eq!(region_scan_output_bytes(&metrics), 0);
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

        assert_eq!(region_scan_output_bytes(&metrics), 60);
    }

    #[test]
    fn merge_scan_reports_region_query_load_on_drop() {
        use store_api::metrics::{REGION_QUERY_CPU_TIME, REGION_QUERY_SCANNED_BYTES};

        let region_id = RegionId::new(1024, 10002);
        let region_id_label = region_id.to_string();
        let labels = [&region_id_label];
        let _ = REGION_QUERY_CPU_TIME.remove_label_values(&labels);
        let _ = REGION_QUERY_SCANNED_BYTES.remove_label_values(&labels);

        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i32).alias("col1")])
            .unwrap()
            .build()
            .unwrap();
        let schema = plan.schema().as_arrow().clone();
        let exec = MergeScanExec::new(
            &SessionStateBuilder::new().build(),
            TableName::new("catalog", "schema", "table"),
            vec![region_id],
            plan,
            &schema,
            Arc::new(TestRegionQueryHandler::default()),
            QueryContext::arc(),
            1,
            AliasMapping::new(),
            None,
            true,
        )
        .unwrap();

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
        exec.sub_stage_metrics
            .lock()
            .unwrap()
            .insert(region_id, metrics);

        assert_eq!(REGION_QUERY_CPU_TIME.with_label_values(&labels).get(), 0);
        assert_eq!(
            REGION_QUERY_SCANNED_BYTES.with_label_values(&labels).get(),
            0
        );

        drop(exec);

        assert_eq!(REGION_QUERY_CPU_TIME.with_label_values(&labels).get(), 42);
        assert_eq!(
            REGION_QUERY_SCANNED_BYTES.with_label_values(&labels).get(),
            24
        );

        let _ = REGION_QUERY_CPU_TIME.remove_label_values(&labels);
        let _ = REGION_QUERY_SCANNED_BYTES.remove_label_values(&labels);
    }

    #[test]
    fn merge_scan_reports_query_load_with_metrics_region_id() {
        use store_api::metrics::{REGION_QUERY_CPU_TIME, REGION_QUERY_SCANNED_BYTES};

        let logical_region_id = RegionId::new(1024, 10002);
        let physical_region_id = RegionId::new(1024, 1);
        let logical_region_id_label = logical_region_id.to_string();
        let physical_region_id_label = physical_region_id.to_string();
        let logical_labels = [&logical_region_id_label];
        let physical_labels = [&physical_region_id_label];
        let _ = REGION_QUERY_CPU_TIME.remove_label_values(&logical_labels);
        let _ = REGION_QUERY_SCANNED_BYTES.remove_label_values(&logical_labels);
        let _ = REGION_QUERY_CPU_TIME.remove_label_values(&physical_labels);
        let _ = REGION_QUERY_SCANNED_BYTES.remove_label_values(&physical_labels);

        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i32).alias("col1")])
            .unwrap()
            .build()
            .unwrap();
        let schema = plan.schema().as_arrow().clone();
        let exec = MergeScanExec::new(
            &SessionStateBuilder::new().build(),
            TableName::new("catalog", "schema", "table"),
            vec![logical_region_id],
            plan,
            &schema,
            Arc::new(TestRegionQueryHandler::default()),
            QueryContext::arc(),
            1,
            AliasMapping::new(),
            None,
            true,
        )
        .unwrap();

        let metrics = RecordBatchMetrics {
            elapsed_compute: 42,
            query_load_region_id: Some(physical_region_id.as_u64()),
            plan_metrics: vec![PlanMetrics {
                plan: "RegionScanExec: region=1".to_string(),
                plan_name: REGION_SCAN_EXEC_NAME.to_string(),
                level: 0,
                metrics: vec![("output_bytes".to_string(), 24)],
            }],
            ..Default::default()
        };
        exec.sub_stage_metrics
            .lock()
            .unwrap()
            .insert(logical_region_id, metrics);

        drop(exec);

        assert_eq!(
            REGION_QUERY_CPU_TIME
                .with_label_values(&logical_labels)
                .get(),
            0
        );
        assert_eq!(
            REGION_QUERY_SCANNED_BYTES
                .with_label_values(&logical_labels)
                .get(),
            0
        );
        assert_eq!(
            REGION_QUERY_CPU_TIME
                .with_label_values(&physical_labels)
                .get(),
            42
        );
        assert_eq!(
            REGION_QUERY_SCANNED_BYTES
                .with_label_values(&physical_labels)
                .get(),
            24
        );

        let _ = REGION_QUERY_CPU_TIME.remove_label_values(&logical_labels);
        let _ = REGION_QUERY_SCANNED_BYTES.remove_label_values(&logical_labels);
        let _ = REGION_QUERY_CPU_TIME.remove_label_values(&physical_labels);
        let _ = REGION_QUERY_SCANNED_BYTES.remove_label_values(&physical_labels);
    }
}
