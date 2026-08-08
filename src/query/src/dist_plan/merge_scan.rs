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
use crate::dist_plan::region_statistics::RegionRowCountProviderRef;
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

/// Largest aggregate row-count estimate that may be reported.
///
/// DataFusion's join cardinality estimation multiplies two input row-count
/// estimates (`estimate_inner_join_cardinality`), so any estimate we report
/// must satisfy `estimate * estimate <= usize::MAX` to keep that multiplication
/// overflow-free in debug builds (which panic on overflow) and release builds
/// (which wrap). `i32::MAX` satisfies this with a wide margin on 64-bit
/// platforms (the only supported targets): `i32::MAX^2 ≈ 4.6e18` versus
/// `usize::MAX ≈ 1.8e19`. On 32-bit platforms `u16::MAX` is used so its square
/// still fits in `usize`.
const MAX_SAFE_NUM_ROWS_ESTIMATE: usize = if usize::BITS >= 64 {
    i32::MAX as usize
} else {
    u16::MAX as usize
};

/// Live provider row counts below this floor are reported as unknown.
///
/// Live counts differ by deployment (a standalone frontend has the
/// region-server-backed provider, a distributed frontend has none) and by
/// timing (flush state, freshly-created tables). Plan decisions that hinge on
/// a small live count — most importantly JoinSelection's CollectLeft
/// threshold — would then differ between environments, and the sqlness
/// goldens under `tests/cases/*/common` (shared between the standalone and
/// distributed runs via symlink) would flap. At or above the floor the count
/// cannot flip a CollectLeft decision relative to Absent, so reporting it is
/// safe; below the floor it is treated as unknown, which is what a
/// distributed frontend reports anyway.
///
/// The floor is the *byte*-threshold-safe value, not merely DataFusion's
/// 128K row threshold. DataFusion's `supports_collect_by_thresholds` checks
/// `total_byte_size` first against `hash_join_single_partition_threshold`
/// (default 1 MiB) and only then falls back to `num_rows` against
/// `hash_join_single_partition_threshold_rows` (default 128K). A
/// `ProjectionExec` above the scan recomputes `total_byte_size` from
/// `num_rows` and the projected primitive column widths
/// (`project_statistics` -> `calculate_total_byte_size`), so a narrow
/// all-primitive projection of a live count just above 128K rows can still
/// land below 1 MiB (e.g. 200_000 rows x a 4-byte Int32 = 800 KiB) and pick
/// CollectLeft standalone while the provider-absent distributed frontend
/// stays Partitioned. 1 MiB rows is the smallest count whose all-primitive
/// projections (minimum width 1 byte, e.g. Int8/UInt8) are always >= 1 MiB,
/// so a provider-derived estimate can never rehydrate a collectable byte
/// estimate.
const MIN_TRUSTED_LIVE_ROW_COUNT: usize = 1024 * 1024;

/// Best-effort upper bound on the number of rows the remote plan can emit
/// **per selected region**.
///
/// Returns `Some(bound)` when the remote plan has a statically-known row cap:
/// - a [`LogicalPlan::Limit`] with a literal `fetch`
/// - a [`LogicalPlan::Sort`] with a `fetch` (top-N sort)
/// - a global [`LogicalPlan::Aggregate`] (no grouping expressions), which
///   always emits exactly one row
///
/// Pass-through nodes ([`LogicalPlan::Projection`], [`LogicalPlan::Filter`],
/// [`LogicalPlan::SubqueryAlias`], [`LogicalPlan::Distinct`],
/// [`LogicalPlan::Window`], row-preserving [`LogicalPlan::Repartition`], and
/// plain grouping [`LogicalPlan::Aggregate`]s, which are row-non-increasing)
/// delegate to their input. When a cap is present, it is combined with the
/// input's bound via `min`, so a tighter nested cap (e.g. `LIMIT 50` over
/// `LIMIT 20`) is not discarded. Grouping-set aggregates can emit more rows
/// than their input and are therefore not pass-through: they fail open
/// (`None`). Plain scans, joins, and other nodes remain uncapped (`None`),
/// so the region total reported by [`MergeScanExec::partition_statistics`] is
/// not inflated beyond what the plan can actually produce (e.g. a bounded CTE
/// side of a join).
fn remote_plan_row_bound(plan: &LogicalPlan) -> Option<usize> {
    match plan {
        LogicalPlan::Limit(limit) => {
            let input_bound = remote_plan_row_bound(&limit.input);
            match limit.get_fetch_type() {
                Ok(FetchType::Literal(Some(fetch))) => Some(match input_bound {
                    Some(bound) => fetch.min(bound),
                    None => fetch,
                }),
                _ => input_bound,
            }
        }
        LogicalPlan::Sort(sort) => {
            let input_bound = remote_plan_row_bound(&sort.input);
            match sort.fetch {
                Some(fetch) => Some(match input_bound {
                    Some(bound) => fetch.min(bound),
                    None => fetch,
                }),
                None => input_bound,
            }
        }
        LogicalPlan::Projection(projection) => remote_plan_row_bound(&projection.input),
        LogicalPlan::Filter(filter) => remote_plan_row_bound(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => remote_plan_row_bound(&alias.input),
        LogicalPlan::Window(window) => remote_plan_row_bound(&window.input),
        // A repartition only redistributes rows; the child's cap survives.
        LogicalPlan::Repartition(repartition) => remote_plan_row_bound(&repartition.input),
        LogicalPlan::Aggregate(aggregate) => {
            // A grouping-set aggregate (group_expr containing an
            // `Expr::GroupingSet`) emits one row per grouping set and can
            // therefore produce more rows than its input: the child's bound
            // is not a sound upper bound, so fail open.
            if aggregate
                .group_expr
                .iter()
                .any(|expr| matches!(expr, Expr::GroupingSet(_)))
            {
                return None;
            }
            // A global aggregate (no grouping expressions) emits exactly one
            // row even from an empty input (e.g. `LIMIT 0`), so report its
            // one-row output instead of inheriting the child's (possibly
            // zero) bound.
            if aggregate.group_expr.is_empty() {
                return Some(1);
            }
            // A plain GROUP BY is row-non-increasing (each output row
            // corresponds to at least one input row): the child's bound
            // remains a sound upper bound.
            remote_plan_row_bound(&aggregate.input)
        }
        LogicalPlan::Distinct(distinct) => remote_plan_row_bound(distinct.input()),
        _ => None,
    }
}

/// Returns true when the remote plan never emits more rows than its input
/// for a single region, i.e. the region's stored row total remains a sound
/// upper bound even without a statically-known cap.
///
/// Row-non-increasing operators delegate to their input:
/// [`LogicalPlan::Projection`], [`LogicalPlan::Filter`],
/// [`LogicalPlan::SubqueryAlias`], [`LogicalPlan::Distinct`],
/// [`LogicalPlan::Window`], [`LogicalPlan::Repartition`] (row-preserving),
/// and plain grouping [`LogicalPlan::Aggregate`]s. A global aggregate emits
/// exactly one row. Scans (the base of the remote plan) are inherently
/// row-non-increasing. Joins, grouping-set aggregates, unions, and unknown
/// operators are NOT row-non-increasing: they can emit more rows than the
/// underlying region stores, so a live provider total would not be a sound
/// upper bound and must not be reported.
fn remote_plan_is_row_non_increasing(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::TableScan(_) | LogicalPlan::EmptyRelation(_) => true,
        LogicalPlan::Projection(projection) => remote_plan_is_row_non_increasing(&projection.input),
        LogicalPlan::Filter(filter) => remote_plan_is_row_non_increasing(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => remote_plan_is_row_non_increasing(&alias.input),
        LogicalPlan::Distinct(distinct) => remote_plan_is_row_non_increasing(distinct.input()),
        LogicalPlan::Window(window) => remote_plan_is_row_non_increasing(&window.input),
        LogicalPlan::Repartition(repartition) => {
            remote_plan_is_row_non_increasing(&repartition.input)
        }
        LogicalPlan::Sort(sort) => remote_plan_is_row_non_increasing(&sort.input),
        LogicalPlan::Limit(limit) => remote_plan_is_row_non_increasing(&limit.input),
        LogicalPlan::Aggregate(aggregate) => {
            if aggregate
                .group_expr
                .iter()
                .any(|expr| matches!(expr, Expr::GroupingSet(_)))
            {
                // Grouping sets can emit more rows than their input.
                return false;
            }
            if aggregate.group_expr.is_empty() {
                // A global aggregate emits exactly one row regardless of its
                // input's cardinality, so the region total remains an upper
                // bound (though [`remote_plan_row_bound`] already reports the
                // tighter cap of one row per region).
                return true;
            }
            remote_plan_is_row_non_increasing(&aggregate.input)
        }
        _ => false,
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
    let is_json = |field: &Field| {
        is_any_json_extension_type(field)
            || field
                .metadata()
                .get(datatypes::schema::TYPE_KEY)
                .map(String::as_ref)
                == Some("Json")
    };

    is_json(expected_field)
        && is_json(actual_field)
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
/// A timestamp timezone difference is the only intentional exception. It is
/// accepted by directly comparing the same timestamp unit, distinct timezones,
/// and equal name, nullability, and field metadata. Top-level Arrow schema
/// metadata is non-semantic at this boundary. JSON columns are additionally
/// compared semantically (see [`json_fields_compatible`]) because their wire
/// and decoded representations use different physical arrow types.
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
        if expected_field == actual_field {
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
                && expected_field.metadata() == actual_field.metadata()
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
    /// Optional best-effort row-count provider used by `partition_statistics`.
    /// `None` keeps statistics unknown (fail-open).
    region_row_count_provider: Option<RegionRowCountProviderRef>,
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
        region_row_count_provider: Option<RegionRowCountProviderRef>,
    ) -> Result<Self> {
        let arrow_schema = maybe_amend_json2_field(arrow_schema);

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
        // Declare only the partitions this scan actually populates: regions are
        // striped across partitions, so declaring `target_partition` partitions
        // for fewer regions leaves the extra ones permanently empty and hides
        // the skew from the optimizer — EnforceDistribution then skips the
        // round-robin repartition that would restore parallelism above a
        // single-region scan (e.g. the probe side of a CollectLeft hash join).
        let partitioning = Partitioning::Hash(
            partition_exprs,
            Self::output_partition_count(regions.len(), target_partition),
        );

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
            region_row_count_provider,
        })
    }

    /// Best-effort aggregate row-count estimate for the selected regions.
    ///
    /// **Determinism contract:** any estimate that can influence plan shape
    /// for the small tables used in sqlness goldens must be a function of the
    /// plan and the selected region set only — never of where the planner
    /// runs or when live statistics arrive. `tests/cases/distributed/common`
    /// is a symlink to `tests/cases/standalone/common`, so both environments
    /// exercise the same `.result` files: a standalone frontend has the
    /// region-server-backed provider while a distributed frontend has none,
    /// and any provider-dependent plan difference makes those goldens flap
    /// between runs. The rules, in order:
    ///
    /// 1. An empty selected region set is exactly 0 rows — deterministic,
    ///    provider not consulted.
    /// 2. A plan-structural bound ([`remote_plan_row_bound`], e.g. a CTE side
    ///    with `LIMIT 50`) is reported as the per-region bound scaled by the
    ///    number of selected regions, provider not consulted. The remote plan
    ///    is executed independently per region (see [`Self::to_stream`]), so a
    ///    whole-scan bound must multiply the per-region cap by the region
    ///    count; checked arithmetic keeps overflow Absent. This is what lets
    ///    DataFusion's `JoinSelection` pick the bounded side as the hash-join
    ///    build side (`try_collect_left` needs only one side's statistics),
    ///    identically in standalone and distributed mode. The provider total
    ///    is deliberately NOT used to refine the bound downward — that
    ///    refinement would differ between environments.
    /// 3. Otherwise, when the remote plan is row-non-increasing (it never
    ///    emits more rows than its input; see
    ///    [`remote_plan_is_row_non_increasing`]), the live provider total is
    ///    reported only when it is at least [`MIN_TRUSTED_LIVE_ROW_COUNT`]:
    ///    small or freshly-created tables (every sqlness table) plan
    ///    identically whether or not the provider is present, while genuinely
    ///    large tables — where the estimate actually matters for join-side
    ///    selection — still benefit. Expanding plans (joins, grouping sets,
    ///    etc.) can emit more rows than the underlying regions store, so a
    ///    live provider total is NOT a sound upper bound for them and they
    ///    remain Absent.
    ///    [`Precision::Absent`] otherwise (no provider, unknown region,
    ///    below-floor count, an unsafe total, or an oversized estimate) so
    ///    incomplete statistics never look complete.
    ///
    /// Aggregation is checked, not saturating, by choice: per-region counts are
    /// `u64` while DataFusion's `Statistics::num_rows` is `usize`, and
    /// DataFusion's join cardinality estimation multiplies input row counts
    /// (`estimate_inner_join_cardinality`). Saturating an overflowing sum to a
    /// near-maximum value would make that multiplication panic in debug builds
    /// or wrap in release builds, so an overflowing sum is reported as unknown
    /// instead. The final `u64` -> `usize` conversion is also checked, and an
    /// estimate above [`MAX_SAFE_NUM_ROWS_ESTIMATE`] is reported as unknown
    /// even when the sum itself fits, because two such estimates could still
    /// overflow DataFusion's multiplication.
    fn estimated_num_rows(&self) -> Precision<usize> {
        if self.regions.is_empty() {
            return Precision::Inexact(0);
        }
        if let Some(bound) = remote_plan_row_bound(&self.plan) {
            // The remote plan is executed once per selected region, so the
            // whole-scan bound is the per-region cap scaled by the region
            // count. An overflowing product is not a conservative bound and
            // stays Absent.
            let Some(bound) = bound.checked_mul(self.regions.len()) else {
                return Precision::Absent;
            };
            if bound > MAX_SAFE_NUM_ROWS_ESTIMATE {
                return Precision::Absent;
            }
            return Precision::Inexact(bound);
        }
        // Without a structural cap, a live provider total is only a sound
        // upper bound when the remote plan is row-non-increasing. Joins,
        // grouping sets, and other expanding plans can emit more rows than
        // the selected regions store, so they must stay Absent even with a
        // provider.
        if !remote_plan_is_row_non_increasing(&self.plan) {
            return Precision::Absent;
        }
        let Some(provider) = self.region_row_count_provider.as_ref() else {
            return Precision::Absent;
        };
        let mut total: u64 = 0;
        for region in &self.regions {
            let Some(region_rows) = provider.row_count(*region) else {
                return Precision::Absent;
            };
            let Some(next) = total.checked_add(region_rows) else {
                return Precision::Absent;
            };
            total = next;
        }
        let Some(total) = usize::try_from(total).ok() else {
            return Precision::Absent;
        };
        if !(MIN_TRUSTED_LIVE_ROW_COUNT..=MAX_SAFE_NUM_ROWS_ESTIMATE).contains(&total) {
            return Precision::Absent;
        }
        Precision::Inexact(total)
    }

    /// Number of output partitions this scan actually populates. Must stay in
    /// sync with the region striping in [`Self::to_stream`].
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
        // Stride by the declared partition count so every region is covered
        // by exactly the partitions DataFusion will actually execute.
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
                expr.as_any()
                    .downcast_ref::<Column>()
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
                Partitioning::Hash(overlaps, self.properties.partitioning.partition_count()),
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
            region_row_count_provider: self.region_row_count_provider.clone(),
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

    /// Number of output partitions this scan actually populates. This is the
    /// effective count used by [`PlanProperties`], [`Self::to_stream`] region
    /// striping, and [`Self::try_with_new_distribution`] — not the raw
    /// `target_partition` hint, which can exceed the region count (declaring
    /// empty partitions would hide the skew from the optimizer).
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

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        // Per-partition row counts are unknown: the hash distribution of rows
        // across output partitions is data-dependent. The whole-plan estimate
        // is what JoinSelection and other consumers use for join-order and
        // collect-left decisions.
        if partition.is_some() {
            return Ok(Statistics::new_unknown(&self.arrow_schema));
        }
        let mut statistics = Statistics::new_unknown(&self.arrow_schema);
        statistics.num_rows = self.estimated_num_rows();
        Ok(statistics)
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
    use datafusion::common::NullEquality;
    use datafusion::config::ConfigOptions;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_optimizer::join_selection::JoinSelection;
    use datafusion::physical_plan::filter_pushdown::ChildFilterPushdownResult;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion_common::TableReference;
    use datafusion_common::stats::Precision;
    use datafusion_expr::{JoinType, LogicalPlanBuilder, col, lit};
    use datafusion_physical_expr::expressions::{
        Column, DynamicFilterPhysicalExpr, lit as physical_lit,
    };
    use datafusion_physical_expr::{Distribution, PhysicalExpr};
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
    use crate::dist_plan::{DynFilterRegistryManager, RegionRowCountProvider};
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
        let session_state = SessionStateBuilder::new().build();
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("ts")])
            .unwrap()
            .sort(vec![col("ts").sort(false, true)])
            .unwrap()
            .build()
            .unwrap();
        let schema = plan.schema().as_arrow().clone();
        let regions = (0..region_count)
            .map(|region_number| RegionId::new(1024, region_number as u32))
            .collect();

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
            None,
        )
        .unwrap()
    }

    /// A deterministic [`RegionRowCountProvider`] backed by a static map.
    #[derive(Clone)]
    struct TestRegionRowCountProvider {
        rows: StdHashMap<RegionId, u64>,
    }

    impl TestRegionRowCountProvider {
        fn new(rows: Vec<(RegionId, u64)>) -> Self {
            Self {
                rows: rows.into_iter().collect(),
            }
        }
    }

    impl RegionRowCountProvider for TestRegionRowCountProvider {
        fn row_count(&self, region: RegionId) -> Option<u64> {
            self.rows.get(&region).copied()
        }
    }

    fn merge_scan_exec_with_row_count_provider(
        regions: Vec<RegionId>,
        provider: Option<RegionRowCountProviderRef>,
    ) -> MergeScanExec {
        let session_state = SessionStateBuilder::new().build();
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .build()
            .unwrap();
        let schema = plan.schema().as_arrow().clone();

        MergeScanExec::new(
            &session_state,
            TableName::new("catalog", "schema", "table"),
            regions,
            plan,
            &schema,
            Arc::new(TestRegionQueryHandler::default()),
            QueryContext::arc(),
            1,
            AliasMapping::new(),
            None,
            false,
            provider,
        )
        .unwrap()
    }

    fn merge_scan_exec_with_plan_and_row_count_provider(
        regions: Vec<RegionId>,
        plan: LogicalPlan,
        provider: Option<RegionRowCountProviderRef>,
    ) -> MergeScanExec {
        let session_state = SessionStateBuilder::new().build();
        let schema = plan.schema().as_arrow().clone();

        MergeScanExec::new(
            &session_state,
            TableName::new("catalog", "schema", "table"),
            regions,
            plan,
            &schema,
            Arc::new(TestRegionQueryHandler::default()),
            QueryContext::arc(),
            1,
            AliasMapping::new(),
            None,
            false,
            provider,
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
            None,
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
    fn partition_count_reports_effective_output_partitions() {
        // `partition_count()` must report the count DataFusion actually
        // executes (`output_partition_count`), not the raw `target_partition`
        // hint: zero/fewer regions and targets larger than the region count
        // all collapse to the populated partition count.
        let cases = [
            (0, 10, 1), // no regions: one (empty) partition
            (1, 10, 1), // fewer regions than target
            (3, 3, 3),  // equal
            (3, 2, 2),  // target caps
            (5, 10, 5), // more regions than target
        ];
        for (region_count, target, expected) in cases {
            let exec = merge_scan_exec_with_sorted_input(region_count, target);
            assert_eq!(
                exec.partition_count(),
                expected,
                "region_count={region_count}, target={target}"
            );
            // The accessor must agree with the properties DataFusion reads.
            assert_eq!(
                exec.partition_count(),
                exec.properties().output_partitioning().partition_count()
            );
        }
    }

    #[test]
    fn partition_count_agrees_with_properties_and_striping_after_distribution_rewrite() {
        // try_with_new_distribution must keep the same effective partition
        // count as PlanProperties and to_stream's striping stride.
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("ts")])
            .unwrap()
            .build()
            .unwrap();
        let schema = plan.schema().as_arrow().clone();
        let mut partition_cols = AliasMapping::new();
        partition_cols.insert(
            "ts".to_string(),
            BTreeSet::from([ColumnExpr::new(Some(TableReference::bare("table")), "ts")]),
        );
        let exec = MergeScanExec::new(
            &SessionStateBuilder::new().build(),
            TableName::new("catalog", "schema", "table"),
            vec![
                RegionId::new(1024, 1),
                RegionId::new(1024, 2),
                RegionId::new(1024, 3),
            ],
            plan,
            &schema,
            Arc::new(TestRegionQueryHandler::default()),
            QueryContext::arc(),
            10,
            partition_cols,
            None,
            false,
            None,
        )
        .unwrap();

        assert_eq!(exec.partition_count(), 3);
        assert_eq!(
            exec.partition_count(),
            exec.properties().output_partitioning().partition_count()
        );

        let new_dist = Distribution::HashPartitioned(vec![
            Arc::new(Column::new("ts", 0)),
            Arc::new(Column::new("other", 1)),
        ]);
        let rewritten = exec
            .try_with_new_distribution(new_dist)
            .expect("expected a cloned exec with overlapping partition col");

        assert_eq!(rewritten.partition_count(), 3);
        assert_eq!(
            rewritten.partition_count(),
            rewritten
                .properties()
                .output_partitioning()
                .partition_count()
        );
        // to_stream strides by the same count.
        assert_eq!(
            rewritten.partition_count(),
            MergeScanExec::output_partition_count(rewritten.regions.len(), 10)
        );
    }

    #[test]
    fn partition_statistics_reports_inexact_num_rows_when_all_regions_have_estimates() {
        let region1 = RegionId::new(1024, 1);
        let region2 = RegionId::new(1024, 2);
        // Counts must clear MIN_TRUSTED_LIVE_ROW_COUNT (1 MiB rows) to be
        // reported, so each region must hold at least half of that.
        let provider = Arc::new(TestRegionRowCountProvider::new(vec![
            (region1, 600_000),
            (region2, 600_000),
        ])) as RegionRowCountProviderRef;
        let exec = merge_scan_exec_with_row_count_provider(vec![region1, region2], Some(provider));

        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(1_200_000));
        // Only num_rows is reported; byte size and column stats stay unknown.
        assert_eq!(stats.total_byte_size, Precision::Absent);
        assert!(
            stats
                .column_statistics
                .iter()
                .all(|column| column.null_count.is_exact().is_none())
        );
    }

    #[test]
    fn partition_statistics_is_absent_when_any_region_estimate_is_missing() {
        let region1 = RegionId::new(1024, 1);
        let region2 = RegionId::new(1024, 2);
        // The provider only knows region1: the incomplete estimate must stay absent.
        let provider = Arc::new(TestRegionRowCountProvider::new(vec![(region1, 42)]))
            as RegionRowCountProviderRef;
        let exec = merge_scan_exec_with_row_count_provider(vec![region1, region2], Some(provider));

        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Absent);
    }

    #[test]
    fn partition_statistics_is_absent_without_provider() {
        let exec = merge_scan_exec_with_row_count_provider(
            vec![RegionId::new(1024, 1), RegionId::new(1024, 2)],
            None,
        );

        // Fail-open: without a provider the estimate is unknown, never fabricated.
        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Absent);
    }

    #[test]
    fn partition_statistics_reports_zero_for_empty_selected_region_set() {
        // An empty selected-region set scans no regions, so it emits zero
        // rows — deterministically, with or without a provider, so both the
        // standalone and distributed frontends plan it identically.
        let provider =
            Arc::new(TestRegionRowCountProvider::new(vec![])) as RegionRowCountProviderRef;
        let exec = merge_scan_exec_with_row_count_provider(vec![], Some(provider));
        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(0));

        let exec = merge_scan_exec_with_row_count_provider(vec![], None);
        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(0));
    }

    #[test]
    fn partition_statistics_stays_absent_below_trusted_live_count_floor() {
        let region1 = RegionId::new(1024, 1);
        let region2 = RegionId::new(1024, 2);
        let provider = Arc::new(TestRegionRowCountProvider::new(vec![
            (region1, 42),
            (region2, 58),
        ])) as RegionRowCountProviderRef;
        let exec = merge_scan_exec_with_row_count_provider(vec![region1, region2], Some(provider));

        // A small live count could flip JoinSelection's CollectLeft decision
        // relative to a frontend without the provider (distributed mode), and
        // the standalone/distributed sqlness runs share the same goldens. It
        // must therefore be treated as unknown.
        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Absent);
    }

    #[test]
    fn bounded_plan_reports_region_scaled_bound_without_provider() {
        // The distributed frontend has no row-count provider, but a
        // plan-structural bound (a CTE side with `LIMIT 50`) must still be
        // reported so the CTE join fix works — and plans identically — in
        // both standalone and distributed mode. The remote plan runs once per
        // selected region, so two regions can emit up to 2 x 50 rows.
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .build()
            .unwrap();
        let exec = merge_scan_exec_with_plan_and_row_count_provider(
            vec![RegionId::new(1024, 1), RegionId::new(1024, 2)],
            plan,
            None,
        );

        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(100));
    }

    #[test]
    fn structural_bound_is_scaled_by_selected_region_count() {
        // The remote plan executes independently per region (`to_stream`
        // stripes regions over partitions but each runs the same plan), so a
        // whole-scan `LIMIT 50` bound is 50 x region_count, not 50. This is
        // the conservative upper bound JoinSelection compares.
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .build()
            .unwrap();
        let exec = merge_scan_exec_with_plan_and_row_count_provider(
            vec![
                RegionId::new(1024, 1),
                RegionId::new(1024, 2),
                RegionId::new(1024, 3),
            ],
            plan,
            None,
        );

        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(150));
    }

    #[test]
    fn global_aggregate_bound_is_scaled_by_selected_region_count() {
        // A global aggregate emits one row per region, so the whole-scan
        // estimate is region_count, not the per-region cap of 1.
        use datafusion::functions_aggregate::expr_fn::count;
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .aggregate(Vec::<datafusion_expr::Expr>::new(), vec![count(lit(1))])
            .unwrap()
            .build()
            .unwrap();
        let exec = merge_scan_exec_with_plan_and_row_count_provider(
            vec![RegionId::new(1024, 1), RegionId::new(1024, 2)],
            plan,
            None,
        );

        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(2));
    }

    #[test]
    fn structural_bound_overflow_after_region_scaling_stays_absent() {
        // `usize::MAX / 2 + 1` per region over two regions overflows the
        // checked multiplication; the unsafe product must stay Absent rather
        // than wrapping to a bogus small bound.
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(usize::MAX / 2 + 1))
            .unwrap()
            .build()
            .unwrap();
        let exec = merge_scan_exec_with_plan_and_row_count_provider(
            vec![RegionId::new(1024, 1), RegionId::new(1024, 2)],
            plan,
            None,
        );

        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Absent);
    }

    #[test]
    fn partition_statistics_stays_absent_for_join_plan_even_with_provider() {
        // A join can emit more rows than the underlying regions store, so the
        // live provider total is not a sound upper bound. The estimate must
        // stay Absent even when the provider knows large region counts.
        let left = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("a")])
            .unwrap()
            .build()
            .unwrap();
        let right = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("b")])
            .unwrap()
            .build()
            .unwrap();
        let plan = LogicalPlanBuilder::from(left)
            .join(right, JoinType::Inner, (vec!["a"], vec!["b"]), None)
            .unwrap()
            .build()
            .unwrap();

        let region1 = RegionId::new(1024, 1);
        let region2 = RegionId::new(1024, 2);
        let provider = Arc::new(TestRegionRowCountProvider::new(vec![
            (region1, 600_000),
            (region2, 600_000),
        ])) as RegionRowCountProviderRef;
        let exec = merge_scan_exec_with_plan_and_row_count_provider(
            vec![region1, region2],
            plan,
            Some(provider),
        );

        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Absent);
    }

    #[test]
    fn partition_statistics_stays_absent_for_grouping_set_even_with_provider() {
        use datafusion_expr::GroupingSet;
        // A grouping-set aggregate can emit more rows than its input, so the
        // provider total is not a sound upper bound.
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .aggregate(
                vec![Expr::GroupingSet(GroupingSet::GroupingSets(vec![
                    vec![],
                    vec![col("col")],
                ]))],
                Vec::<datafusion_expr::Expr>::new(),
            )
            .unwrap()
            .build()
            .unwrap();

        let region1 = RegionId::new(1024, 1);
        let region2 = RegionId::new(1024, 2);
        let provider = Arc::new(TestRegionRowCountProvider::new(vec![
            (region1, 600_000),
            (region2, 600_000),
        ])) as RegionRowCountProviderRef;
        let exec = merge_scan_exec_with_plan_and_row_count_provider(
            vec![region1, region2],
            plan,
            Some(provider),
        );

        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Absent);
    }

    #[test]
    fn partition_statistics_uses_provider_for_row_preserving_repartition() {
        // A repartition only redistributes rows, so the child's row count is
        // preserved and the live provider total remains a sound upper bound.
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .repartition(datafusion_expr::Partitioning::RoundRobinBatch(4))
            .unwrap()
            .build()
            .unwrap();

        let region1 = RegionId::new(1024, 1);
        let region2 = RegionId::new(1024, 2);
        let provider = Arc::new(TestRegionRowCountProvider::new(vec![
            (region1, 600_000),
            (region2, 600_000),
        ])) as RegionRowCountProviderRef;
        let exec = merge_scan_exec_with_plan_and_row_count_provider(
            vec![region1, region2],
            plan,
            Some(provider),
        );

        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(1_200_000));
    }

    #[test]
    fn remote_plan_row_bound_preserves_cap_through_repartition() {
        // A repartition is row-preserving: a `LIMIT 50` below it must still
        // produce a per-region cap of 50 (the whole scan then scales it).
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .repartition(datafusion_expr::Partitioning::RoundRobinBatch(4))
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(remote_plan_row_bound(&plan), Some(50));
    }

    #[test]
    fn partition_statistics_stays_absent_on_aggregation_overflow() {
        let region1 = RegionId::new(1024, 1);
        let region2 = RegionId::new(1024, 2);
        let provider = Arc::new(TestRegionRowCountProvider::new(vec![
            (region1, u64::MAX),
            (region2, 1),
        ])) as RegionRowCountProviderRef;
        let exec = merge_scan_exec_with_row_count_provider(vec![region1, region2], Some(provider));

        // The u64 sum overflows. Saturating to a huge value would be unsafe:
        // DataFusion's join cardinality estimation multiplies input row counts,
        // so a near-maximum estimate could panic in debug or wrap in release.
        // The estimate must therefore stay unknown instead.
        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Absent);
    }

    #[test]
    fn partition_statistics_stays_absent_above_product_safe_bound() {
        let region1 = RegionId::new(1024, 1);
        let region2 = RegionId::new(1024, 2);
        // Each individual count fits, but the total exceeds the product-safe
        // bound: two such scan estimates would overflow DataFusion's join
        // cardinality multiplication.
        let provider = Arc::new(TestRegionRowCountProvider::new(vec![
            (region1, u32::MAX as u64),
            (region2, u32::MAX as u64),
        ])) as RegionRowCountProviderRef;
        let exec = merge_scan_exec_with_row_count_provider(vec![region1, region2], Some(provider));

        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Absent);
    }

    #[test]
    fn large_estimates_do_not_overflow_join_statistics() {
        let region1 = RegionId::new(1024, 1);
        let region2 = RegionId::new(1024, 2);
        let big_provider = Arc::new(TestRegionRowCountProvider::new(vec![
            (region1, u32::MAX as u64),
            (region2, u32::MAX as u64),
        ])) as RegionRowCountProviderRef;
        let left = Arc::new(merge_scan_exec_with_row_count_provider(
            vec![region1, region2],
            Some(big_provider.clone()),
        )) as Arc<dyn ExecutionPlan>;
        let right = Arc::new(merge_scan_exec_with_row_count_provider(
            vec![region1, region2],
            Some(big_provider),
        )) as Arc<dyn ExecutionPlan>;

        // The safe semantics must hold per scan: an oversized total is reported
        // as unknown, never as a huge value that could overflow DataFusion's
        // join cardinality multiplication.
        assert_eq!(
            left.partition_statistics(None).unwrap().num_rows,
            Precision::Absent
        );
        assert_eq!(
            right.partition_statistics(None).unwrap().num_rows,
            Precision::Absent
        );

        let on = vec![(
            Arc::new(Column::new("col", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("col", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let join = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        // And DataFusion's join statistics estimation must not panic or wrap.
        let stats = join.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Absent);
    }

    #[test]
    fn partition_statistics_survives_clone_and_with_new_children() {
        let region1 = RegionId::new(1024, 1);
        let region2 = RegionId::new(1024, 2);
        let provider = Arc::new(TestRegionRowCountProvider::new(vec![
            (region1, 600_000),
            (region2, 600_000),
        ])) as RegionRowCountProviderRef;
        let exec = merge_scan_exec_with_row_count_provider(vec![region1, region2], Some(provider));

        let cloned = exec.clone();
        assert_eq!(
            cloned.partition_statistics(None).unwrap().num_rows,
            Precision::Inexact(1_200_000)
        );

        // DataFusion rewrites children unconditionally; the estimate must survive.
        let rewritten = Arc::new(exec)
            .with_new_children(vec![])
            .unwrap()
            .as_any()
            .downcast_ref::<MergeScanExec>()
            .unwrap()
            .clone();
        assert_eq!(
            rewritten.partition_statistics(None).unwrap().num_rows,
            Precision::Inexact(1_200_000)
        );
    }

    #[test]
    fn partition_statistics_survives_distribution_rewrite() {
        let region1 = RegionId::new(1024, 1);
        let region2 = RegionId::new(1024, 2);
        let provider = Arc::new(TestRegionRowCountProvider::new(vec![
            (region1, 600_000),
            (region2, 600_000),
        ])) as RegionRowCountProviderRef;

        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col1")])
            .unwrap()
            .build()
            .unwrap();
        let schema = plan.schema().as_arrow().clone();
        let mut partition_cols = AliasMapping::new();
        partition_cols.insert(
            "col1".to_string(),
            BTreeSet::from([ColumnExpr::new(Some(TableReference::bare("table")), "col1")]),
        );
        let exec = MergeScanExec::new(
            &SessionStateBuilder::new().build(),
            TableName::new("catalog", "schema", "table"),
            vec![region1, region2],
            plan,
            &schema,
            Arc::new(TestRegionQueryHandler::default()),
            QueryContext::arc(),
            1,
            partition_cols,
            None,
            false,
            Some(provider),
        )
        .unwrap();

        // A distribution that differs from the current partitioning but shares a
        // column name present in partition_cols, so try_with_new_distribution
        // produces a clone instead of returning None.
        let new_dist = Distribution::HashPartitioned(vec![
            Arc::new(Column::new("col1", 0)),
            Arc::new(Column::new("col2", 1)),
        ]);
        let rewritten = exec
            .try_with_new_distribution(new_dist)
            .expect("expected a cloned exec with overlapping partition col");
        assert_eq!(
            rewritten.partition_statistics(None).unwrap().num_rows,
            Precision::Inexact(1_200_000)
        );
    }

    #[test]
    fn remote_plan_row_bound_caps_at_limit_fetch() {
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(remote_plan_row_bound(&plan), Some(50));
    }

    #[test]
    fn remote_plan_row_bound_caps_at_sort_fetch() {
        let input = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .build()
            .unwrap();
        let plan = LogicalPlan::Sort(datafusion_expr::Sort {
            expr: vec![col("col").sort(false, true)],
            input: Arc::new(input),
            fetch: Some(30),
        });

        assert_eq!(remote_plan_row_bound(&plan), Some(30));
    }

    #[test]
    fn remote_plan_row_bound_returns_none_for_uncapped_plan() {
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(remote_plan_row_bound(&plan), None);
    }

    #[test]
    fn remote_plan_row_bound_returns_none_for_join_without_cap() {
        // A join is not a pass-through node and has no statically-known cap.
        let left = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("a")])
            .unwrap()
            .build()
            .unwrap();
        let right = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("b")])
            .unwrap()
            .build()
            .unwrap();
        let plan = LogicalPlanBuilder::from(left)
            .join(right, JoinType::Inner, (vec!["a"], vec!["b"]), None)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(remote_plan_row_bound(&plan), None);
    }

    #[test]
    fn remote_plan_row_bound_passes_through_row_preserving_nodes() {
        // Build each supported pass-through wrapper as the OUTER node over a
        // `LIMIT 50` child and assert the bound survives it. Each builder call
        // wraps the current plan, so the wrapper must be applied after limit.
        let filter_over_limit = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .filter(col("col").gt(lit(0)))
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(remote_plan_row_bound(&filter_over_limit), Some(50));

        let alias_over_limit = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .alias("cte")
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(remote_plan_row_bound(&alias_over_limit), Some(50));

        let projection_over_limit = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .project(vec![col("col")])
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(remote_plan_row_bound(&projection_over_limit), Some(50));

        let aggregate_over_limit = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .aggregate(vec![col("col")], Vec::<datafusion_expr::Expr>::new())
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(remote_plan_row_bound(&aggregate_over_limit), Some(50));

        let distinct_over_limit = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .distinct()
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(remote_plan_row_bound(&distinct_over_limit), Some(50));
    }

    #[test]
    fn remote_plan_row_bound_combines_nested_caps_with_min() {
        // A tighter nested cap must not be discarded: `LIMIT 50` over
        // `LIMIT 20` reports 20.
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .limit(0, Some(20))
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(remote_plan_row_bound(&plan), Some(20));
    }

    #[test]
    fn remote_plan_row_bound_global_aggregate_over_limit_zero_reports_one() {
        use datafusion::functions_aggregate::expr_fn::count;
        // A global aggregate (no grouping expressions) always emits exactly
        // one row, even from an empty input (`LIMIT 0`). The bound must be 1,
        // not the child's unsound zero cap.
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(0))
            .unwrap()
            .aggregate(Vec::<datafusion_expr::Expr>::new(), vec![count(lit(1))])
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(remote_plan_row_bound(&plan), Some(1));
    }

    #[test]
    fn remote_plan_row_bound_grouping_set_returns_none() {
        use datafusion_expr::GroupingSet;
        // A grouping-set aggregate can emit more rows than its input (one row
        // per grouping set), so the child's cap is not a sound upper bound:
        // fail open instead of passing `LIMIT 50` through.
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .aggregate(
                vec![Expr::GroupingSet(GroupingSet::GroupingSets(vec![
                    vec![],
                    vec![col("col")],
                ]))],
                Vec::<datafusion_expr::Expr>::new(),
            )
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(remote_plan_row_bound(&plan), None);
    }

    #[test]
    fn estimated_num_rows_is_capped_by_remote_plan_row_bound() {
        let region1 = RegionId::new(1024, 1);
        let region2 = RegionId::new(1024, 2);
        let provider = Arc::new(TestRegionRowCountProvider::new(vec![
            (region1, 10_000),
            (region2, 10_000),
        ])) as RegionRowCountProviderRef;

        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .build()
            .unwrap();
        let exec = merge_scan_exec_with_plan_and_row_count_provider(
            vec![region1, region2],
            plan,
            Some(provider),
        );

        // The remote plan runs once per selected region, so two regions can
        // emit up to 2 x 50 = 100 rows even though each region is capped at 50.
        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(100));
    }

    #[test]
    fn estimated_num_rows_ignores_uncapped_plan_when_regions_are_known() {
        let region1 = RegionId::new(1024, 1);
        let region2 = RegionId::new(1024, 2);
        let provider = Arc::new(TestRegionRowCountProvider::new(vec![
            (region1, 600_000),
            (region2, 600_000),
        ])) as RegionRowCountProviderRef;

        let exec = merge_scan_exec_with_row_count_provider(vec![region1, region2], Some(provider));

        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(1_200_000));
    }

    #[test]
    fn estimated_num_rows_caps_oversized_complete_total_with_small_logical_bound() {
        // The sum of the region totals exceeds MAX_SAFE_NUM_ROWS_ESTIMATE, but
        // the remote plan caps the output at 50 rows per region: the
        // plan-structural bound is reported directly (the provider is not
        // consulted for a bounded plan), so two regions report 100, not Absent.
        let region1 = RegionId::new(1024, 1);
        let region2 = RegionId::new(1024, 2);
        let provider = Arc::new(TestRegionRowCountProvider::new(vec![
            (region1, u32::MAX as u64),
            (region2, u32::MAX as u64),
        ])) as RegionRowCountProviderRef;

        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .build()
            .unwrap();
        let exec = merge_scan_exec_with_plan_and_row_count_provider(
            vec![region1, region2],
            plan,
            Some(provider),
        );

        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(100));
    }

    #[test]
    fn join_selection_makes_cte_side_with_limit_the_hash_build_side() {
        // Both sides of the join read the SAME table with the SAME real row
        // counts, so a region-only estimate cannot distinguish them. Only the
        // CTE side carries a logical `LIMIT 50`; the logical bound must make
        // that side the smaller estimate and therefore the hash build side.
        let region1 = RegionId::new(1024, 1);
        let region2 = RegionId::new(1024, 2);
        let provider = Arc::new(TestRegionRowCountProvider::new(vec![
            (region1, 600_000),
            (region2, 600_000),
        ])) as RegionRowCountProviderRef;

        // Base table side: full scan, no logical cap.
        let base = Arc::new(merge_scan_exec_with_row_count_provider(
            vec![region1, region2],
            Some(provider.clone()),
        )) as Arc<dyn ExecutionPlan>;

        // CTE side: `SELECT ... FROM t LIMIT 50`.
        let cte_plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .build()
            .unwrap();
        let cte = Arc::new(merge_scan_exec_with_plan_and_row_count_provider(
            vec![region1, region2],
            cte_plan,
            Some(provider),
        )) as Arc<dyn ExecutionPlan>;

        let on = vec![(
            Arc::new(Column::new("col", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("col", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let join = Arc::new(
            HashJoinExec::try_new(
                base,
                cte,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Auto,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let optimized = JoinSelection::new()
            .optimize(join, &ConfigOptions::default())
            .unwrap();

        // Swapping rewrites output column order, so the join may be wrapped in a
        // projection. Walk down to the actual HashJoinExec.
        let mut current = optimized.as_ref();
        while current.as_any().downcast_ref::<HashJoinExec>().is_none() {
            current = current.children()[0].as_ref();
        }
        let hash_join = current.as_any().downcast_ref::<HashJoinExec>().unwrap();

        // The CTE side (1_200_000 rows in the table, but at most 100 rows
        // through the per-region LIMIT, 50 per region x 2 regions) becomes the
        // left (build) side in CollectLeft mode, while the unbounded base scan
        // stays on the probe side.
        assert_eq!(hash_join.partition_mode(), &PartitionMode::CollectLeft);
        assert_eq!(
            hash_join
                .left()
                .partition_statistics(None)
                .unwrap()
                .num_rows,
            Precision::Inexact(100)
        );
        assert_eq!(
            hash_join
                .right()
                .partition_statistics(None)
                .unwrap()
                .num_rows,
            Precision::Inexact(1_200_000)
        );
    }

    #[test]
    fn join_selection_makes_bounded_merge_scan_the_build_side_without_provider() {
        // Distributed frontends have no row-count provider. The CollectLeft
        // pick must still work there, driven purely by the plan-structural
        // bound — and identically to standalone, because the standalone and
        // distributed sqlness runs share the same goldens.
        let bounded_plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .build()
            .unwrap();
        let small = Arc::new(merge_scan_exec_with_plan_and_row_count_provider(
            vec![RegionId::new(1024, 1), RegionId::new(1024, 2)],
            bounded_plan,
            None,
        )) as Arc<dyn ExecutionPlan>;
        let unknown = Arc::new(merge_scan_exec_with_row_count_provider(
            vec![RegionId::new(1024, 3), RegionId::new(1024, 4)],
            None,
        )) as Arc<dyn ExecutionPlan>;

        let on = vec![(
            Arc::new(Column::new("col", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("col", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let join = Arc::new(
            HashJoinExec::try_new(
                unknown,
                small,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Auto,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let optimized = JoinSelection::new()
            .optimize(join, &ConfigOptions::default())
            .unwrap();

        // Swapping rewrites output column order, so the join may be wrapped in a
        // projection. Walk down to the actual HashJoinExec.
        let mut current = optimized.as_ref();
        while current.as_any().downcast_ref::<HashJoinExec>().is_none() {
            current = current.children()[0].as_ref();
        }
        let hash_join = current.as_any().downcast_ref::<HashJoinExec>().unwrap();

        // The bounded MergeScan becomes the left (build) side in CollectLeft
        // mode, while the unknown peer stays on the probe side. The bound is
        // per-region: two regions can emit up to 2 x 50 = 100 rows.
        assert_eq!(hash_join.partition_mode(), &PartitionMode::CollectLeft);
        assert_eq!(
            hash_join
                .left()
                .partition_statistics(None)
                .unwrap()
                .num_rows,
            Precision::Inexact(100)
        );
        assert_eq!(
            hash_join
                .right()
                .partition_statistics(None)
                .unwrap()
                .num_rows,
            Precision::Absent
        );
    }

    /// Builds a narrow `ProjectionExec` over a `MergeScanExec`: a single
    /// all-primitive column, so `ProjectionExec::project_statistics`
    /// recomputes `total_byte_size` from `num_rows` and the column width.
    fn narrow_projection_merge_scan(
        regions: Vec<RegionId>,
        plan: LogicalPlan,
        provider: Option<RegionRowCountProviderRef>,
    ) -> Arc<dyn ExecutionPlan> {
        let exec = merge_scan_exec_with_plan_and_row_count_provider(regions, plan, provider);
        let projection = ProjectionExec::try_new(
            vec![(
                Arc::new(Column::new("col", 0)) as Arc<dyn PhysicalExpr>,
                "col".to_string(),
            )],
            Arc::new(exec),
        )
        .unwrap();
        Arc::new(projection)
    }

    #[test]
    fn join_selection_narrow_projection_with_provider_matches_provider_absent() {
        // DataFusion's `supports_collect_by_thresholds` checks
        // `total_byte_size` FIRST against `hash_join_single_partition_threshold`
        // (default 1 MiB), and a `ProjectionExec` above the scan recomputes
        // bytes from `num_rows` x primitive width. If the provider rehydrated
        // a collectable byte estimate (200_000 rows x 4-byte Int32 = 800 KiB
        // < 1 MiB), standalone (provider present) would pick CollectLeft while
        // distributed (provider absent) stays Partitioned. The byte-safe live
        // floor must keep both environments identical.
        let region1 = RegionId::new(1024, 1);
        let region2 = RegionId::new(1024, 2);
        // 100_000 rows per region, 200_000 total: below the byte-safe floor
        // (1 MiB rows), so the provider estimate is Absent — exactly what a
        // provider-less distributed frontend reports. Under the old 128K
        // floor, 200_000 rows would have been reported and the narrow Int32
        // projection would have rehydrated a collectable 800 KiB estimate.
        let provider = Arc::new(TestRegionRowCountProvider::new(vec![
            (region1, 100_000),
            (region2, 100_000),
        ])) as RegionRowCountProviderRef;

        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i32).alias("col")])
            .unwrap()
            .build()
            .unwrap();

        let provider_present =
            narrow_projection_merge_scan(vec![region1, region2], plan.clone(), Some(provider));
        let provider_absent =
            narrow_projection_merge_scan(vec![region1, region2], plan.clone(), None);
        // Peer side with the same narrow schema (provider absent) so the join
        // on-column types match.
        let unknown = narrow_projection_merge_scan(
            vec![RegionId::new(1024, 3), RegionId::new(1024, 4)],
            plan,
            None,
        );

        let on = vec![(
            Arc::new(Column::new("col", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("col", 0)) as Arc<dyn PhysicalExpr>,
        )];

        for narrow in [provider_present, provider_absent] {
            let join = Arc::new(
                HashJoinExec::try_new(
                    narrow,
                    unknown.clone(),
                    on.clone(),
                    None,
                    &JoinType::Inner,
                    None,
                    PartitionMode::Auto,
                    NullEquality::NullEqualsNothing,
                    false,
                )
                .unwrap(),
            ) as Arc<dyn ExecutionPlan>;

            let optimized = JoinSelection::new()
                .optimize(join, &ConfigOptions::default())
                .unwrap();

            let mut current = optimized.as_ref();
            while current.as_any().downcast_ref::<HashJoinExec>().is_none() {
                current = current.children()[0].as_ref();
            }
            let hash_join = current.as_any().downcast_ref::<HashJoinExec>().unwrap();

            // The narrow projection must NOT rehydrate a collectable byte
            // estimate: both sides are unknown, so the join stays Partitioned
            // in standalone exactly as it does in distributed mode.
            assert_eq!(
                hash_join.partition_mode(),
                &PartitionMode::Partitioned,
                "provider-derived estimates must not flip JoinSelection into CollectLeft"
            );
        }
    }

    #[test]
    fn join_selection_narrow_projection_bounded_cte_still_collects_left() {
        // The byte-safe floor must NOT regress the issue #6874 behavior: a
        // bounded CTE side (`LIMIT 50`, two regions -> 100 rows) still
        // produces a small byte estimate even through a narrow projection, so
        // JoinSelection keeps CollectLeft — identically with or without a
        // provider, because the structural bound is provider-independent.
        let bounded_plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i32).alias("col")])
            .unwrap()
            .limit(0, Some(50))
            .unwrap()
            .build()
            .unwrap();
        let small = narrow_projection_merge_scan(
            vec![RegionId::new(1024, 1), RegionId::new(1024, 2)],
            bounded_plan.clone(),
            None,
        );
        let unknown = narrow_projection_merge_scan(
            vec![RegionId::new(1024, 3), RegionId::new(1024, 4)],
            bounded_plan,
            None,
        );

        let on = vec![(
            Arc::new(Column::new("col", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("col", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let join = Arc::new(
            HashJoinExec::try_new(
                unknown,
                small,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Auto,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let optimized = JoinSelection::new()
            .optimize(join, &ConfigOptions::default())
            .unwrap();

        let mut current = optimized.as_ref();
        while current.as_any().downcast_ref::<HashJoinExec>().is_none() {
            current = current.children()[0].as_ref();
        }
        let hash_join = current.as_any().downcast_ref::<HashJoinExec>().unwrap();

        // The bounded CTE side (100 rows through the per-region LIMIT, 50 per
        // region x 2 regions) becomes the CollectLeft build side.
        assert_eq!(hash_join.partition_mode(), &PartitionMode::CollectLeft);
        assert_eq!(
            hash_join
                .left()
                .partition_statistics(None)
                .unwrap()
                .num_rows,
            Precision::Inexact(100)
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
            None,
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
            None,
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
    fn merge_scan_remote_schema_identity_rejects_json_vs_plain_binary_field() {
        // A JSON column must not be accepted as compatible with a plain Binary
        // column lacking the JSON extension metadata.
        let expected = ArrowSchema::new(vec![
            Field::new("j", TestArrowDataType::Binary, true).with_metadata(json_field_metadata(
                true,
                r#"{"json_settings":{"type_hints":[]}}"#,
            )),
        ]);
        let actual = ArrowSchema::new(vec![Field::new("j", TestArrowDataType::Binary, true)]);
        assert!(validate_remote_schema(&expected, &actual, "test").is_err());
    }

    #[test]
    fn merge_scan_remote_schema_identity_rejects_field_metadata_and_nullability_mismatch() {
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
        assert!(validate_remote_schema(&expected, &metadata_mismatch, "test").is_err());
        assert!(validate_remote_schema(&expected, &nullability_mismatch, "test").is_err());
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
        for actual in [&different_unit, &different_name, &nullability, &metadata] {
            assert!(validate_remote_schema(&expected, actual, "test").is_err());
        }
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
            None,
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
            None,
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
            None,
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
            None,
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
