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
#[cfg(feature = "enterprise")]
use std::sync::{LazyLock, Mutex};
#[cfg(feature = "enterprise")]
use std::time::{Duration, Instant};

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
use datafusion::execution::SessionStateBuilder;
#[cfg(feature = "enterprise")]
use datafusion::execution::memory_pool::MemoryLimit as DfMemoryLimit;
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
use session::context::{QueryContextRef, QueryMemoryScope};
use snafu::{OptionExt, ResultExt, ensure};
use sqlparser::ast::AnalyzeFormat;
use table::TableRef;
use table::requests::{DeleteRequest, InsertRequest};
use table::table::scan::{REGION_SCAN_EXEC_NAME, RegionScanExec};
use tracing::Span;

use crate::analyze::DistAnalyzeExec;
pub use crate::datafusion::planner::DfContextProviderAdapter;
use crate::dist_plan::{MergeScanLogicalPlan, RemoteDynFilterReceiverInjectorRef};
#[cfg(not(feature = "enterprise"))]
use crate::error::UnsupportedFlowMemoryBudgetSnafu;
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
#[cfg(feature = "enterprise")]
use crate::options::{FLOW_ID, QueryMemoryPoolPolicy};
use crate::options::{FLOW_MAX_MEMORY_BYTES, ScheduledTimeExtension};
use crate::physical_wrapper::PhysicalPlanWrapperRef;
use crate::planner::{DfLogicalPlanner, LogicalPlanner};
#[cfg(feature = "enterprise")]
use crate::query_engine::new_flow_memory_pool;
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

// The invalid-budget warning machinery below belongs to the Flow memory budget
// derivation, which only the `enterprise` feature activates. It is gated with it
// so a default build carries no unused items.
//
// Longest client-supplied extension value a log line keeps verbatim.
#[cfg(feature = "enterprise")]
const MAX_LOGGED_VALUE_CHARS: usize = 64;

/// How long a malformed-budget warning stays suppressed for the same Flow and
/// value. This runs once per engine execution (an `InsertIntoPlan` request
/// builds two query contexts), so a client that always sends the same broken
/// budget would otherwise write one line per execution.
#[cfg(feature = "enterprise")]
const INVALID_BUDGET_WARN_WINDOW: Duration = Duration::from_secs(60);

/// Caps the number of `(flow id, value)` pairs the warning limiter remembers,
/// so a client rotating keys cannot grow the map without bound.
#[cfg(feature = "enterprise")]
const MAX_TRACKED_INVALID_BUDGET_WARNINGS: usize = 1024;

/// Renders a client-supplied extension value for a log line, keeping at most
/// [`MAX_LOGGED_VALUE_CHARS`] characters and marking a cut with `...`.
///
/// Bounded by characters rather than bytes: the value is arbitrary client input
/// and slicing on a byte offset would panic inside a multi-byte character.
#[cfg(feature = "enterprise")]
fn truncate_for_log(raw: &str) -> String {
    match raw.char_indices().nth(MAX_LOGGED_VALUE_CHARS) {
        Some((cut, _)) => format!("{}...", &raw[..cut]),
        None => raw.to_string(),
    }
}

/// Emits at most one warning per key and [`INVALID_BUDGET_WARN_WINDOW`].
///
/// Keyed by the whole message body (Flow id plus the truncated value), so a
/// client cannot flood the log by varying the budget of the same Flow, and the
/// map is pruned on every decision, so its size is bounded by the distinct keys
/// of one window. A window that fills up is cleared outright instead of being
/// allowed to grow.
#[cfg(feature = "enterprise")]
#[derive(Default)]
struct WarnRateLimiter {
    last_emitted: Mutex<HashMap<String, Instant>>,
}

#[cfg(feature = "enterprise")]
impl WarnRateLimiter {
    /// Records `key` at `now` and reports whether the caller should log it.
    ///
    /// The clock is an argument so the decision is testable without sleeping.
    fn should_emit_at(&self, key: &str, now: Instant, window: Duration) -> bool {
        // A poisoned lock (a thread panicked while holding it) must never take a
        // query down: log rather than stay silent.
        let Ok(mut last_emitted) = self.last_emitted.lock() else {
            return true;
        };
        last_emitted.retain(|_, emitted| now.duration_since(*emitted) < window);
        if let Some(emitted) = last_emitted.get(key)
            && now.duration_since(*emitted) < window
        {
            // Keep the original timestamp, so sustained traffic still gets one
            // line per window instead of none until it goes quiet.
            return false;
        }
        if last_emitted.len() >= MAX_TRACKED_INVALID_BUDGET_WARNINGS {
            last_emitted.clear();
        }
        last_emitted.insert(key.to_string(), now);
        true
    }

    /// [`Self::should_emit_at`] against the monotonic clock.
    fn should_emit(&self, key: &str) -> bool {
        self.should_emit_at(key, Instant::now(), INVALID_BUDGET_WARN_WINDOW)
    }
}

#[cfg(feature = "enterprise")]
static INVALID_BUDGET_WARNINGS: LazyLock<WarnRateLimiter> = LazyLock::new(WarnRateLimiter::default);

/// Reports a budget declaration that does not denote a positive byte count.
///
/// The value is client-supplied, so it is truncated before it reaches the log
/// and repeated identical reports are rate limited.
#[cfg(feature = "enterprise")]
fn warn_invalid_flow_budget(flow_id: &str, raw: &str) {
    let logged_value = truncate_for_log(raw);
    // NUL separates the two parts, which neither part can contain in a way that
    // would let one key collide with another.
    let key = format!("{flow_id}\u{0}{logged_value}");
    if INVALID_BUDGET_WARNINGS.should_emit(&key) {
        common_telemetry::warn!(
            "Ignoring Flow memory budget: flow_id={}, {}={} is not a positive \
             decimal byte count, the query runs on the engine memory pool",
            flow_id,
            FLOW_MAX_MEMORY_BYTES,
            logged_value
        );
    }
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
        let state = ctx.state();

        common_telemetry::debug!("Create physical plan, input plan: {logical_plan}");

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

    /// Resolves the Flow-scoped memory pool declared by the extensions of
    /// `query_ctx`, or `Ok(None)` when the query declares no budget of its own.
    ///
    /// The derivation itself is documented on
    /// [`Self::enterprise_flow_memory_scope`].
    ///
    /// # Enterprise gating
    ///
    /// `max_memory` is an enterprise flow option, so a build without the
    /// `enterprise` feature does not support it. Such a build must not silently
    /// run the query on the node-wide pool either: a declared budget is refused
    /// with the `UnsupportedFlowMemoryBudget` query error, so a limit that is not
    /// enforced cannot be mistaken for one that is. Only a query that declares no
    /// budget runs on the node pool, exactly as before the option existed.
    ///
    /// The mechanism around this derivation - [`QueryMemoryScope`],
    /// `NestedMemoryPool`, `QueryEngineState::runtime_env_with_memory_pool` and
    /// the hook below - stays compiled in every build: it is where an enterprise
    /// build plugs in, and gating it would fork the runtime behaviour.
    fn flow_memory_scope(&self, query_ctx: &QueryContextRef) -> Result<Option<QueryMemoryScope>> {
        #[cfg(not(feature = "enterprise"))]
        {
            if query_ctx.extensions().contains_key(FLOW_MAX_MEMORY_BYTES) {
                return UnsupportedFlowMemoryBudgetSnafu {
                    reason: common_meta::ddl::create_flow::FLOW_MAX_MEMORY_UNSUPPORTED_MSG
                        .to_string(),
                }
                .fail();
            }
            Ok(None)
        }
        #[cfg(feature = "enterprise")]
        {
            self.enterprise_flow_memory_scope(query_ctx)
        }
    }

    /// Builds the process-local memory scope a Flow asked for through the query
    /// extensions of `query_ctx`, or returns `Ok(None)` when the query declares
    /// no budget of its own.
    ///
    /// Only compiled with the `enterprise` feature: [`Self::flow_memory_scope`]
    /// is the entry point, and it rejects a declared budget on every other
    /// build.
    ///
    /// A Flow declares its budget with [`FLOW_MAX_MEMORY_BYTES`] (a decimal
    /// byte count) and names itself with [`FLOW_ID`]. Turning that declaration
    /// into the pool the query actually runs against is what keeps a single
    /// Flow from taking down the process that executes it: the Flow fails its
    /// own query on exhaustion instead of draining the pool every other query
    /// in the process shares.
    ///
    /// Two rules keep the Flow's own share bounded:
    ///
    /// - The pool built here is always nested under the node-wide pool by
    ///   [`QueryEngineState::runtime_env_with_memory_pool`], so every byte a
    ///   Flow reserves is charged to `query.memory_pool_size` as well.
    /// - The declared budget is clamped to the node-wide limit, so a Flow pool
    ///   is never wider than the node pool it is nested in.
    ///
    /// With the default [`QueryMemoryPoolPolicy::Greedy`] policy the sum of all
    /// Flow reservations plus ordinary reservations stays within the node
    /// limit: every tracked reservation is checked against both pools. The
    /// experimental `Fair` policy is *not* a bound on the node's total - it
    /// keeps the pre-existing `FairSpillPool` semantics, where a spillable
    /// consumer is checked against its current fair share rather than against
    /// the pool total - and neither policy enforces the infallible
    /// [`MemoryPool::grow`][grow]. The budget therefore bounds tracked,
    /// spill-checkable reservations, not process RSS.
    ///
    /// [grow]: datafusion::execution::memory_pool::MemoryPool::grow
    ///
    /// # Trust boundary
    ///
    /// The extension is supplied by the client. `servers/src/grpc/flight.rs`
    /// reads the Flow hints from the request metadata and builds the query
    /// context with them *before* authentication runs, so the budget must not
    /// be treated as an authorization decision. It is safe only because of the
    /// nesting above: a forged budget can repartition the node pool between
    /// flows - hand one Flow a narrow slice and another a wide one - but for
    /// growths that consult the pool it can never make the process reserve more
    /// than `query.memory_pool_size` (under the default policy; see the caveats
    /// above). For the same reason no internal-channel gate is needed here: a
    /// default distributed deployment has no internal listener at all
    /// (`frontend/src/frontend.rs` leaves `internal_grpc` unset), so there is
    /// no privileged entry point that could be trusted over this one.
    ///
    /// # Limits of the declaration
    ///
    /// `0`, an empty value and a value that is not a decimal byte count mean
    /// "no budget declared": the query keeps running on the node pool, but a
    /// warning is logged with the Flow id and the (truncated) raw value, so
    /// nobody believes a limit exists when it does not. The warning is rate
    /// limited because this runs once per engine execution, not once per
    /// request. A query that declares no budget at all runs on the node pool
    /// exactly as before.
    ///
    /// The allocation policy is [`QueryMemoryPoolPolicy::Greedy`]: the engine's
    /// `experimental_memory_pool_policy` is consumed while the runtime
    /// environment is built and is not retained in [`QueryEngineState`], so it
    /// is not reachable from here.
    #[cfg(feature = "enterprise")]
    fn enterprise_flow_memory_scope(
        &self,
        query_ctx: &QueryContextRef,
    ) -> Result<Option<QueryMemoryScope>> {
        let extensions = query_ctx.extensions();
        let Some(raw) = extensions.get(FLOW_MAX_MEMORY_BYTES) else {
            return Ok(None);
        };
        // `-` keeps log lines parseable when the optional id is absent.
        let flow_id = extensions.get(FLOW_ID).map(String::as_str).unwrap_or("-");

        let requested = match raw.trim().parse::<u64>() {
            Ok(bytes) if bytes > 0 => bytes,
            _ => {
                // Fail open: a Flow with a malformed budget must still run, but
                // the operator must not believe it was ever bounded.
                warn_invalid_flow_budget(flow_id, raw);
                return Ok(None);
            }
        };

        // Clamp against the node-wide pool. `Finite(0)` and `Infinite` are how
        // an unset `query.memory_pool_size` reaches this point (`0` is
        // documented as unbounded, and the engine then keeps DataFusion's
        // unbounded pool): both leave the declared budget as the only bound.
        let effective = match self
            .state
            .session_state()
            .runtime_env()
            .memory_pool
            .memory_limit()
        {
            DfMemoryLimit::Finite(node_limit) if node_limit > 0 => requested.min(node_limit as u64),
            _ => requested,
        };
        // A budget that does not fit the address space is clamped to `usize::MAX`.
        let effective = usize::try_from(effective).unwrap_or(usize::MAX);

        // Ties the `flow-scoped` pool - and every exhaustion error it reports -
        // to the Flow that owns it. The extensions name the Flow by id only, so
        // the label is `flow:<id>`; a caller that knows the name labels an
        // explicit scope with it instead.
        let label = match extensions.get(FLOW_ID) {
            Some(id) => format!("flow:{id}"),
            None => "flow".to_string(),
        };

        common_telemetry::debug!(
            "Query runs against a Flow-scoped memory pool: flow_id={}, scope={}, \
             effective_bytes={}, requested_bytes={}",
            flow_id,
            label,
            effective,
            requested
        );

        Ok(Some(QueryMemoryScope::new(
            label,
            new_flow_memory_pool(effective, QueryMemoryPoolPolicy::Greedy),
        )))
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

    fn flow_memory_scope_for(&self, query_ctx: &QueryContextRef) -> Option<QueryMemoryScope> {
        // Resolving an already scoped context returns the scope it carries, so a
        // caller can resolve the same context any number of times and always
        // land on the same pool.
        match query_ctx.memory_scope() {
            Some(scope) => Some(scope.clone()),
            // This hint-style entry point cannot fail, so a budget this build
            // does not support leaves the context on the node pool here. It is
            // rejected by `with_flow_memory_scope_fallible` at the request entry
            // - the path every request goes through - and by `engine_context`
            // for an execution that is entered directly.
            None => self.flow_memory_scope(query_ctx).ok().flatten(),
        }
    }

    fn with_flow_memory_scope_fallible(
        &self,
        query_ctx: QueryContextRef,
    ) -> Result<QueryContextRef> {
        // Only the resolution can fail - it rejects a budget this build does not
        // support - so an already scoped context is returned as is, without
        // re-deriving (and possibly re-creating) its pool.
        if query_ctx.memory_scope().is_some() {
            return Ok(query_ctx);
        }
        let Some(scope) = self.flow_memory_scope(&query_ctx)? else {
            return Ok(query_ctx);
        };
        let mut forked = query_ctx.fork();
        forked.set_memory_scope(scope);
        Ok(Arc::new(forked))
    }

    fn engine_context(&self, query_ctx: QueryContextRef) -> Result<QueryEngineContext> {
        let mut state = self.state.session_state();
        // A process-local memory scope replaces the engine-wide pool for this
        // unit of work only. Rebuild before applying per-query config so the
        // options/timezone/extensions below land on the final state.
        //
        // The resolved scope is recorded on the query context - in a fork, never
        // in the caller's shared `QueryContext` - and not merely installed on
        // this state, because one logical request re-enters the engine several
        // times with the same context: server-side plan composition calls
        // `execute` again for the sub-plan it built, and the DML path runs its
        // input query through `exec_query_plan`. Every one of those executions
        // calls `engine_context` again, so without a recorded scope each would
        // derive a *fresh* full budget from the extension and a single request
        // could hold several budgets at once. With the scope recorded, all of
        // them resolve to the same pool and share one budget.
        //
        // A context that already carries a scope - an explicit one installed by
        // the process that owns the unit of work, or one recorded at the request
        // entry - is used unchanged: an explicit scope always wins over a budget
        // declared through query extensions, because the extension is only a
        // request from whoever issued the query. Queries with neither run on the
        // engine pool unchanged.
        //
        // Resolution is fallible: a build without the `enterprise` feature does
        // not support the `max_memory` flow option, so a declared budget fails
        // this query here instead of running it unbudgeted. The request entry
        // rejects it first, so this is the backstop for an execution that is
        // entered directly - not the only place the promise is kept.
        let query_ctx = self.with_flow_memory_scope_fallible(query_ctx)?;
        if let Some(pool) = query_ctx.memory_scope().map(QueryMemoryScope::pool) {
            let session_id = state.session_id().to_string();
            let runtime_env = self.state.runtime_env_with_memory_pool(pool);
            state = SessionStateBuilder::new_from_existing(state)
                .with_session_id(session_id)
                .with_runtime_env(runtime_env)
                .build();
        }
        // `apply_query_options` forks again, and its snapshot keeps the scope.
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
    use common_base::memory_limit::MemoryLimit;
    use common_base::readable_size::ReadableSize;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, NUMBERS_TABLE_ID};
    use common_error::ext::BoxedError;
    use common_recordbatch::{
        EmptyRecordBatchStream, RecordBatch, SendableRecordBatchStream, util,
    };
    #[cfg(feature = "enterprise")]
    use datafusion::execution::memory_pool::MemoryPool;
    use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit as DfMemoryLimit};
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
    use session::context::{QueryContext, QueryContextBuilder, QueryMemoryScope};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::region_engine::{
        PartitionRange, PrepareRequest, QueryScanContext, RegionScanner, ScannerProperties,
    };
    use store_api::storage::{RegionId, ScanRequest};
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::table::numbers::{NUMBERS_TABLE_NAME, NumbersTable};
    use table::table::scan::RegionScanExec;

    use super::*;
    use crate::options::{FLOW_ID, QueryMemoryPoolPolicy, QueryOptions};
    use crate::parser::{QueryLanguageParser, QueryStatement};
    use crate::part_sort::PartSortExec;
    use crate::query_engine::{QueryEngineFactory, QueryEngineRef, new_flow_memory_pool};

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

    #[test]
    fn engine_context_swaps_memory_pool_only_for_scoped_queries() {
        let engine = QueryEngineFactory::new(
            catalog::memory::new_memory_catalog_manager().unwrap(),
            None,
            None,
            None,
            None,
            false,
            QueryOptions {
                memory_pool_size: MemoryLimit::Size(ReadableSize(1024)),
                ..Default::default()
            },
        )
        .query_engine();

        // Without a scope the engine-configured pool is used unchanged.
        let ctx = engine.engine_context(QueryContext::arc()).unwrap();
        assert!(matches!(
            ctx.state().runtime_env().memory_pool.memory_limit(),
            DfMemoryLimit::Finite(1024)
        ));

        // With a scope this unit of work is bounded by the scope pool.
        let pool = new_flow_memory_pool(4096, QueryMemoryPoolPolicy::Greedy);
        let query_ctx = QueryContextBuilder::default()
            .set_extension(
                "datafusion.optimizer.max_passes".to_string(),
                "7".to_string(),
            )
            .memory_scope(Some(QueryMemoryScope::new("flow_a", pool.clone())))
            .build();
        let ctx = engine.engine_context(Arc::new(query_ctx)).unwrap();
        let runtime_env = ctx.state().runtime_env();
        assert!(matches!(
            runtime_env.memory_pool.memory_limit(),
            DfMemoryLimit::Finite(4096)
        ));
        // The scope is nested inside the node-wide pool instead of replacing
        // it, so the scoped runtime reports the scope bound and the flow pool's
        // reservation. The previous `Arc::ptr_eq` on the flow pool no longer
        // holds because the installed pool is a `NestedMemoryPool`.
        assert_eq!("flow-scoped", runtime_env.memory_pool.name());
        assert_eq!(0, pool.reserved());

        // Nesting at engine level: a scoped reservation above the engine's 1KB
        // node limit is refused even though the flow budget is 4KB, and both
        // pools always agree on what was actually granted.
        let node_pool = engine
            .engine_state()
            .session_state()
            .runtime_env()
            .memory_pool
            .clone();
        let reservation = MemoryConsumer::new("scoped").register(&runtime_env.memory_pool);
        reservation.try_grow(1025).unwrap_err();
        assert_eq!(0, node_pool.reserved());
        assert_eq!(0, pool.reserved());
        reservation.try_grow(1024).unwrap();
        assert_eq!(1024, node_pool.reserved());
        assert_eq!(1024, pool.reserved());
        // Per-query config still lands on the rebuilt state...
        assert_eq!(7, ctx.state().config().options().optimizer.max_passes);
        assert_eq!(
            Some(ctx.query_ctx().timezone().to_string()),
            ctx.state().config().options().execution.time_zone
        );
        // ...and the engine-wide state is untouched.
        assert!(matches!(
            engine
                .engine_state()
                .session_state()
                .runtime_env()
                .memory_pool
                .memory_limit(),
            DfMemoryLimit::Finite(1024)
        ));
    }

    /// Builds an engine over the `numbers` table whose node-wide memory pool is
    /// `node_bytes` bytes, mirroring how `query.memory_pool_size` reaches
    /// [`QueryEngineState`].
    fn query_engine_with_node_memory_pool(node_bytes: u64) -> QueryEngineRef {
        let catalog_manager = catalog::memory::new_memory_catalog_manager().unwrap();
        catalog_manager
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: NUMBERS_TABLE_NAME.to_string(),
                table_id: NUMBERS_TABLE_ID,
                table: NumbersTable::table(NUMBERS_TABLE_ID),
            })
            .unwrap();

        QueryEngineFactory::new(
            catalog_manager,
            None,
            None,
            None,
            None,
            false,
            QueryOptions {
                memory_pool_size: MemoryLimit::Size(ReadableSize(node_bytes)),
                ..Default::default()
            },
        )
        .query_engine()
    }

    /// The process-wide pool every query of the engine is charged to.
    #[cfg(feature = "enterprise")]
    fn node_memory_pool(engine: &QueryEngineRef) -> Arc<dyn MemoryPool> {
        engine
            .engine_state()
            .session_state()
            .runtime_env()
            .memory_pool
            .clone()
    }

    /// `max_memory` is an enterprise flow option, so a default build refuses a
    /// query that declares a budget instead of silently running it on the node
    /// pool: a limit that is not enforced must not look like one that is.
    #[cfg(not(feature = "enterprise"))]
    #[test]
    fn engine_context_rejects_flow_memory_budget_on_default_build() {
        let engine = query_engine_with_node_memory_pool(8192);
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .set_extension(FLOW_MAX_MEMORY_BYTES.to_string(), "4096".to_string())
                .set_extension(FLOW_ID.to_string(), "42".to_string())
                .build(),
        );

        let err = engine.engine_context(query_ctx.clone()).unwrap_err();
        assert!(
            matches!(
                &err,
                crate::error::Error::UnsupportedFlowMemoryBudget { .. }
            ),
            "unexpected error: {err:?}"
        );
        assert_eq!(
            "flow option `max_memory` requires the enterprise edition",
            err.to_string()
        );

        // The caller's shared context is left alone and the request-entry hook
        // stays a no-op: the error is reported by the execution itself.
        assert!(query_ctx.memory_scope().is_none());
        let scoped = engine.with_flow_memory_scope(query_ctx.clone());
        assert!(Arc::ptr_eq(&scoped, &query_ctx));

        // The rejected declaration never resizes the node pool.
        assert!(matches!(
            engine
                .engine_state()
                .session_state()
                .runtime_env()
                .memory_pool
                .memory_limit(),
            DfMemoryLimit::Finite(8192)
        ));
    }

    #[cfg(feature = "enterprise")]
    #[test]
    fn engine_context_installs_flow_memory_pool_from_extension() {
        let engine = query_engine_with_node_memory_pool(8192);
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .set_extension(FLOW_MAX_MEMORY_BYTES.to_string(), "4096".to_string())
                .set_extension(FLOW_ID.to_string(), "42".to_string())
                .build(),
        );

        let ctx = engine.engine_context(query_ctx.clone()).unwrap();
        let runtime_env = ctx.state().runtime_env();
        // The declared budget (4KB, below the 8KB node pool) becomes the bound
        // of this unit of work.
        assert!(matches!(
            runtime_env.memory_pool.memory_limit(),
            DfMemoryLimit::Finite(4096)
        ));
        assert_eq!("flow-scoped", runtime_env.memory_pool.name());

        // The resolved scope is recorded on the context the engine runs under, so
        // a nested execution under it reuses this pool (see
        // `engine_context_shares_one_flow_budget_across_re_entries`)...
        let scoped_ctx = ctx.query_ctx();
        let scope = scoped_ctx.memory_scope().expect("scope is recorded");
        assert_eq!("flow:42", scope.label());
        assert!(matches!(
            scope.pool().memory_limit(),
            DfMemoryLimit::Finite(4096)
        ));
        // ...while the caller's context is left alone: it is shared, so the
        // engine forks it instead of mutating it.
        assert!(query_ctx.memory_scope().is_none());

        // The Flow pool is nested, so every byte it grants is also charged to
        // the node pool: a Flow can only use memory an ordinary query could not
        // have taken anyway.
        let node_pool = node_memory_pool(&engine);
        let reservation = MemoryConsumer::new("flow_42").register(&runtime_env.memory_pool);
        reservation.try_grow(4096).unwrap();
        assert_eq!(4096, node_pool.reserved());
        // The Flow budget binds first, and a refused growth is not charged to
        // the node pool either.
        reservation.try_grow(1).unwrap_err();
        assert_eq!(4096, node_pool.reserved());
        assert_eq!(4096, reservation.size());

        // The engine-wide state still serves ordinary queries with its own pool.
        assert!(matches!(
            node_pool.memory_limit(),
            DfMemoryLimit::Finite(8192)
        ));
    }

    /// One logical request re-enters the engine several times (server-side plan
    /// composition, the DML input query, nested scans). Each re-entry asks for an
    /// `engine_context` again, so the resolved scope must be recorded on the
    /// context they share - otherwise every one of them would derive a fresh,
    /// independent full budget for the same request.
    #[cfg(feature = "enterprise")]
    #[test]
    fn engine_context_shares_one_flow_budget_across_re_entries() {
        let engine = query_engine_with_node_memory_pool(8192);
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .set_extension(FLOW_MAX_MEMORY_BYTES.to_string(), "4096".to_string())
                .set_extension(FLOW_ID.to_string(), "42".to_string())
                .build(),
        );

        // The request entry resolves the budget once and records the scope on a
        // fork; the incoming context stays pool-free because it is shared.
        let scoped = engine.with_flow_memory_scope(query_ctx.clone());
        assert!(query_ctx.memory_scope().is_none());
        assert!(!Arc::ptr_eq(&scoped, &query_ctx));
        let entry_scope = scoped.memory_scope().expect("entry records the scope");
        assert_eq!("flow:42", entry_scope.label());

        let first = engine.engine_context(scoped.clone()).unwrap();
        // A nested execution - `engine.execute(plan, ctx)` - builds its context
        // from the one it was handed.
        let nested = engine.engine_context(first.query_ctx().clone()).unwrap();

        let first_ctx = first.query_ctx();
        let first_scope = first_ctx.memory_scope().expect("scope on first");
        let nested_ctx = nested.query_ctx();
        let nested_scope = nested_ctx.memory_scope().expect("scope on nested");
        // Same pool, same limit: one budget for the request, not two.
        assert!(Arc::ptr_eq(&first_scope.pool(), &nested_scope.pool()));
        assert!(Arc::ptr_eq(&first_scope.pool(), &entry_scope.pool()));
        assert!(matches!(
            nested_scope.pool().memory_limit(),
            DfMemoryLimit::Finite(4096)
        ));

        // Both executions install their own nested wrapper around that one pool,
        // so what the outer execution reserved is already gone from the nested
        // one's budget, and the node pool is charged once rather than per
        // execution.
        let outer_runtime = first.state().runtime_env();
        let outer = MemoryConsumer::new("flow_42_outer").register(&outer_runtime.memory_pool);
        outer.try_grow(4096).unwrap();
        assert_eq!(4096, nested_scope.pool().reserved());
        assert_eq!(4096, node_memory_pool(&engine).reserved());
        let nested_runtime = nested.state().runtime_env();
        let inner = MemoryConsumer::new("flow_42_nested").register(&nested_runtime.memory_pool);
        inner.try_grow(1).unwrap_err();
        assert_eq!(4096, node_memory_pool(&engine).reserved());
    }

    /// A context captured before execution - as the computed semantic-graph
    /// tables capture it during table lookup - already carries the scope, so the
    /// engine contexts built from it later all use one and the same pool.
    #[cfg(feature = "enterprise")]
    #[test]
    fn captured_context_keeps_one_flow_pool_before_execution() {
        let engine = query_engine_with_node_memory_pool(8192);
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .set_extension(FLOW_MAX_MEMORY_BYTES.to_string(), "4096".to_string())
                .set_extension(FLOW_ID.to_string(), "5".to_string())
                .build(),
        );

        // Resolution happens at the request entry, before the context is captured.
        let captured = engine.with_flow_memory_scope(query_ctx);
        let pool = captured.memory_scope().expect("scope recorded").pool();

        for _ in 0..3 {
            let ctx = engine.engine_context(captured.clone()).unwrap();
            let scoped = ctx.query_ctx();
            let scope = scoped.memory_scope().expect("scope kept");
            assert!(Arc::ptr_eq(&scope.pool(), &pool));
            assert_eq!("flow:5", scope.label());
        }
    }

    #[cfg(feature = "enterprise")]
    #[test]
    fn invalid_flow_budget_warning_is_truncated_and_rate_limited() {
        // A client-supplied value never reaches the log verbatim.
        let long = "1".repeat(MAX_LOGGED_VALUE_CHARS * 4);
        let logged = truncate_for_log(&long);
        assert_eq!(MAX_LOGGED_VALUE_CHARS + 3, logged.len());
        assert!(logged.ends_with("..."));
        assert_eq!("nonsense", truncate_for_log("nonsense"));
        // Bounded by characters: a multi-byte value is cut on a char boundary
        // instead of panicking.
        assert_eq!(
            format!("{}...", "é".repeat(MAX_LOGGED_VALUE_CHARS)),
            truncate_for_log(&"é".repeat(MAX_LOGGED_VALUE_CHARS + 1))
        );

        // The same (flow, value) pair warns once per window, a different pair
        // warns immediately, and sustained traffic still gets one line per window
        // instead of silence.
        let limiter = WarnRateLimiter::default();
        let now = Instant::now();
        let window = INVALID_BUDGET_WARN_WINDOW;
        assert!(limiter.should_emit_at("9\u{0}nonsense", now, window));
        assert!(!limiter.should_emit_at("9\u{0}nonsense", now, window));
        assert!(!limiter.should_emit_at("9\u{0}nonsense", now + window / 2, window));
        assert!(limiter.should_emit_at("9\u{0}nonsense", now + window, window));
        assert!(limiter.should_emit_at("7\u{0}nonsense", now, window));
        assert!(limiter.should_emit_at("9\u{0}0", now, window));

        // Rotating keys cannot grow the limiter without bound.
        for key in 0..(MAX_TRACKED_INVALID_BUDGET_WARNINGS * 2) {
            limiter.should_emit_at(&format!("{key}"), now, window);
        }
        let tracked = limiter
            .last_emitted
            .lock()
            .map(|map| map.len())
            .unwrap_or(0);
        assert!(tracked <= MAX_TRACKED_INVALID_BUDGET_WARNINGS, "{tracked}");
    }

    #[cfg(feature = "enterprise")]
    #[test]
    fn engine_context_clamps_flow_memory_pool_to_node_pool() {
        let engine = query_engine_with_node_memory_pool(1024);
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .set_extension(FLOW_MAX_MEMORY_BYTES.to_string(), "4096".to_string())
                .set_extension(FLOW_ID.to_string(), "7".to_string())
                .build(),
        );

        let ctx = engine.engine_context(query_ctx).unwrap();
        let runtime_env = ctx.state().runtime_env();
        // `min(4096, 1024)`: a Flow that asks for more than the node has is
        // clamped, so the Flow pool is never wider than the node pool.
        assert!(matches!(
            runtime_env.memory_pool.memory_limit(),
            DfMemoryLimit::Finite(1024)
        ));
        assert_eq!("flow-scoped", runtime_env.memory_pool.name());

        let node_pool = node_memory_pool(&engine);
        let reservation = MemoryConsumer::new("flow_7").register(&runtime_env.memory_pool);
        reservation.try_grow(1025).unwrap_err();
        assert_eq!(0, node_pool.reserved());
        assert_eq!(0, reservation.size());
        reservation.try_grow(1024).unwrap();
        assert_eq!(1024, node_pool.reserved());
    }

    #[test]
    fn engine_context_keeps_node_memory_pool_without_flow_budget() {
        let engine = query_engine_with_node_memory_pool(4096);
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .set_extension(
                    "datafusion.optimizer.max_passes".to_string(),
                    "7".to_string(),
                )
                .build(),
        );

        let ctx = engine.engine_context(query_ctx).unwrap();
        let runtime_env = ctx.state().runtime_env();
        // No flow extension: exactly the pool the engine was built with, with
        // no scoping wrapper around it.
        assert!(matches!(
            runtime_env.memory_pool.memory_limit(),
            DfMemoryLimit::Finite(4096)
        ));
        assert_eq!("metrics", runtime_env.memory_pool.name());
        // Nothing was derived, so nothing is recorded either: the query keeps
        // running on the node pool for its whole lifetime.
        assert!(ctx.query_ctx().memory_scope().is_none());
        assert_eq!(7, ctx.state().config().options().optimizer.max_passes);
    }

    #[cfg(feature = "enterprise")]
    #[test]
    fn engine_context_flow_pool_cannot_exceed_the_node_pool() {
        let engine = query_engine_with_node_memory_pool(4096);
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .set_extension(FLOW_MAX_MEMORY_BYTES.to_string(), "3000".to_string())
                .build(),
        );
        let ctx = engine.engine_context(query_ctx).unwrap();
        let runtime_env = ctx.state().runtime_env();
        assert!(matches!(
            runtime_env.memory_pool.memory_limit(),
            DfMemoryLimit::Finite(3000)
        ));

        let node_pool = node_memory_pool(&engine);
        // An ordinary query already took 3200 of the 4096-byte node pool.
        let ordinary = MemoryConsumer::new("ordinary").register(&node_pool);
        ordinary.try_grow(3200).unwrap();

        // The Flow still has its whole 3000-byte budget, but only 896 bytes are
        // left in the node pool, so the node pool refuses: the aggregate of all
        // flows and ordinary queries stays under `query.memory_pool_size`.
        let flow = MemoryConsumer::new("flow").register(&runtime_env.memory_pool);
        let err = flow.try_grow(1024).unwrap_err();
        let message = format!("{err}");
        assert!(message.contains("896.0 B"), "unexpected error: {message}");
        assert!(!message.contains("2.9 KB"), "unexpected error: {message}");
        assert_eq!(3200, node_pool.reserved());
        assert_eq!(0, flow.size());
    }

    #[test]
    fn engine_context_explicit_memory_scope_beats_flow_budget() {
        let engine = query_engine_with_node_memory_pool(8192);
        let explicit = new_flow_memory_pool(2048, QueryMemoryPoolPolicy::Greedy);
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .set_extension(FLOW_MAX_MEMORY_BYTES.to_string(), "4096".to_string())
                .memory_scope(Some(QueryMemoryScope::new("explicit", explicit.clone())))
                .build(),
        );

        // An explicit scope is kept verbatim: the request entry neither forks the
        // context nor replaces the scope the caller installed.
        let scoped_ctx = engine.with_flow_memory_scope(query_ctx.clone());
        assert!(Arc::ptr_eq(&scoped_ctx, &query_ctx));
        assert!(Arc::ptr_eq(
            &scoped_ctx.memory_scope().unwrap().pool(),
            &explicit
        ));

        let ctx = engine.engine_context(query_ctx).unwrap();
        let runtime_env = ctx.state().runtime_env();
        // The explicitly installed scope (2048) wins over the extension (4096).
        assert!(matches!(
            runtime_env.memory_pool.memory_limit(),
            DfMemoryLimit::Finite(2048)
        ));
        assert_eq!("flow-scoped", runtime_env.memory_pool.name());
        // The extension did not replace or re-derive it.
        let scoped_ctx = ctx.query_ctx();
        let scope = scoped_ctx.memory_scope().expect("scope is recorded");
        assert_eq!("explicit", scope.label());
        assert!(Arc::ptr_eq(&scope.pool(), &explicit));

        let reservation = MemoryConsumer::new("scoped").register(&runtime_env.memory_pool);
        reservation.try_grow(2049).unwrap_err();
        reservation.try_grow(2048).unwrap();
        assert_eq!(2048, explicit.reserved());
    }

    #[cfg(feature = "enterprise")]
    #[tokio::test]
    async fn test_invalid_flow_memory_budget_keeps_query_on_node_pool() {
        let engine = query_engine_with_node_memory_pool(4 * 1024 * 1024);
        for raw in ["nonsense", "0"] {
            let query_ctx = Arc::new(
                QueryContextBuilder::default()
                    .set_extension(FLOW_MAX_MEMORY_BYTES.to_string(), raw.to_string())
                    .set_extension(FLOW_ID.to_string(), "9".to_string())
                    .build(),
            );

            // No scope is installed for a value that declares no budget.
            let ctx = engine.engine_context(query_ctx.clone()).unwrap();
            assert!(matches!(
                ctx.state().runtime_env().memory_pool.memory_limit(),
                DfMemoryLimit::Finite(4194304)
            ));
            assert_eq!("metrics", ctx.state().runtime_env().memory_pool.name());

            // The query still runs, it is just not bounded by a Flow budget.
            let stmt = QueryLanguageParser::parse_sql(
                "select sum(number) from numbers limit 20",
                &query_ctx,
            )
            .unwrap();
            let plan = engine
                .planner()
                .plan(&stmt, query_ctx.clone())
                .await
                .unwrap();
            let output = engine.execute(plan, query_ctx).await.unwrap();
            match output.data {
                OutputData::Stream(stream) => {
                    assert_eq!(1, util::collect(stream).await.unwrap().len());
                }
                _ => unreachable!(),
            }
        }
    }
}
