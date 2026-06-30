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

use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_base::Plugins;
use common_function::aggrs::aggr_wrapper::fix_order::FixStateUdafOrderingAnalyzer;
use common_function::function_factory::ScalarFunctionFactory;
use common_function::function_registry::FUNCTION_REGISTRY;
use common_function::handlers::{
    FlowServiceHandlerRef, ProcedureServiceHandlerRef, TableMutationHandlerRef,
};
use common_function::state::FunctionState;
use common_stat::get_total_memory_bytes;
use common_telemetry::warn;
use datafusion::catalog::TableFunction;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result as DfResult;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::{QueryPlanner, SessionConfig, SessionContext, SessionState};
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
    TrackConsumersPool,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_common::config::SpillCompression;
use datafusion_expr::{AggregateUDF, LogicalPlan as DfLogicalPlan, WindowUDF};
use datafusion_optimizer::Analyzer;
use datafusion_optimizer::analyzer::function_rewrite::ApplyFunctionRewrites;
use datafusion_optimizer::optimizer::Optimizer;
use partition::manager::PartitionRuleManagerRef;
use promql::extension_plan::PromExtensionPlanner;
use session::context::QueryContextRef;
use table::TableRef;
use table::table::adapter::DfTableProviderAdapter;

use crate::QueryEngineContext;
use crate::dist_plan::{
    DistExtensionPlanner, DistPlannerAnalyzer, DistPlannerOptions, DynFilterRegistryManager,
    MergeSortExtensionPlanner, RemoteDynFilterReceiverExtensionPlanner,
    RemoteDynFilterRegistryLease,
};
use crate::metrics::{QUERY_MEMORY_POOL_REJECTED_TOTAL, QUERY_MEMORY_POOL_USAGE_BYTES};
use crate::optimizer::ExtensionAnalyzerRule;
use crate::optimizer::const_normalization::ConstNormalizationRule;
use crate::optimizer::constant_term::MatchesConstantTermOptimizer;
use crate::optimizer::count_nest_aggr::CountNestAggrRule;
use crate::optimizer::count_wildcard::CountWildcardToTimeIndexRule;
use crate::optimizer::json_type_concretize::JsonTypeConcretizeRule;
use crate::optimizer::parallelize_scan::ParallelizeScan;
use crate::optimizer::pass_distribution::PassDistribution;
use crate::optimizer::promql_tsid_narrow_join::PromqlTsidNarrowJoin;
use crate::optimizer::remove_duplicate::RemoveDuplicate;
use crate::optimizer::scan_hint::ScanHintRule;
use crate::optimizer::string_normalization::StringNormalizationRule;
use crate::optimizer::transcribe_atat::TranscribeAtatRule;
use crate::optimizer::type_conversion::TypeConversionRule;
use crate::optimizer::windowed_sort::WindowedSortPhysicalRule;
use crate::options::{
    QueryMemoryPoolPolicy, QueryOptions as QueryOptionsNew, QuerySpillCompression, QuerySpillMode,
};
use crate::query_engine::DefaultSerializer;
use crate::query_engine::options::QueryOptions;
use crate::range_select::planner::RangeSelectPlanner;
use crate::region_query::RegionQueryHandlerRef;

/// Query engine global state
#[derive(Clone)]
pub struct QueryEngineState {
    df_context: SessionContext,
    catalog_manager: CatalogManagerRef,
    dyn_filter_registry_manager: Arc<DynFilterRegistryManager>,
    function_state: Arc<FunctionState>,
    scalar_functions: Arc<RwLock<HashMap<String, ScalarFunctionFactory>>>,
    aggr_functions: Arc<RwLock<HashMap<String, AggregateUDF>>>,
    table_functions: Arc<RwLock<HashMap<String, Arc<TableFunction>>>>,
    extension_rules: Vec<Arc<dyn ExtensionAnalyzerRule + Send + Sync>>,
    plugins: Plugins,
}

impl fmt::Debug for QueryEngineState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("QueryEngineState")
            .field("state", &self.df_context.state())
            .finish()
    }
}

impl QueryEngineState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_list: CatalogManagerRef,
        partition_rule_manager: Option<PartitionRuleManagerRef>,
        region_query_handler: Option<RegionQueryHandlerRef>,
        table_mutation_handler: Option<TableMutationHandlerRef>,
        procedure_service_handler: Option<ProcedureServiceHandlerRef>,
        flow_service_handler: Option<FlowServiceHandlerRef>,
        with_dist_planner: bool,
        plugins: Plugins,
        options: QueryOptionsNew,
    ) -> Self {
        let total_memory = get_total_memory_bytes().max(0) as u64;
        let memory_pool_size = options.memory_pool_size.resolve(total_memory) as usize;
        let runtime_env = build_runtime_env(&options, memory_pool_size);
        let mut session_config = SessionConfig::new().with_create_default_catalog_and_schema(false);
        if options.parallelism > 0 {
            session_config = session_config.with_target_partitions(options.parallelism);
        }
        if options.allow_query_fallback {
            session_config
                .options_mut()
                .extensions
                .insert(DistPlannerOptions {
                    allow_query_fallback: true,
                });
        }

        // Set spill compression on session config only when spill mode is
        // Custom. In Default/Disabled modes, DataFusion's own default
        // (Uncompressed) is preserved—setting compression when spill is not
        // explicitly configured would be misleading.
        if options.experimental_spill_mode == QuerySpillMode::Custom {
            session_config.options_mut().execution.spill_compression =
                spill_compression_from_options(options.experimental_spill_compression);
        }

        // todo(hl): This serves as a workaround for https://github.com/GreptimeTeam/greptimedb/issues/5659
        // and we can add that check back once we upgrade datafusion.
        session_config
            .options_mut()
            .execution
            .skip_physical_aggregate_schema_check = true;

        // Apply extension rules
        let mut extension_rules = Vec::new();

        // The [`TypeConversionRule`] must be at first
        extension_rules.insert(0, Arc::new(TypeConversionRule) as _);
        extension_rules.push(Arc::new(CountNestAggrRule) as _);

        // Apply the datafusion rules
        let mut analyzer = Analyzer::new();
        analyzer.rules.insert(0, Arc::new(TranscribeAtatRule));
        analyzer.rules.insert(0, Arc::new(StringNormalizationRule));
        analyzer
            .rules
            .insert(0, Arc::new(CountWildcardToTimeIndexRule));
        analyzer.rules.push(Arc::new(ConstNormalizationRule));

        // Add ApplyFunctionRewrites rule,
        // Note we cannot use `analyzer.add_function_rewrite`
        // because only rules are copied into session_state
        analyzer.rules.insert(
            0,
            Arc::new(ApplyFunctionRewrites::new(
                FUNCTION_REGISTRY.function_rewrites(),
            )),
        );

        if with_dist_planner {
            analyzer.rules.push(Arc::new(DistPlannerAnalyzer));
        }
        analyzer.rules.push(Arc::new(FixStateUdafOrderingAnalyzer));

        let mut optimizer = Optimizer::new();
        optimizer.rules.push(Arc::new(ScanHintRule));
        optimizer.rules.push(Arc::new(JsonTypeConcretizeRule));

        // add physical optimizer
        let mut physical_optimizer = PhysicalOptimizer::new();
        // Change TableScan's partition right before enforcing distribution
        physical_optimizer
            .rules
            .insert(5, Arc::new(ParallelizeScan));
        // Pass distribution requirement to MergeScanExec to avoid unnecessary shuffling
        physical_optimizer
            .rules
            .insert(6, Arc::new(PassDistribution));
        // Prefer collecting narrow PromQL build sides over repartitioning wide label streams.
        physical_optimizer
            .rules
            .insert(7, Arc::new(PromqlTsidNarrowJoin));
        // Enforce sorting AFTER custom rules that modify the plan structure
        physical_optimizer.rules.insert(
            8,
            Arc::new(datafusion::physical_optimizer::enforce_sorting::EnforceSorting {}),
        );
        // Add rule for windowed sort
        physical_optimizer
            .rules
            .push(Arc::new(WindowedSortPhysicalRule));
        // explicitly not do filter pushdown for windowed sort&part sort
        // (notice that `PartSortExec` create another new dyn filter that need to be pushdown if want to use dyn filter optimization)
        // benchmark shows it can cause performance regression due to useless filtering and extra shuffle.
        // We can add a rule to do filter pushdown for windowed sort in the future if we find a way to avoid the performance regression.
        physical_optimizer
            .rules
            .push(Arc::new(MatchesConstantTermOptimizer));
        // Add rule to remove duplicate nodes generated by other rules. Run this in the last.
        physical_optimizer.rules.push(Arc::new(RemoveDuplicate));
        // Place SanityCheckPlan at the end of the list to ensure that it runs after all other rules.
        Self::remove_physical_optimizer_rule(
            &mut physical_optimizer.rules,
            SanityCheckPlan {}.name(),
        );
        physical_optimizer.rules.push(Arc::new(SanityCheckPlan {}));

        let session_state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime_env)
            .with_default_features()
            .with_analyzer_rules(analyzer.rules)
            .with_serializer_registry(Arc::new(DefaultSerializer))
            .with_query_planner(Arc::new(DfQueryPlanner::new(
                catalog_list.clone(),
                partition_rule_manager,
                region_query_handler.clone(),
                options.enable_per_region_metrics,
            )))
            .with_optimizer_rules(optimizer.rules)
            .with_physical_optimizer_rules(physical_optimizer.rules)
            .build();

        let df_context = SessionContext::new_with_state(session_state);
        register_function_aliases(&df_context);

        Self {
            df_context,
            catalog_manager: catalog_list,
            dyn_filter_registry_manager: Arc::new(DynFilterRegistryManager::default()),
            function_state: Arc::new(FunctionState {
                table_mutation_handler,
                procedure_service_handler,
                flow_service_handler,
            }),
            aggr_functions: Arc::new(RwLock::new(HashMap::new())),
            table_functions: Arc::new(RwLock::new(HashMap::new())),
            extension_rules,
            plugins,
            scalar_functions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn remove_physical_optimizer_rule(
        rules: &mut Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
        name: &str,
    ) {
        rules.retain(|rule| rule.name() != name);
    }

    /// Optimize the logical plan by the extension analyzer rules.
    pub fn optimize_by_extension_rules(
        &self,
        plan: DfLogicalPlan,
        context: &QueryEngineContext,
    ) -> DfResult<DfLogicalPlan> {
        self.extension_rules
            .iter()
            .try_fold(plan, |acc_plan, rule| {
                rule.analyze(acc_plan, context, self.session_state().config_options())
            })
    }

    /// Run the full logical plan optimize phase for the given plan.
    pub fn optimize_logical_plan(&self, plan: DfLogicalPlan) -> DfResult<DfLogicalPlan> {
        self.session_state().optimize(&plan)
    }

    /// Retrieve the scalar function by name
    pub fn scalar_function(&self, function_name: &str) -> Option<ScalarFunctionFactory> {
        self.scalar_functions
            .read()
            .unwrap()
            .get(function_name)
            .cloned()
    }

    /// Retrieve scalar function names.
    pub fn scalar_names(&self) -> Vec<String> {
        self.scalar_functions
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }

    /// Retrieve the aggregate function by name
    pub fn aggr_function(&self, function_name: &str) -> Option<AggregateUDF> {
        self.aggr_functions
            .read()
            .unwrap()
            .get(function_name)
            .cloned()
    }

    /// Retrieve aggregate function names.
    pub fn aggr_names(&self) -> Vec<String> {
        self.aggr_functions
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }

    /// Retrieve table function by name
    pub fn table_function(&self, function_name: &str) -> Option<Arc<TableFunction>> {
        self.table_functions
            .read()
            .unwrap()
            .get(function_name)
            .cloned()
    }

    /// Retrieve table function names.
    pub fn table_function_names(&self) -> Vec<String> {
        self.table_functions
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }

    /// Register an scalar function.
    /// Will override if the function with same name is already registered.
    pub fn register_scalar_function(&self, func: ScalarFunctionFactory) {
        let name = func.name().to_string();
        let x = self
            .scalar_functions
            .write()
            .unwrap()
            .insert(name.clone(), func);

        if x.is_some() {
            warn!("Already registered scalar function '{name}'");
        }
    }

    /// Register an aggregate function.
    ///
    /// # Panics
    /// Will panic if the function with same name is already registered.
    ///
    /// Panicking consideration: currently the aggregated functions are all statically registered,
    /// user cannot define their own aggregate functions on the fly. So we can panic here. If that
    /// invariant is broken in the future, we should return an error instead of panicking.
    pub fn register_aggr_function(&self, func: AggregateUDF) {
        let name = func.name().to_string();
        let x = self
            .aggr_functions
            .write()
            .unwrap()
            .insert(name.clone(), func);
        assert!(
            x.is_none(),
            "Already registered aggregate function '{name}'"
        );
    }

    pub fn register_table_function(&self, func: Arc<TableFunction>) {
        let name = func.name();
        let x = self
            .table_functions
            .write()
            .unwrap()
            .insert(name.to_string(), func.clone());

        if x.is_some() {
            warn!("Already registered table function '{name}'");
        }
    }

    /// Register a window function (UDWF) directly on the DataFusion SessionContext.
    ///
    /// This makes the function visible via `session_state.window_functions()`,
    /// which is used by `DfContextProviderAdapter::get_window_meta`.
    pub fn register_window_function(&self, func: WindowUDF) {
        self.df_context.register_udwf(func);
    }

    pub fn catalog_manager(&self) -> &CatalogManagerRef {
        &self.catalog_manager
    }

    pub fn dyn_filter_registry_manager(&self) -> Arc<DynFilterRegistryManager> {
        self.dyn_filter_registry_manager.clone()
    }

    pub fn acquire_remote_dyn_filter_registry_lease(
        &self,
        query_ctx: &QueryContextRef,
    ) -> Option<RemoteDynFilterRegistryLease> {
        let query_id = query_ctx.remote_query_id_value()?;
        Some(
            self.dyn_filter_registry_manager
                .clone()
                .acquire_lease(query_id),
        )
    }

    pub fn function_state(&self) -> Arc<FunctionState> {
        self.function_state.clone()
    }

    /// Returns the [`TableMutationHandlerRef`] in state.
    pub fn table_mutation_handler(&self) -> Option<&TableMutationHandlerRef> {
        self.function_state.table_mutation_handler.as_ref()
    }

    /// Returns the [`ProcedureServiceHandlerRef`] in state.
    pub fn procedure_service_handler(&self) -> Option<&ProcedureServiceHandlerRef> {
        self.function_state.procedure_service_handler.as_ref()
    }

    pub(crate) fn disallow_cross_catalog_query(&self) -> bool {
        self.plugins
            .map::<QueryOptions, _, _>(|x| x.disallow_cross_catalog_query)
            .unwrap_or(false)
    }

    pub fn session_state(&self) -> SessionState {
        self.df_context.state()
    }

    /// Create a DataFrame for a table
    pub fn read_table(&self, table: TableRef) -> DfResult<DataFrame> {
        self.df_context
            .read_table(Arc::new(DfTableProviderAdapter::new(table)))
    }
}

struct DfQueryPlanner {
    physical_planner: DefaultPhysicalPlanner,
}

impl fmt::Debug for DfQueryPlanner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DfQueryPlanner").finish()
    }
}

#[async_trait]
impl QueryPlanner for DfQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &DfLogicalPlan,
        session_state: &SessionState,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        self.physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// MySQL-compatible scalar function aliases: (target_name, alias)
const SCALAR_FUNCTION_ALIASES: &[(&str, &str)] = &[
    ("upper", "ucase"),
    ("lower", "lcase"),
    ("ceil", "ceiling"),
    ("substr", "mid"),
    ("random", "rand"),
];

/// MySQL-compatible aggregate function aliases: (target_name, alias)
const AGGREGATE_FUNCTION_ALIASES: &[(&str, &str)] =
    &[("stddev_pop", "std"), ("var_pop", "variance")];

/// Register function aliases.
///
/// This function adds aliases like `ucase` -> `upper`, `lcase` -> `lower`, etc.
/// to make GreptimeDB more compatible with MySQL syntax.
fn register_function_aliases(ctx: &SessionContext) {
    let state = ctx.state();

    for (target, alias) in SCALAR_FUNCTION_ALIASES {
        if let Some(func) = state.scalar_functions().get(*target) {
            let aliased = func.as_ref().clone().with_aliases([*alias]);
            ctx.register_udf(aliased);
        }
    }

    for (target, alias) in AGGREGATE_FUNCTION_ALIASES {
        if let Some(func) = state.aggregate_functions().get(*target) {
            let aliased = func.as_ref().clone().with_aliases([*alias]);
            ctx.register_udaf(aliased);
        }
    }
}

impl DfQueryPlanner {
    fn new(
        catalog_manager: CatalogManagerRef,
        partition_rule_manager: Option<PartitionRuleManagerRef>,
        region_query_handler: Option<RegionQueryHandlerRef>,
        enable_per_region_metrics: bool,
    ) -> Self {
        let mut planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> = vec![
            Arc::new(PromExtensionPlanner),
            Arc::new(RangeSelectPlanner),
            Arc::new(RemoteDynFilterReceiverExtensionPlanner),
        ];
        if let (Some(region_query_handler), Some(partition_rule_manager)) =
            (region_query_handler, partition_rule_manager)
        {
            planners.push(Arc::new(DistExtensionPlanner::new(
                catalog_manager,
                partition_rule_manager,
                region_query_handler,
                enable_per_region_metrics,
            )));
            planners.push(Arc::new(MergeSortExtensionPlanner {}));
        }
        Self {
            physical_planner: DefaultPhysicalPlanner::with_extension_planners(planners),
        }
    }
}

/// A wrapper around a memory pool that records metrics.
///
/// This wrapper intercepts all memory pool operations and updates
/// Prometheus metrics for monitoring query memory usage and rejections.
///
/// The inner pool is always wrapped with `TrackConsumersPool` to preserve
/// top-consumer error context on rejection, regardless of whether the
/// underlying pool is greedy or fair.
#[derive(Debug)]
struct MetricsMemoryPool {
    inner: Arc<dyn MemoryPool>,
}

impl MetricsMemoryPool {
    // Number of top memory consumers to report in OOM error messages
    const TOP_CONSUMERS_TO_REPORT: usize = 5;

    /// Create a new metrics-wrapped memory pool with the given size limit and
    /// allocation policy. Both greedy and fair pools are wrapped in
    /// [`TrackConsumersPool`] to preserve top-consumer error context on rejection.
    fn new(limit: usize, policy: QueryMemoryPoolPolicy) -> Self {
        let top_n = NonZeroUsize::new(Self::TOP_CONSUMERS_TO_REPORT).unwrap();
        let inner: Arc<dyn MemoryPool> = match policy {
            QueryMemoryPoolPolicy::Greedy => {
                Arc::new(TrackConsumersPool::new(GreedyMemoryPool::new(limit), top_n))
            }
            QueryMemoryPoolPolicy::Fair => {
                Arc::new(TrackConsumersPool::new(FairSpillPool::new(limit), top_n))
            }
        };
        Self { inner }
    }

    #[inline]
    fn update_metrics(&self) {
        QUERY_MEMORY_POOL_USAGE_BYTES.set(self.inner.reserved() as i64);
    }
}

impl MemoryPool for MetricsMemoryPool {
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.update_metrics();
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.update_metrics();
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion_common::Result<()> {
        let result = self.inner.try_grow(reservation, additional);
        if result.is_err() {
            QUERY_MEMORY_POOL_REJECTED_TOTAL.inc();
        }
        self.update_metrics();
        result
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

/// Map [`QuerySpillCompression`] to DataFusion's [`SpillCompression`].
///
/// This conversion is intentionally not a `From` impl because the
/// semantics depend on the spill mode; callers should only invoke
/// this when `experimental_spill_mode == Custom`.
fn spill_compression_from_options(comp: QuerySpillCompression) -> SpillCompression {
    match comp {
        QuerySpillCompression::Uncompressed => SpillCompression::Uncompressed,
        QuerySpillCompression::Lz4Frame => SpillCompression::Lz4Frame,
        QuerySpillCompression::Zstd => SpillCompression::Zstd,
    }
}

/// Build an [`Arc<RuntimeEnv>`] from query options and resolved memory pool size.
///
/// # Spill mode behavior
///
/// - `Default`: preserves DataFusion built-in behavior. No custom disk manager
///   builder is attached. A bounded metrics memory pool is attached only when
///   `memory_pool_size > 0`.
/// - `Custom`: attaches a [`DiskManagerBuilder`] with an optional explicit
///   directory and max temp directory size.
/// - `Disabled`: explicitly uses [`DiskManagerMode::Disabled`]; temporary file
///   creation will error.
///
/// # Panics
///
/// Panics if `RuntimeEnvBuilder::build()` fails (e.g., cannot create the
/// configured spill directory). The panic message includes the spill mode,
/// path, and quota for diagnostics.
fn build_runtime_env(options: &QueryOptionsNew, memory_pool_size: usize) -> Arc<RuntimeEnv> {
    let mut builder = RuntimeEnvBuilder::new();

    // Attach the bounded metrics memory pool only when limit is set (>0).
    // When unbounded (0), use the builder default / UnboundedMemoryPool.
    if memory_pool_size > 0 {
        let pool =
            MetricsMemoryPool::new(memory_pool_size, options.experimental_memory_pool_policy);
        builder = builder.with_memory_pool(Arc::new(pool));
    }

    match options.experimental_spill_mode {
        QuerySpillMode::Default => {
            // No custom disk manager builder; preserve DataFusion default OS temp dir.
        }
        QuerySpillMode::Disabled => {
            let dm_builder = DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled);
            builder = builder.with_disk_manager_builder(dm_builder);
        }
        QuerySpillMode::Custom => {
            let mut dm_builder = DiskManagerBuilder::default();
            if let Some(ref path) = options.experimental_spill_path {
                dm_builder = dm_builder.with_mode(DiskManagerMode::Directories(vec![path.clone()]));
            }
            dm_builder = dm_builder.with_max_temp_directory_size(
                options
                    .experimental_spill_max_temp_directory_size
                    .as_bytes(),
            );
            builder = builder.with_disk_manager_builder(dm_builder);
        }
    }

    builder.build().map(Arc::new).unwrap_or_else(|e| {
        panic!(
            "Failed to build DataFusion RuntimeEnv: {e}\
             (spill_mode={:?}, spill_path={:?}, spill_max_temp_directory_size={})",
            options.experimental_spill_mode,
            options.experimental_spill_path,
            options.experimental_spill_max_temp_directory_size,
        )
    })
}

#[cfg(test)]
mod tests {
    use common_base::Plugins;
    use session::context::QueryContext;

    use super::*;
    use crate::options::QueryOptions;

    fn new_query_engine_state() -> QueryEngineState {
        QueryEngineState::new(
            catalog::memory::new_memory_catalog_manager().unwrap(),
            None,
            None,
            None,
            None,
            None,
            false,
            Plugins::default(),
            QueryOptions::default(),
        )
    }

    #[test]
    fn query_engine_state_reuses_query_scoped_dyn_filter_registry_lease() {
        let state = new_query_engine_state();
        let query_ctx = QueryContext::arc();

        let first = state
            .acquire_remote_dyn_filter_registry_lease(&query_ctx)
            .unwrap();
        let second = state
            .acquire_remote_dyn_filter_registry_lease(&query_ctx)
            .unwrap();

        assert!(first.ptr_eq(&second));
        assert_eq!(state.dyn_filter_registry_manager().registry_count(), 1);
        assert_eq!(
            first.registry().query_id(),
            query_ctx.remote_query_id_value().unwrap()
        );
    }

    #[test]
    fn query_engine_state_relies_on_query_context_remote_query_id_contract() {
        let state = new_query_engine_state();
        let query_ctx = QueryContext::arc();

        assert!(query_ctx.remote_query_id_value().is_some());

        let lease = state
            .acquire_remote_dyn_filter_registry_lease(&query_ctx)
            .unwrap();

        assert_eq!(
            lease.registry().query_id(),
            query_ctx.remote_query_id_value().unwrap()
        );
        assert_eq!(state.dyn_filter_registry_manager().registry_count(), 1);
    }

    #[test]
    fn query_engine_state_separates_registries_for_different_query_contexts() {
        let state = new_query_engine_state();
        let first_query_ctx = QueryContext::arc();
        let second_query_ctx = QueryContext::arc();

        let first = state
            .acquire_remote_dyn_filter_registry_lease(&first_query_ctx)
            .unwrap();
        let second = state
            .acquire_remote_dyn_filter_registry_lease(&second_query_ctx)
            .unwrap();

        assert!(!first.ptr_eq(&second));
        assert_eq!(state.dyn_filter_registry_manager().registry_count(), 2);
        assert_eq!(
            first.registry().query_id(),
            first_query_ctx.remote_query_id_value().unwrap()
        );
        assert_eq!(
            second.registry().query_id(),
            second_query_ctx.remote_query_id_value().unwrap()
        );
    }

    // --- spill / memory pool tests ---

    /// Default mode: unbounded memory, OS temp dir disk manager.
    #[test]
    fn test_build_runtime_env_default_mode_unbounded() {
        let opts = QueryOptions::default();
        let env = build_runtime_env(&opts, 0);
        // Default mode + unbounded pool: no custom disk manager override,
        // so tmp files should be enabled (DataFusion default).
        assert!(env.disk_manager.tmp_files_enabled());
    }

    /// Default mode with bounded memory: pool is attached but disk manager
    /// remains default.
    #[test]
    fn test_build_runtime_env_default_mode_bounded() {
        let mut opts = QueryOptions::default();
        opts.memory_pool_size = common_base::memory_limit::MemoryLimit::Size(
            common_base::readable_size::ReadableSize::mb(128),
        );
        let env = build_runtime_env(&opts, 128 * 1024 * 1024);
        // Bounded pool is attached
        assert!(env.memory_pool.reserved() == 0);
        // Disk manager remains default (OS temp dir)
        assert!(env.disk_manager.tmp_files_enabled());
    }

    /// Custom mode with explicit path: disk manager is configured with the
    /// custom directory and the directory actually gets created.
    #[test]
    fn test_build_runtime_env_custom_mode_with_path() {
        // Use a temp directory managed manually so we don't need the `tempfile` crate.
        let spill_dir = std::env::temp_dir().join(format!("df_spill_test_{}", std::process::id()));
        // Clean up if exists from a previous run
        let _ = std::fs::remove_dir_all(&spill_dir);
        let mut opts = QueryOptions::default();
        opts.experimental_spill_mode = QuerySpillMode::Custom;
        opts.experimental_spill_path = Some(spill_dir.clone());
        opts.experimental_spill_max_temp_directory_size =
            common_base::readable_size::ReadableSize::gb(1);
        let env = build_runtime_env(&opts, 0);

        assert!(env.disk_manager.tmp_files_enabled());
        // Creating a temp file should succeed
        let tmp_file = env.disk_manager.create_tmp_file("test spill");
        assert!(tmp_file.is_ok());
        // The directory should have been created
        assert!(spill_dir.exists());

        // Cleanup
        let _ = std::fs::remove_dir_all(&spill_dir);
    }

    /// Disabled mode: tmp files are disabled; creating a temp file returns an error.
    #[test]
    fn test_build_runtime_env_disabled_mode() {
        let mut opts = QueryOptions::default();
        opts.experimental_spill_mode = QuerySpillMode::Disabled;
        let env = build_runtime_env(&opts, 0);

        assert!(!env.disk_manager.tmp_files_enabled());
        let result = env.disk_manager.create_tmp_file("test spill");
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("DiskManager is disabled"));
    }

    /// MetricsMemoryPool defaults to greedy with TrackConsumersPool wrapping.
    #[test]
    fn test_metrics_memory_pool_default_greedy() {
        let pool = MetricsMemoryPool::new(1024, QueryMemoryPoolPolicy::Greedy);
        assert!(matches!(pool.memory_limit(), MemoryLimit::Finite(1024)));
        // Smoke test: register a consumer and grow
        let pool: Arc<dyn MemoryPool> = Arc::new(pool);
        let consumer = MemoryConsumer::new("test");
        let reservation = consumer.register(&pool);
        pool.try_grow(&reservation, 128).unwrap();
        assert!(pool.reserved() > 0);
    }

    /// MetricsMemoryPool with fair policy: still wrapped in TrackConsumersPool.
    #[test]
    fn test_metrics_memory_pool_fair() {
        let pool = MetricsMemoryPool::new(1024, QueryMemoryPoolPolicy::Fair);
        assert!(matches!(pool.memory_limit(), MemoryLimit::Finite(1024)));
        let pool: Arc<dyn MemoryPool> = Arc::new(pool);
        let consumer = MemoryConsumer::new("test fair");
        let reservation = consumer.register(&pool);
        pool.try_grow(&reservation, 128).unwrap();
        assert!(pool.reserved() > 0);
    }

    /// Bounded fair pool rejects over-limit growth.
    #[test]
    fn test_metrics_memory_pool_fair_rejects_over_limit() {
        let pool = MetricsMemoryPool::new(200, QueryMemoryPoolPolicy::Fair);
        let pool: Arc<dyn MemoryPool> = Arc::new(pool);
        let consumer = MemoryConsumer::new("test fair oom");
        let reservation = consumer.register(&pool);
        let result = pool.try_grow(&reservation, 201);
        assert!(result.is_err(), "expected error but got Ok");
    }

    /// Bounded greedy pool rejects over-limit growth.
    #[test]
    fn test_metrics_memory_pool_greedy_rejects_over_limit() {
        let pool = MetricsMemoryPool::new(200, QueryMemoryPoolPolicy::Greedy);
        let pool: Arc<dyn MemoryPool> = Arc::new(pool);
        let consumer = MemoryConsumer::new("test greedy oom");
        let reservation = consumer.register(&pool);
        let result = pool.try_grow(&reservation, 201);
        assert!(result.is_err(), "expected error but got Ok");
    }

    // --- end-to-end spill smoke test ---

    /// Builds a runtime with custom spill configuration and a bounded memory
    /// pool, then runs sort queries that probe spill-to-disk behaviour:
    ///
    /// - Phase 1: a pool too small to sort in-memory → OOM (proves pool active).
    /// - Phase 2: a pool large enough for merge chunks but still much smaller
    ///   than the total data.  Executes the physical plan directly so we can
    ///   walk the plan tree afterwards and sum `spill_count` / `spilled_rows` /
    ///   `spilled_bytes` on `SortExec` nodes.  All three must be > 0.
    ///
    /// Also checks the custom spill-path directory was initialised, pool
    /// reserved returns to 0, and output is correctly sorted.
    #[tokio::test]
    async fn test_sort_spill_smoke_with_custom_runtime() {
        // ---- shared imports (in-function to avoid polluting the module) ----
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::physical_plan::collect as df_collect;

        // ---- setup: spill directory ----
        let spill_dir =
            std::env::temp_dir().join(format!("greptime_spill_smoke_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&spill_dir);

        // ---- shared options ----
        let mut opts = QueryOptions::default();
        opts.experimental_spill_mode = QuerySpillMode::Custom;
        opts.experimental_spill_path = Some(spill_dir.clone());
        opts.experimental_spill_max_temp_directory_size =
            common_base::readable_size::ReadableSize::gb(1);
        opts.experimental_memory_pool_policy = QueryMemoryPoolPolicy::Greedy;

        // ---- shared session config ----
        let session_config = SessionConfig::new()
            .with_target_partitions(1)
            .with_sort_in_place_threshold_bytes(0)
            .with_sort_spill_reservation_bytes(64 * 1024);

        // ---- data: 200 K rows Int32×2, split into 40 batches of 5 K ----
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let n_rows: i32 = 200_000;
        let batch_size: i32 = 5_000;
        let partitions = 1usize;

        let mut table_partitions: Vec<Vec<RecordBatch>> = Vec::with_capacity(partitions);
        for _ in 0..partitions {
            let mut batches = Vec::new();
            let mut row_offset: i32 = 0;
            while row_offset < n_rows {
                let chunk_end = (row_offset + batch_size).min(n_rows);
                let chunk_len = (chunk_end - row_offset) as usize;
                let mut id_builder = Int32Array::builder(chunk_len);
                let mut val_builder = Int32Array::builder(chunk_len);
                for i in row_offset..chunk_end {
                    id_builder.append_value(i);
                    val_builder.append_value(n_rows - 1 - i);
                }
                batches.push(
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(id_builder.finish()),
                            Arc::new(val_builder.finish()),
                        ],
                    )
                    .unwrap(),
                );
                row_offset = chunk_end;
            }
            table_partitions.push(batches);
        }

        // ---- helper: build a SessionContext from options + data ----
        fn build_ctx(
            session_config: &SessionConfig,
            runtime: &Arc<RuntimeEnv>,
            schema: &Arc<Schema>,
            partitions: &[Vec<RecordBatch>],
        ) -> SessionContext {
            let session_state = SessionStateBuilder::new()
                .with_config(session_config.clone())
                .with_runtime_env(Arc::clone(runtime))
                .with_default_features()
                .build();
            let ctx = SessionContext::new_with_state(session_state);
            let table = MemTable::try_new(Arc::clone(schema), partitions.to_vec()).unwrap();
            ctx.register_table("t", Arc::new(table)).unwrap();
            ctx
        }

        // ---- Phase 1: 64 KB pool → OOM (proves pool is active) ----
        {
            let mem_limit: usize = 64 * 1024;
            let runtime = build_runtime_env(&opts, mem_limit);
            let ctx = build_ctx(&session_config, &runtime, &schema, &table_partitions);
            let result = ctx
                .sql("SELECT * FROM t ORDER BY val ASC")
                .await
                .unwrap()
                .collect()
                .await;
            assert!(
                result.is_err(),
                "64 KB pool should be too small for 200K rows, but query succeeded"
            );
            let err_msg = format!("{}", result.unwrap_err());
            assert!(
                err_msg.contains("Resources exhausted") || err_msg.contains("Memory Exhausted"),
                "error should mention resource exhaustion: {err_msg}"
            );
            assert_eq!(runtime.memory_pool.reserved(), 0);
        }

        // ---- Phase 2: 512 KB pool → spilling to disk ----
        {
            let mem_limit: usize = 512 * 1024; // 512 KB pool, 64 KB reserved for merge

            let runtime = build_runtime_env(&opts, mem_limit);
            assert_eq!(runtime.memory_pool.reserved(), 0);
            let ctx = build_ctx(&session_config, &runtime, &schema, &table_partitions);

            // Execute via direct physical-plan path so we can inspect metrics.
            let df = ctx
                .sql("SELECT val FROM t ORDER BY val ASC")
                .await
                .expect("planning ORDER BY");
            let plan = df
                .create_physical_plan()
                .await
                .expect("creating physical plan");
            let task_ctx = ctx.task_ctx();

            let batches = df_collect(Arc::clone(&plan), task_ctx)
                .await
                .expect("executing ORDER BY");

            // ---- spill metrics on SortExec nodes ----
            let (spill_count, spilled_rows, spilled_bytes, _sort_elapsed_ns) =
                sum_sort_spill_metrics(&plan);
            assert!(
                spill_count > 0,
                "expected SortExec spill_count > 0 (pool={} KB, reservation=64 KB), got 0",
                mem_limit / 1024,
            );
            assert!(
                spilled_rows > 0,
                "expected SortExec spilled_rows > 0, got 0 \
                 (spill_count={spill_count}, spilled_bytes={spilled_bytes})",
            );
            assert!(
                spilled_bytes > 0,
                "expected SortExec spilled_bytes > 0, got 0 \
                 (spill_count={spill_count}, spilled_rows={spilled_rows})",
            );

            // ---- correctness: row count & sorted order ----
            let vals: Vec<i32> = batches
                .iter()
                .flat_map(|b| {
                    let col = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                    (0..b.num_rows()).map(move |i| col.value(i))
                })
                .collect();
            assert_eq!(vals.len(), n_rows as usize);
            for w in vals.windows(2) {
                assert!(w[0] <= w[1], "sort order violation: {} > {}", w[0], w[1]);
            }

            // ---- spill-path directory was initialised ----
            assert!(
                std::fs::read_dir(&spill_dir)
                    .ok()
                    .map(|mut entries| entries.any(|e| {
                        e.as_ref()
                            .map(|de| de.file_name().to_string_lossy().starts_with("datafusion-"))
                            .unwrap_or(false)
                    }))
                    .unwrap_or(false),
                "Expected 'datafusion-*' directory in spill path {:?} \
                 (DiskManager should create it on build).",
                spill_dir,
            );

            // Memory pool fully released.
            assert_eq!(runtime.memory_pool.reserved(), 0);
        }

        // ---- cleanup ----
        let _ = std::fs::remove_dir_all(&spill_dir);
    }

    /// Manual local benchmark for spill-to-disk experiments.
    ///
    /// This is ignored by default and intended for local tuning only. Example:
    ///
    /// ```text
    /// GT_SPILL_BENCH_ROWS=1000000 \
    /// GT_SPILL_BENCH_PAYLOAD_MODE=metric \
    /// GT_SPILL_BENCH_PAYLOAD_BYTES=256 \
    /// GT_SPILL_BENCH_REPEATS=2 \
    /// GT_SPILL_BENCH_SPILL_MB_LIST=64,128,256,384,512 \
    /// GT_SPILL_BENCH_INMEM_MB=2048 \
    /// GT_SPILL_BENCH_CASES=spill-fair-uncompressed \
    /// cargo test --release -p query -- bench_sort_spill_local --ignored --nocapture --test-threads=1
    /// ```
    #[tokio::test]
    #[ignore]
    async fn bench_sort_spill_local() {
        use std::time::Instant;

        use arrow::array::{Int32Array, StringBuilder};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::physical_plan::collect as df_collect;

        fn env_usize(name: &str, default: usize) -> usize {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }

        fn env_usize_list(name: &str, default: Vec<usize>) -> Vec<usize> {
            std::env::var(name)
                .ok()
                .map(|value| {
                    value
                        .split(',')
                        .filter_map(|item| {
                            let item = item.trim();
                            if item.is_empty() {
                                None
                            } else {
                                Some(item.parse::<usize>().unwrap())
                            }
                        })
                        .collect::<Vec<_>>()
                })
                .filter(|values| !values.is_empty())
                .unwrap_or(default)
        }

        fn mix64(mut value: u64) -> u64 {
            value = value.wrapping_add(0x9e3779b97f4a7c15);
            value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
            value ^ (value >> 31)
        }

        fn write_payload(buf: &mut String, mode: &str, row: usize, target_len: usize) {
            use std::fmt::Write;

            buf.clear();
            match mode {
                "repeat" => {
                    buf.extend(std::iter::repeat_n('x', target_len));
                }
                "metric" => {
                    let h1 = mix64(row as u64);
                    let h2 = mix64((row as u64) ^ 0x517cc1b727220a95);
                    write!(
                        buf,
                        "metric=cpu_usage,host=host-{:06},region=region-{},service=svc-{},pod=pod-{:08x},instance={:016x}",
                        row % 1_000_000,
                        row % 32,
                        row % 4096,
                        h1 as u32,
                        h2,
                    )
                    .unwrap();
                    let mut tag = 0;
                    while buf.len() < target_len {
                        let hash = mix64((row as u64).wrapping_add(tag));
                        write!(buf, ",tag{}={:016x}", tag, hash).unwrap();
                        tag += 1;
                    }
                    buf.truncate(target_len);
                }
                "random" => {
                    let mut tag = 0;
                    while buf.len() < target_len {
                        let hash = mix64((row as u64).wrapping_add(tag));
                        write!(buf, "{:016x}", hash).unwrap();
                        tag += 1;
                    }
                    buf.truncate(target_len);
                }
                other => panic!("unsupported GT_SPILL_BENCH_PAYLOAD_MODE: {other}"),
            }
        }

        fn memory_limit_label(limit: usize) -> String {
            if limit == 0 {
                "unlimited".to_string()
            } else {
                format!("{}MiB", limit / 1024 / 1024)
            }
        }

        fn env_string_list(name: &str) -> Vec<String> {
            std::env::var(name)
                .ok()
                .map(|value| {
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|item| !item.is_empty())
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        }

        let n_rows = env_usize("GT_SPILL_BENCH_ROWS", 300_000);
        let batch_size = env_usize("GT_SPILL_BENCH_BATCH_SIZE", 10_000);
        let payload_bytes = env_usize("GT_SPILL_BENCH_PAYLOAD_BYTES", 128);
        let payload_mode =
            std::env::var("GT_SPILL_BENCH_PAYLOAD_MODE").unwrap_or_else(|_| "repeat".to_string());
        let repeats = env_usize("GT_SPILL_BENCH_REPEATS", 3);
        let spill_mb = env_usize("GT_SPILL_BENCH_SPILL_MB", 16);
        let spill_mb_list = env_usize_list("GT_SPILL_BENCH_SPILL_MB_LIST", vec![spill_mb]);
        let in_memory_mb = env_usize("GT_SPILL_BENCH_INMEM_MB", 256);
        let sort_reservation_mb = env_usize("GT_SPILL_BENCH_SORT_RESERVATION_MB", 1);
        let case_filter = env_string_list("GT_SPILL_BENCH_CASES");
        let in_memory_limit = in_memory_mb * 1024 * 1024;
        let sort_reservation = sort_reservation_mb * 1024 * 1024;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, false),
        ]));

        let mut batches = Vec::new();
        let mut row_offset = 0usize;
        let mut payload = String::with_capacity(payload_bytes + 128);
        while row_offset < n_rows {
            let chunk_end = (row_offset + batch_size).min(n_rows);
            let chunk_len = chunk_end - row_offset;
            let mut id_builder = Int32Array::builder(chunk_len);
            let mut val_builder = Int32Array::builder(chunk_len);
            let mut payload_builder = StringBuilder::new();
            for i in row_offset..chunk_end {
                id_builder.append_value(i as i32);
                val_builder.append_value((n_rows - 1 - i) as i32);
                write_payload(&mut payload, payload_mode.as_str(), i, payload_bytes);
                payload_builder.append_value(payload.as_str());
            }
            batches.push(
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(id_builder.finish()),
                        Arc::new(val_builder.finish()),
                        Arc::new(payload_builder.finish()),
                    ],
                )
                .unwrap(),
            );
            row_offset = chunk_end;
        }
        let table_partitions = vec![batches];

        fn build_ctx(
            mut session_config: SessionConfig,
            opts: &QueryOptions,
            runtime: &Arc<RuntimeEnv>,
            schema: &Arc<Schema>,
            partitions: &[Vec<RecordBatch>],
        ) -> SessionContext {
            if opts.experimental_spill_mode == QuerySpillMode::Custom {
                session_config.options_mut().execution.spill_compression =
                    spill_compression_from_options(opts.experimental_spill_compression);
            }
            let session_state = SessionStateBuilder::new()
                .with_config(session_config)
                .with_runtime_env(Arc::clone(runtime))
                .with_default_features()
                .build();
            let ctx = SessionContext::new_with_state(session_state);
            let table = MemTable::try_new(Arc::clone(schema), partitions.to_vec()).unwrap();
            ctx.register_table("t", Arc::new(table)).unwrap();
            ctx
        }

        #[derive(Clone, Copy)]
        struct Case {
            name: &'static str,
            mode: QuerySpillMode,
            compression: QuerySpillCompression,
            policy: QueryMemoryPoolPolicy,
            mem_limit: usize,
        }

        let mut cases = Vec::new();
        for spill_mb in &spill_mb_list {
            let mem_limit = spill_mb * 1024 * 1024;
            cases.extend([
                Case {
                    name: "disabled-same-limit",
                    mode: QuerySpillMode::Disabled,
                    compression: QuerySpillCompression::Uncompressed,
                    policy: QueryMemoryPoolPolicy::Greedy,
                    mem_limit,
                },
                Case {
                    name: "spill-greedy-uncompressed",
                    mode: QuerySpillMode::Custom,
                    compression: QuerySpillCompression::Uncompressed,
                    policy: QueryMemoryPoolPolicy::Greedy,
                    mem_limit,
                },
                Case {
                    name: "spill-greedy-lz4",
                    mode: QuerySpillMode::Custom,
                    compression: QuerySpillCompression::Lz4Frame,
                    policy: QueryMemoryPoolPolicy::Greedy,
                    mem_limit,
                },
                Case {
                    name: "spill-greedy-zstd",
                    mode: QuerySpillMode::Custom,
                    compression: QuerySpillCompression::Zstd,
                    policy: QueryMemoryPoolPolicy::Greedy,
                    mem_limit,
                },
                Case {
                    name: "spill-fair-uncompressed",
                    mode: QuerySpillMode::Custom,
                    compression: QuerySpillCompression::Uncompressed,
                    policy: QueryMemoryPoolPolicy::Fair,
                    mem_limit,
                },
            ]);
        }
        cases.extend([
            Case {
                name: "bounded-inmem-greedy-uncompressed",
                mode: QuerySpillMode::Custom,
                compression: QuerySpillCompression::Uncompressed,
                policy: QueryMemoryPoolPolicy::Greedy,
                mem_limit: in_memory_limit,
            },
            Case {
                name: "unlimited-greedy-uncompressed",
                mode: QuerySpillMode::Custom,
                compression: QuerySpillCompression::Uncompressed,
                policy: QueryMemoryPoolPolicy::Greedy,
                mem_limit: 0,
            },
        ]);

        if !case_filter.is_empty() {
            cases.retain(|case| case_filter.iter().any(|name| name == case.name));
            assert!(
                !cases.is_empty(),
                "GT_SPILL_BENCH_CASES did not match any benchmark cases: {:?}",
                case_filter
            );
        }

        let base_spill_dir =
            std::env::temp_dir().join(format!("greptime_spill_bench_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base_spill_dir);

        println!(
            "BENCH_DATA rows={} batch_size={} payload_mode={} payload_bytes={} raw_payload_mb={:.1} repeats={} spill_mb_list={:?} in_memory_mb={} sort_reservation_mb={} cases={:?}",
            n_rows,
            batch_size,
            payload_mode,
            payload_bytes,
            n_rows as f64 * payload_bytes as f64 / (1024.0 * 1024.0),
            repeats,
            spill_mb_list,
            in_memory_mb,
            sort_reservation_mb,
            case_filter,
        );
        println!(
            "BENCH_COLUMNS name,memory_limit,iter,status,elapsed_ms,sort_elapsed_ms,spill_count,spilled_rows,spilled_bytes,memory_reserved,error"
        );

        for case in cases {
            for iter in 0..repeats {
                let spill_dir = base_spill_dir.join(format!("{}-{}", case.name, iter));
                let _ = std::fs::remove_dir_all(&spill_dir);
                std::fs::create_dir_all(&spill_dir).unwrap();

                let mut opts = QueryOptions::default();
                opts.experimental_spill_mode = case.mode;
                opts.experimental_spill_path = Some(spill_dir.clone());
                opts.experimental_spill_max_temp_directory_size =
                    common_base::readable_size::ReadableSize::gb(8);
                opts.experimental_spill_compression = case.compression;
                opts.experimental_memory_pool_policy = case.policy;

                let runtime = build_runtime_env(&opts, case.mem_limit);
                let session_config = SessionConfig::new()
                    .with_target_partitions(1)
                    .with_sort_in_place_threshold_bytes(0)
                    .with_sort_spill_reservation_bytes(sort_reservation);
                let ctx = build_ctx(session_config, &opts, &runtime, &schema, &table_partitions);

                let started = Instant::now();
                let result = async {
                    let df = ctx
                        .sql("SELECT val, payload FROM t ORDER BY val ASC")
                        .await?;
                    let plan = df.create_physical_plan().await?;
                    let batches = df_collect(Arc::clone(&plan), ctx.task_ctx()).await?;
                    let (spill_count, spilled_rows, spilled_bytes, sort_elapsed_ns) =
                        sum_sort_spill_metrics(&plan);
                    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    assert_eq!(rows, n_rows);
                    Ok::<_, datafusion_common::DataFusionError>((
                        spill_count,
                        spilled_rows,
                        spilled_bytes,
                        sort_elapsed_ns,
                    ))
                }
                .await;
                let elapsed = started.elapsed();
                let mem_label = memory_limit_label(case.mem_limit);
                match result {
                    Ok((spill_count, spilled_rows, spilled_bytes, sort_elapsed_ns)) => println!(
                        "BENCH_RESULT {},{},{},ok,{:.3},{:.3},{},{},{},{},",
                        case.name,
                        mem_label,
                        iter,
                        elapsed.as_secs_f64() * 1000.0,
                        sort_elapsed_ns as f64 / 1_000_000.0,
                        spill_count,
                        spilled_rows,
                        spilled_bytes,
                        runtime.memory_pool.reserved(),
                    ),
                    Err(e) => println!(
                        "BENCH_RESULT {},{},{},err,{:.3},0,0,0,0,{},{}",
                        case.name,
                        mem_label,
                        iter,
                        elapsed.as_secs_f64() * 1000.0,
                        runtime.memory_pool.reserved(),
                        format!("{e}").replace('\n', " "),
                    ),
                }
                assert_eq!(runtime.memory_pool.reserved(), 0);
            }
        }

        let _ = std::fs::remove_dir_all(&base_spill_dir);
    }

    /// Walk a physical plan tree, summing spill metrics and `elapsed_compute`
    /// for every node whose name starts with `"SortExec"`.
    fn sum_sort_spill_metrics(plan: &Arc<dyn ExecutionPlan>) -> (usize, usize, usize, usize) {
        let mut spill_count = 0usize;
        let mut spilled_rows = 0usize;
        let mut spilled_bytes = 0usize;
        let mut elapsed_compute_ns = 0usize;

        fn walk(
            plan: &Arc<dyn ExecutionPlan>,
            sc: &mut usize,
            sr: &mut usize,
            sb: &mut usize,
            elapsed: &mut usize,
        ) {
            let name = plan.name();
            if name.starts_with("SortExec") {
                if let Some(m) = plan.metrics() {
                    *sc += m.spill_count().unwrap_or(0);
                    *sr += m.spilled_rows().unwrap_or(0);
                    *sb += m.spilled_bytes().unwrap_or(0);
                    *elapsed += m.elapsed_compute().unwrap_or(0);
                }
            }
            for child in plan.children() {
                walk(child, sc, sr, sb, elapsed);
            }
        }

        walk(
            plan,
            &mut spill_count,
            &mut spilled_rows,
            &mut spilled_bytes,
            &mut elapsed_compute_ns,
        );
        (spill_count, spilled_rows, spilled_bytes, elapsed_compute_ns)
    }
}
