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
use datafusion::catalog::{Session, TableFunction};
use datafusion::dataframe::DataFrame;
use datafusion::error::Result as DfResult;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::{QueryPlanner, SessionConfig, SessionContext, SessionState};
use datafusion::execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
    TrackConsumersPool,
};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
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
use crate::optimizer::global_limit::EnsureGlobalLimitForFetch;
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
use crate::options::{QueryMemoryPoolPolicy, QueryOptions as QueryOptionsNew};
use crate::query_engine::DefaultSerializer;
use crate::query_engine::options::QueryOptions;
use crate::query_engine::runtime::{
    DefaultQueryRuntimeProvider, QueryRuntimeContext, QueryRuntimeProvider, QueryRuntimeProviderRef,
};
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
        Self::try_new(
            catalog_list,
            partition_rule_manager,
            region_query_handler,
            table_mutation_handler,
            procedure_service_handler,
            flow_service_handler,
            with_dist_planner,
            plugins,
            options,
        )
        .expect("Failed to build query engine state")
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        catalog_list: CatalogManagerRef,
        partition_rule_manager: Option<PartitionRuleManagerRef>,
        region_query_handler: Option<RegionQueryHandlerRef>,
        table_mutation_handler: Option<TableMutationHandlerRef>,
        procedure_service_handler: Option<ProcedureServiceHandlerRef>,
        flow_service_handler: Option<FlowServiceHandlerRef>,
        with_dist_planner: bool,
        plugins: Plugins,
        options: QueryOptionsNew,
    ) -> DfResult<Self> {
        let total_memory = get_total_memory_bytes().max(0) as u64;
        let memory_pool_size = options.memory_pool_size.resolve(total_memory) as usize;
        let runtime_provider = plugins.get::<QueryRuntimeProviderRef>();
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

        // todo(hl): This serves as a workaround for https://github.com/GreptimeTeam/greptimedb/issues/5659
        // and we can add that check back once we upgrade datafusion.
        session_config
            .options_mut()
            .execution
            .skip_physical_aggregate_schema_check = true;

        let runtime_context = QueryRuntimeContext::new(&options, memory_pool_size);
        let default_runtime_provider = DefaultQueryRuntimeProvider;
        default_runtime_provider.configure_session_config(runtime_context, &mut session_config);
        if let Some(provider) = runtime_provider.as_ref() {
            provider.configure_session_config(runtime_context, &mut session_config);
        }
        let runtime_builder = DefaultQueryRuntimeProvider::runtime_env_builder(runtime_context);
        let runtime_env = match runtime_provider {
            Some(provider) => provider.build_runtime_env(runtime_context, runtime_builder)?,
            None => default_runtime_provider.build_runtime_env(runtime_context, runtime_builder)?,
        };

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

        // Note: Postgres oid-alias string coercion (`'x'::regclass`,
        // `'public'::regnamespace`, ...) is handled by the
        // `PostgresCompatibilityParser`'s built-in `RewriteRegCastToSubquery`
        // rule at SQL-parse time, so no analyzer rule is registered here. The
        // implicit `oid_col = 'name'` form is not resolved (it needs schema
        // awareness the parser lacks); the few client queries that use it are
        // handled by the parser's blacklist.

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
        physical_optimizer
            .rules
            .push(Arc::new(EnsureGlobalLimitForFetch));
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
        register_pg_catalog_compat(&df_context);

        Ok(Self {
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
        })
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
        session: &dyn Session,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        self.physical_planner
            .create_physical_plan(logical_plan, session)
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

/// Register the session-level (not function-registry) Postgres-compatibility
/// extension sourced from `datafusion-pg-catalog`: widen Postgres `int4`
/// bounds to `int8` for `generate_series`/`range` (their stock implementations
/// require `int8` literals, and DF invokes them at planning time, before any
/// analyzer rule can run).
///
/// The oid-alias type planner is supplied per SQL-planner context by
/// [`DfContextProviderAdapter::get_type_planner`](crate::datafusion::planner::DfContextProviderAdapter::get_type_planner).
/// Forward oid-alias name->oid resolution is handled at SQL-parse time by the
/// `PostgresCompatibilityParser`'s built-in rewrite rule, not here.
fn register_pg_catalog_compat(ctx: &SessionContext) {
    datafusion_pg_catalog::pg_catalog::generate_series_arg_coercion::CoerceIntArgsToBigInt::widen(
        ctx,
    );
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
/// The inner pool is wrapped with `TrackConsumersPool` to preserve
/// top-consumer error context on rejection.
#[derive(Debug)]
pub(super) struct MetricsMemoryPool {
    inner: Arc<dyn MemoryPool>,
}

impl MetricsMemoryPool {
    // Number of top memory consumers to report in OOM error messages
    const TOP_CONSUMERS_TO_REPORT: usize = 5;

    /// Create a new metrics-wrapped memory pool with the given size limit and
    /// allocation policy.
    pub(super) fn new(limit: usize, policy: QueryMemoryPoolPolicy) -> Self {
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

impl fmt::Display for MetricsMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(inner_pool: {})", self.name(), self.inner)
    }
}

impl MemoryPool for MetricsMemoryPool {
    fn name(&self) -> &str {
        "metrics"
    }

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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use common_base::Plugins;
    use common_base::memory_limit::MemoryLimit;
    use common_base::readable_size::ReadableSize;
    use datafusion::error::DataFusionError;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryLimit as DfMemoryLimit};
    use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
    use datafusion_common::config::SpillCompression;
    use session::context::QueryContext;

    use super::*;
    use crate::options::{QueryOptions, QuerySpillCompression, QuerySpillMode};
    use crate::query_engine::runtime::{
        DefaultQueryRuntimeProvider, QueryRuntimeContext, QueryRuntimeProvider,
        QueryRuntimeProviderRef,
    };

    fn new_query_engine_state() -> QueryEngineState {
        new_query_engine_state_with(Plugins::default(), QueryOptions::default())
    }

    fn new_query_engine_state_with(plugins: Plugins, options: QueryOptions) -> QueryEngineState {
        QueryEngineState::new(
            catalog::memory::new_memory_catalog_manager().unwrap(),
            None,
            None,
            None,
            None,
            None,
            false,
            plugins,
            options,
        )
    }

    struct TestRuntimeProvider {
        build_called: AtomicBool,
        configure_called: AtomicBool,
    }

    impl TestRuntimeProvider {
        fn new() -> Self {
            Self {
                build_called: AtomicBool::new(false),
                configure_called: AtomicBool::new(false),
            }
        }
    }

    impl QueryRuntimeProvider for TestRuntimeProvider {
        fn configure_session_config(
            &self,
            ctx: QueryRuntimeContext<'_>,
            config: &mut SessionConfig,
        ) {
            assert_eq!(ctx.resolved_memory_pool_size, 1024);
            self.configure_called.store(true, Ordering::SeqCst);
            *config = config.clone().with_target_partitions(7);
        }

        fn build_runtime_env(
            &self,
            ctx: QueryRuntimeContext<'_>,
            builder: RuntimeEnvBuilder,
        ) -> DfResult<Arc<RuntimeEnv>> {
            assert_eq!(ctx.resolved_memory_pool_size, 1024);
            self.build_called.store(true, Ordering::SeqCst);
            builder
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(2048)))
                .build()
                .map(Arc::new)
        }
    }

    struct ErrorRuntimeProvider;

    impl QueryRuntimeProvider for ErrorRuntimeProvider {
        fn build_runtime_env(
            &self,
            _ctx: QueryRuntimeContext<'_>,
            _builder: RuntimeEnvBuilder,
        ) -> DfResult<Arc<RuntimeEnv>> {
            Err(DataFusionError::Execution("runtime provider error".into()))
        }
    }

    #[test]
    fn query_runtime_default_provider_keeps_bounded_memory_pool() {
        let state = new_query_engine_state_with(
            Plugins::default(),
            QueryOptions {
                memory_pool_size: MemoryLimit::Size(ReadableSize(1024)),
                ..Default::default()
            },
        );

        assert!(matches!(
            state
                .session_state()
                .runtime_env()
                .memory_pool
                .memory_limit(),
            DfMemoryLimit::Finite(1024)
        ));
    }

    #[test]
    fn query_runtime_provider_from_plugins_builds_runtime_env() {
        let plugins = Plugins::default();
        let provider = Arc::new(TestRuntimeProvider::new());
        plugins.insert::<QueryRuntimeProviderRef>(provider.clone());

        let state = new_query_engine_state_with(
            plugins,
            QueryOptions {
                memory_pool_size: MemoryLimit::Size(ReadableSize(1024)),
                ..Default::default()
            },
        );

        assert!(provider.build_called.load(Ordering::SeqCst));
        assert!(matches!(
            state
                .session_state()
                .runtime_env()
                .memory_pool
                .memory_limit(),
            DfMemoryLimit::Finite(2048)
        ));
    }

    #[test]
    fn query_runtime_provider_from_plugins_configures_session_config() {
        let plugins = Plugins::default();
        let provider = Arc::new(TestRuntimeProvider::new());
        plugins.insert::<QueryRuntimeProviderRef>(provider.clone());

        let state = new_query_engine_state_with(
            plugins,
            QueryOptions {
                memory_pool_size: MemoryLimit::Size(ReadableSize(1024)),
                experimental_spill_mode: QuerySpillMode::Custom,
                experimental_spill_compression: QuerySpillCompression::Zstd,
                ..Default::default()
            },
        );

        assert!(provider.configure_called.load(Ordering::SeqCst));
        assert_eq!(7, state.session_state().config().target_partitions());
        assert_eq!(
            SpillCompression::Zstd,
            state
                .session_state()
                .config()
                .options()
                .execution
                .spill_compression
        );
    }

    #[test]
    fn query_runtime_provider_error_is_returned_by_try_new() {
        let plugins = Plugins::default();
        plugins.insert::<QueryRuntimeProviderRef>(Arc::new(ErrorRuntimeProvider));

        let err = QueryEngineState::try_new(
            catalog::memory::new_memory_catalog_manager().unwrap(),
            None,
            None,
            None,
            None,
            None,
            false,
            plugins,
            QueryOptions::default(),
        )
        .unwrap_err();

        assert!(
            matches!(err, DataFusionError::Execution(message) if message == "runtime provider error")
        );
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

    /// Builds a runtime env through the default provider seam, mirroring what
    /// [`QueryEngineState::try_new`] does.
    fn build_runtime_env(options: &QueryOptions, memory_pool_size: usize) -> Arc<RuntimeEnv> {
        let ctx = QueryRuntimeContext::new(options, memory_pool_size);
        let builder = DefaultQueryRuntimeProvider::runtime_env_builder(ctx);
        DefaultQueryRuntimeProvider
            .build_runtime_env(ctx, builder)
            .expect("Failed to build RuntimeEnv")
    }

    #[test]
    fn test_build_runtime_env_custom_mode_with_path() {
        // Use a temp directory managed manually so we don't need the `tempfile` crate.
        let spill_dir = std::env::temp_dir().join(format!("df_spill_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&spill_dir);
        let opts = QueryOptions {
            experimental_spill_mode: QuerySpillMode::Custom,
            experimental_spill_path: Some(spill_dir.clone()),
            experimental_spill_max_temp_directory_size: ReadableSize::gb(1),
            ..Default::default()
        };
        let env = build_runtime_env(&opts, 0);

        assert!(env.disk_manager.tmp_files_enabled());
        let tmp_file = env.disk_manager.create_tmp_file("test spill");
        assert!(tmp_file.is_ok());
        assert!(spill_dir.exists());

        let _ = std::fs::remove_dir_all(&spill_dir);
    }

    #[test]
    fn test_build_runtime_env_disabled_mode() {
        let opts = QueryOptions {
            experimental_spill_mode: QuerySpillMode::Disabled,
            ..Default::default()
        };
        let env = build_runtime_env(&opts, 0);

        assert!(!env.disk_manager.tmp_files_enabled());
        let result = env.disk_manager.create_tmp_file("test spill");
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("DiskManager is disabled"));
    }

    #[test]
    fn test_metrics_memory_pool_policy_differs_with_multiple_spillable_consumers() {
        for (policy, greedy) in [
            (QueryMemoryPoolPolicy::Greedy, true),
            (QueryMemoryPoolPolicy::Fair, false),
        ] {
            let pool: Arc<dyn MemoryPool> = Arc::new(MetricsMemoryPool::new(100, policy));
            let first = MemoryConsumer::new("first")
                .with_can_spill(true)
                .register(&pool);
            let second = MemoryConsumer::new("second")
                .with_can_spill(true)
                .register(&pool);

            let first_result = first.try_grow(75);
            assert_eq!(first_result.is_ok(), greedy);
            if greedy {
                assert!(second.try_grow(26).is_err());
            } else {
                first.try_grow(50).unwrap();
                second.try_grow(50).unwrap();
            }
        }
    }

    /// Builds a runtime with custom spill configuration and a bounded memory
    /// pool, then runs sort queries that probe spill-to-disk behaviour:
    ///
    /// - A pool large enough for merge chunks but still much smaller
    ///   than the total data.  Executes the physical plan directly so we can
    ///   walk the plan tree afterwards and sum `spill_count` / `spilled_rows` /
    ///   `spilled_bytes` on `SortExec` nodes.  All three must be > 0.
    #[tokio::test]
    async fn test_sort_spill_smoke_with_custom_runtime() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::physical_plan::collect as df_collect;

        let spill_dir =
            std::env::temp_dir().join(format!("greptime_spill_smoke_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&spill_dir);

        let opts = QueryOptions {
            experimental_spill_mode: QuerySpillMode::Custom,
            experimental_spill_path: Some(spill_dir.clone()),
            experimental_spill_max_temp_directory_size: ReadableSize::gb(1),
            experimental_memory_pool_policy: QueryMemoryPoolPolicy::Greedy,
            ..Default::default()
        };

        let session_config = SessionConfig::new()
            .with_target_partitions(1)
            .with_sort_in_place_threshold_bytes(0)
            .with_sort_spill_reservation_bytes(64 * 1024);

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

        {
            let mem_limit: usize = 512 * 1024; // 512 KB pool, 64 KB reserved for merge

            let runtime = build_runtime_env(&opts, mem_limit);
            assert_eq!(runtime.memory_pool.reserved(), 0);
            let ctx = build_ctx(&session_config, &runtime, &schema, &table_partitions);

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

            let (spill_count, spilled_rows, spilled_bytes) = sum_sort_spill_metrics(&plan);
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

            assert_eq!(runtime.memory_pool.reserved(), 0);
        }

        let _ = std::fs::remove_dir_all(&spill_dir);
    }

    /// Walk a physical plan tree, summing spill metrics for every node whose
    /// name starts with `"SortExec"`.
    fn sum_sort_spill_metrics(plan: &Arc<dyn ExecutionPlan>) -> (usize, usize, usize) {
        let mut spill_count = 0usize;
        let mut spilled_rows = 0usize;
        let mut spilled_bytes = 0usize;

        fn walk(plan: &Arc<dyn ExecutionPlan>, sc: &mut usize, sr: &mut usize, sb: &mut usize) {
            let name = plan.name();
            if name.starts_with("SortExec")
                && let Some(m) = plan.metrics()
            {
                *sc += m.spill_count().unwrap_or(0);
                *sr += m.spilled_rows().unwrap_or(0);
                *sb += m.spilled_bytes().unwrap_or(0);
            }
            for child in plan.children() {
                walk(child, sc, sr, sb);
            }
        }

        walk(
            plan,
            &mut spill_count,
            &mut spilled_rows,
            &mut spilled_bytes,
        );
        (spill_count, spilled_rows, spilled_bytes)
    }
}
