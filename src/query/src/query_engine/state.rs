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
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_base::Plugins;
use common_function::function_factory::ScalarFunctionFactory;
use common_function::handlers::{
    FlowServiceHandlerRef, ProcedureServiceHandlerRef, TableMutationHandlerRef,
};
use common_function::state::FunctionState;
use common_telemetry::warn;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::{QueryPlanner, SessionConfig, SessionContext, SessionState};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_expr::{AggregateUDF, LogicalPlan as DfLogicalPlan};
use datafusion_optimizer::analyzer::Analyzer;
use datafusion_optimizer::optimizer::Optimizer;
use promql::extension_plan::PromExtensionPlanner;
use table::table::adapter::DfTableProviderAdapter;
use table::TableRef;

use crate::dist_plan::{
    DistExtensionPlanner, DistPlannerAnalyzer, DistPlannerOptions, MergeSortExtensionPlanner,
};
use crate::optimizer::constant_term::MatchesConstantTermOptimizer;
use crate::optimizer::count_wildcard::CountWildcardToTimeIndexRule;
use crate::optimizer::parallelize_scan::ParallelizeScan;
use crate::optimizer::pass_distribution::PassDistribution;
use crate::optimizer::remove_duplicate::RemoveDuplicate;
use crate::optimizer::scan_hint::ScanHintRule;
use crate::optimizer::string_normalization::StringNormalizationRule;
use crate::optimizer::transcribe_atat::TranscribeAtatRule;
use crate::optimizer::type_conversion::TypeConversionRule;
use crate::optimizer::windowed_sort::WindowedSortPhysicalRule;
use crate::optimizer::ExtensionAnalyzerRule;
use crate::options::QueryOptions as QueryOptionsNew;
use crate::query_engine::options::QueryOptions;
use crate::query_engine::DefaultSerializer;
use crate::range_select::planner::RangeSelectPlanner;
use crate::region_query::RegionQueryHandlerRef;
use crate::QueryEngineContext;

/// Query engine global state
#[derive(Clone)]
pub struct QueryEngineState {
    df_context: SessionContext,
    catalog_manager: CatalogManagerRef,
    function_state: Arc<FunctionState>,
    scalar_functions: Arc<RwLock<HashMap<String, ScalarFunctionFactory>>>,
    aggr_functions: Arc<RwLock<HashMap<String, AggregateUDF>>>,
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
        region_query_handler: Option<RegionQueryHandlerRef>,
        table_mutation_handler: Option<TableMutationHandlerRef>,
        procedure_service_handler: Option<ProcedureServiceHandlerRef>,
        flow_service_handler: Option<FlowServiceHandlerRef>,
        with_dist_planner: bool,
        plugins: Plugins,
        options: QueryOptionsNew,
    ) -> Self {
        let runtime_env = Arc::new(RuntimeEnv::default());
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

        // Apply extension rules
        let mut extension_rules = Vec::new();

        // The [`TypeConversionRule`] must be at first
        extension_rules.insert(0, Arc::new(TypeConversionRule) as _);

        // Apply the datafusion rules
        let mut analyzer = Analyzer::new();
        analyzer.rules.insert(0, Arc::new(TranscribeAtatRule));
        analyzer.rules.insert(0, Arc::new(StringNormalizationRule));
        analyzer
            .rules
            .insert(0, Arc::new(CountWildcardToTimeIndexRule));

        if with_dist_planner {
            analyzer.rules.push(Arc::new(DistPlannerAnalyzer));
        }

        let mut optimizer = Optimizer::new();
        optimizer.rules.push(Arc::new(ScanHintRule));

        // add physical optimizer
        let mut physical_optimizer = PhysicalOptimizer::new();
        // Change TableScan's partition at first
        physical_optimizer
            .rules
            .insert(0, Arc::new(ParallelizeScan));
        // Pass distribution requirement to MergeScanExec to avoid unnecessary shuffling
        physical_optimizer
            .rules
            .insert(1, Arc::new(PassDistribution));
        // Add rule for windowed sort
        physical_optimizer
            .rules
            .push(Arc::new(WindowedSortPhysicalRule));
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
                region_query_handler,
            )))
            .with_optimizer_rules(optimizer.rules)
            .with_physical_optimizer_rules(physical_optimizer.rules)
            .build();

        let df_context = SessionContext::new_with_state(session_state);

        Self {
            df_context,
            catalog_manager: catalog_list,
            function_state: Arc::new(FunctionState {
                table_mutation_handler,
                procedure_service_handler,
                flow_service_handler,
            }),
            aggr_functions: Arc::new(RwLock::new(HashMap::new())),
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

    /// Optimize the logical plan by the extension anayzer rules.
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

    pub fn catalog_manager(&self) -> &CatalogManagerRef {
        &self.catalog_manager
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

impl DfQueryPlanner {
    fn new(
        catalog_manager: CatalogManagerRef,
        region_query_handler: Option<RegionQueryHandlerRef>,
    ) -> Self {
        let mut planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> =
            vec![Arc::new(PromExtensionPlanner), Arc::new(RangeSelectPlanner)];
        if let Some(region_query_handler) = region_query_handler {
            planners.push(Arc::new(DistExtensionPlanner::new(
                catalog_manager,
                region_query_handler,
            )));
            planners.push(Arc::new(MergeSortExtensionPlanner {}));
        }
        Self {
            physical_planner: DefaultPhysicalPlanner::with_extension_planners(planners),
        }
    }
}
