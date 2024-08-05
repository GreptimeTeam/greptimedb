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
use common_function::function::FunctionRef;
use common_function::handlers::{
    FlowServiceHandlerRef, ProcedureServiceHandlerRef, TableMutationHandlerRef,
};
use common_function::scalars::aggregate::AggregateFunctionMetaRef;
use common_function::state::FunctionState;
use common_query::prelude::ScalarUdf;
use common_telemetry::warn;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::{QueryPlanner, SessionConfig, SessionContext, SessionState};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_expr::LogicalPlan as DfLogicalPlan;
use datafusion_optimizer::analyzer::count_wildcard_rule::CountWildcardRule;
use datafusion_optimizer::analyzer::{Analyzer, AnalyzerRule};
use datafusion_optimizer::optimizer::Optimizer;
use promql::extension_plan::PromExtensionPlanner;
use table::table::adapter::DfTableProviderAdapter;
use table::TableRef;

use crate::dist_plan::{DistExtensionPlanner, DistPlannerAnalyzer};
use crate::optimizer::count_wildcard::CountWildcardToTimeIndexRule;
use crate::optimizer::parallelize_scan::ParallelizeScan;
use crate::optimizer::remove_duplicate::RemoveDuplicate;
use crate::optimizer::scan_hint::ScanHintRule;
use crate::optimizer::string_normalization::StringNormalizationRule;
use crate::optimizer::type_conversion::TypeConversionRule;
use crate::optimizer::ExtensionAnalyzerRule;
use crate::query_engine::options::QueryOptions;
use crate::query_engine::DefaultSerializer;
use crate::range_select::planner::RangeSelectPlanner;
use crate::region_query::RegionQueryHandlerRef;
use crate::QueryEngineContext;

/// Query engine global state
// TODO(yingwen): This QueryEngineState still relies on datafusion, maybe we can define a trait for it,
// which allows different implementation use different engine state. The state can also be an associated
// type in QueryEngine trait.
#[derive(Clone)]
pub struct QueryEngineState {
    df_context: SessionContext,
    catalog_manager: CatalogManagerRef,
    function_state: Arc<FunctionState>,
    udf_functions: Arc<RwLock<HashMap<String, FunctionRef>>>,
    aggregate_functions: Arc<RwLock<HashMap<String, AggregateFunctionMetaRef>>>,
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
    pub fn new(
        catalog_list: CatalogManagerRef,
        region_query_handler: Option<RegionQueryHandlerRef>,
        table_mutation_handler: Option<TableMutationHandlerRef>,
        procedure_service_handler: Option<ProcedureServiceHandlerRef>,
        flow_service_handler: Option<FlowServiceHandlerRef>,
        with_dist_planner: bool,
        plugins: Plugins,
    ) -> Self {
        let runtime_env = Arc::new(RuntimeEnv::default());
        let session_config = SessionConfig::new().with_create_default_catalog_and_schema(false);
        // Apply extension rules
        let mut extension_rules = Vec::new();

        // The [`TypeConversionRule`] must be at first
        extension_rules.insert(0, Arc::new(TypeConversionRule) as _);

        // Apply the datafusion rules
        let mut analyzer = Analyzer::new();
        analyzer.rules.insert(0, Arc::new(StringNormalizationRule));

        // Use our custom rule instead to optimize the count(*) query
        Self::remove_analyzer_rule(&mut analyzer.rules, CountWildcardRule {}.name());
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
        // Add rule to remove duplicate nodes generated by other rules. Run this in the last.
        physical_optimizer.rules.push(Arc::new(RemoveDuplicate));

        let session_state = SessionState::new_with_config_rt(session_config, runtime_env)
            .with_analyzer_rules(analyzer.rules)
            .with_serializer_registry(Arc::new(DefaultSerializer))
            .with_query_planner(Arc::new(DfQueryPlanner::new(
                catalog_list.clone(),
                region_query_handler,
            )))
            .with_optimizer_rules(optimizer.rules)
            .with_physical_optimizer_rules(physical_optimizer.rules);

        let df_context = SessionContext::new_with_state(session_state);

        Self {
            df_context,
            catalog_manager: catalog_list,
            function_state: Arc::new(FunctionState {
                table_mutation_handler,
                procedure_service_handler,
                flow_service_handler,
            }),
            aggregate_functions: Arc::new(RwLock::new(HashMap::new())),
            extension_rules,
            plugins,
            udf_functions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn remove_analyzer_rule(rules: &mut Vec<Arc<dyn AnalyzerRule + Send + Sync>>, name: &str) {
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

    /// Register an udf function.
    /// Will override if the function with same name is already registered.
    pub fn register_function(&self, func: FunctionRef) {
        let name = func.name().to_string();
        let x = self
            .udf_functions
            .write()
            .unwrap()
            .insert(name.clone(), func);

        if x.is_some() {
            warn!("Already registered udf function '{name}'");
        }
    }

    /// Retrieve the udf function by name
    pub fn udf_function(&self, function_name: &str) -> Option<FunctionRef> {
        self.udf_functions
            .read()
            .unwrap()
            .get(function_name)
            .cloned()
    }

    /// Retrieve the aggregate function by name
    pub fn aggregate_function(&self, function_name: &str) -> Option<AggregateFunctionMetaRef> {
        self.aggregate_functions
            .read()
            .unwrap()
            .get(function_name)
            .cloned()
    }

    /// Register a [`ScalarUdf`].
    pub fn register_udf(&self, udf: ScalarUdf) {
        self.df_context.register_udf(udf.into());
    }

    /// Register an aggregate function.
    ///
    /// # Panics
    /// Will panic if the function with same name is already registered.
    ///
    /// Panicking consideration: currently the aggregated functions are all statically registered,
    /// user cannot define their own aggregate functions on the fly. So we can panic here. If that
    /// invariant is broken in the future, we should return an error instead of panicking.
    pub fn register_aggregate_function(&self, func: AggregateFunctionMetaRef) {
        let name = func.name();
        let x = self
            .aggregate_functions
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

    pub(crate) fn session_state(&self) -> SessionState {
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
        }
        Self {
            physical_planner: DefaultPhysicalPlanner::with_extension_planners(planners),
        }
    }
}
