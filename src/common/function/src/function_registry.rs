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

//! functions registry
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

use datafusion::catalog::TableFunction;
use datafusion_expr::AggregateUDF;
use datafusion_expr::expr_rewriter::FunctionRewrite;

use crate::admin::AdminFunction;
use crate::aggrs::aggr_wrapper::StateMergeHelper;
use crate::aggrs::approximate::ApproximateFunction;
use crate::aggrs::count_hash::CountHash;
use crate::aggrs::vector::VectorFunction as VectorAggrFunction;
use crate::function::{Function, FunctionRef};
use crate::function_factory::ScalarFunctionFactory;
use crate::scalars::date::DateFunction;
use crate::scalars::expression::ExpressionFunction;
use crate::scalars::hll_count::HllCalcFunction;
use crate::scalars::ip::IpFunctions;
use crate::scalars::json::JsonFunction;
use crate::scalars::matches::MatchesFunction;
use crate::scalars::matches_term::MatchesTermFunction;
use crate::scalars::math::MathFunction;
use crate::scalars::primary_key::DecodePrimaryKeyFunction;
use crate::scalars::string::register_string_functions;
use crate::scalars::timestamp::TimestampFunction;
use crate::scalars::uddsketch_calc::UddSketchCalcFunction;
use crate::scalars::vector::VectorFunction as VectorScalarFunction;
use crate::system::SystemFunction;

#[derive(Default)]
pub struct FunctionRegistry {
    functions: RwLock<HashMap<String, ScalarFunctionFactory>>,
    aggregate_functions: RwLock<HashMap<String, AggregateUDF>>,
    table_functions: RwLock<HashMap<String, Arc<TableFunction>>>,
    function_rewrites: RwLock<Vec<Arc<dyn FunctionRewrite + Send + Sync>>>,
}

impl FunctionRegistry {
    /// Register a function in the registry by converting it into a `ScalarFunctionFactory`.
    ///
    /// # Arguments
    ///
    /// * `func` - An object that can be converted into a `ScalarFunctionFactory`.
    ///
    /// The function is inserted into the internal function map, keyed by its name.
    /// If a function with the same name already exists, it will be replaced.
    pub fn register(&self, func: impl Into<ScalarFunctionFactory>) {
        let func = func.into();
        let _ = self
            .functions
            .write()
            .unwrap()
            .insert(func.name().to_string(), func);
    }

    /// Register a scalar function in the registry.
    pub fn register_scalar(&self, func: impl Function + 'static) {
        let func = Arc::new(func) as FunctionRef;

        for alias in func.aliases() {
            let func: ScalarFunctionFactory = func.clone().into();
            let alias = ScalarFunctionFactory {
                name: alias.clone(),
                ..func
            };
            self.register(alias);
        }

        self.register(func)
    }

    /// Register an aggregate function in the registry.
    pub fn register_aggr(&self, func: AggregateUDF) {
        let _ = self
            .aggregate_functions
            .write()
            .unwrap()
            .insert(func.name().to_string(), func);
    }

    /// Register a table function
    pub fn register_table_function(&self, func: TableFunction) {
        let _ = self
            .table_functions
            .write()
            .unwrap()
            .insert(func.name().to_string(), Arc::new(func));
    }

    /// Register a function rewrite rule.
    pub fn register_function_rewrite(&self, func: impl FunctionRewrite + Send + Sync + 'static) {
        self.function_rewrites.write().unwrap().push(Arc::new(func));
    }

    pub fn get_function(&self, name: &str) -> Option<ScalarFunctionFactory> {
        self.functions.read().unwrap().get(name).cloned()
    }

    /// Returns a list of all scalar functions registered in the registry.
    pub fn scalar_functions(&self) -> Vec<ScalarFunctionFactory> {
        self.functions.read().unwrap().values().cloned().collect()
    }

    /// Returns a list of all aggregate functions registered in the registry.
    pub fn aggregate_functions(&self) -> Vec<AggregateUDF> {
        self.aggregate_functions
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    pub fn table_functions(&self) -> Vec<Arc<TableFunction>> {
        self.table_functions
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    /// Returns true if an aggregate function with the given name exists in the registry.
    pub fn is_aggr_func_exist(&self, name: &str) -> bool {
        self.aggregate_functions.read().unwrap().contains_key(name)
    }

    /// Returns a list of all function rewrite rules registered in the registry.
    pub fn function_rewrites(&self) -> Vec<Arc<dyn FunctionRewrite + Send + Sync>> {
        self.function_rewrites.read().unwrap().clone()
    }
}

pub static FUNCTION_REGISTRY: LazyLock<Arc<FunctionRegistry>> = LazyLock::new(|| {
    let function_registry = FunctionRegistry::default();

    // Utility functions
    MathFunction::register(&function_registry);
    TimestampFunction::register(&function_registry);
    DateFunction::register(&function_registry);
    ExpressionFunction::register(&function_registry);
    UddSketchCalcFunction::register(&function_registry);
    HllCalcFunction::register(&function_registry);
    DecodePrimaryKeyFunction::register(&function_registry);

    // Full text search function
    MatchesFunction::register(&function_registry);
    MatchesTermFunction::register(&function_registry);

    // System and administration functions
    SystemFunction::register(&function_registry);
    AdminFunction::register(&function_registry);

    // Json related functions
    JsonFunction::register(&function_registry);

    // String related functions
    register_string_functions(&function_registry);

    // Vector related functions
    VectorScalarFunction::register(&function_registry);
    VectorAggrFunction::register(&function_registry);

    // Geo functions
    #[cfg(feature = "geo")]
    crate::scalars::geo::GeoFunctions::register(&function_registry);
    #[cfg(feature = "geo")]
    crate::aggrs::geo::GeoFunction::register(&function_registry);

    // Ip functions
    IpFunctions::register(&function_registry);

    // Approximate functions
    ApproximateFunction::register(&function_registry);

    // CountHash function
    CountHash::register(&function_registry);

    // state function of supported aggregate functions
    StateMergeHelper::register(&function_registry);

    Arc::new(function_registry)
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scalars::test::TestAndFunction;

    #[test]
    fn test_function_registry() {
        let registry = FunctionRegistry::default();

        assert!(registry.get_function("test_and").is_none());
        assert!(registry.scalar_functions().is_empty());
        registry.register_scalar(TestAndFunction::default());
        let _ = registry.get_function("test_and").unwrap();
        assert_eq!(1, registry.scalar_functions().len());
    }
}
