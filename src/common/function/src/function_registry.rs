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
use std::sync::{Arc, RwLock};

use once_cell::sync::Lazy;

use crate::function::{AsyncFunctionRef, FunctionRef};
use crate::scalars::aggregate::{AggregateFunctionMetaRef, AggregateFunctions};
use crate::scalars::date::DateFunction;
use crate::scalars::expression::ExpressionFunction;
use crate::scalars::hll_count::HllCalcFunction;
use crate::scalars::ip::IpFunctions;
use crate::scalars::json::JsonFunction;
use crate::scalars::matches::MatchesFunction;
use crate::scalars::math::MathFunction;
use crate::scalars::timestamp::TimestampFunction;
use crate::scalars::uddsketch_calc::UddSketchCalcFunction;
use crate::scalars::vector::VectorFunction;
use crate::system::SystemFunction;
use crate::table::TableFunction;

#[derive(Default)]
pub struct FunctionRegistry {
    functions: RwLock<HashMap<String, FunctionRef>>,
    async_functions: RwLock<HashMap<String, AsyncFunctionRef>>,
    aggregate_functions: RwLock<HashMap<String, AggregateFunctionMetaRef>>,
}

impl FunctionRegistry {
    pub fn register(&self, func: FunctionRef) {
        let _ = self
            .functions
            .write()
            .unwrap()
            .insert(func.name().to_string(), func);
    }

    pub fn register_async(&self, func: AsyncFunctionRef) {
        let _ = self
            .async_functions
            .write()
            .unwrap()
            .insert(func.name().to_string(), func);
    }

    pub fn get_async_function(&self, name: &str) -> Option<AsyncFunctionRef> {
        self.async_functions.read().unwrap().get(name).cloned()
    }

    pub fn async_functions(&self) -> Vec<AsyncFunctionRef> {
        self.async_functions
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    pub fn register_aggregate_function(&self, func: AggregateFunctionMetaRef) {
        let _ = self
            .aggregate_functions
            .write()
            .unwrap()
            .insert(func.name(), func);
    }

    pub fn get_aggr_function(&self, name: &str) -> Option<AggregateFunctionMetaRef> {
        self.aggregate_functions.read().unwrap().get(name).cloned()
    }

    pub fn get_function(&self, name: &str) -> Option<FunctionRef> {
        self.functions.read().unwrap().get(name).cloned()
    }

    pub fn functions(&self) -> Vec<FunctionRef> {
        self.functions.read().unwrap().values().cloned().collect()
    }

    pub fn aggregate_functions(&self) -> Vec<AggregateFunctionMetaRef> {
        self.aggregate_functions
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }
}

pub static FUNCTION_REGISTRY: Lazy<Arc<FunctionRegistry>> = Lazy::new(|| {
    let function_registry = FunctionRegistry::default();

    // Utility functions
    MathFunction::register(&function_registry);
    TimestampFunction::register(&function_registry);
    DateFunction::register(&function_registry);
    ExpressionFunction::register(&function_registry);
    UddSketchCalcFunction::register(&function_registry);
    HllCalcFunction::register(&function_registry);

    // Aggregate functions
    AggregateFunctions::register(&function_registry);

    // Full text search function
    MatchesFunction::register(&function_registry);

    // System and administration functions
    SystemFunction::register(&function_registry);
    TableFunction::register(&function_registry);

    // Json related functions
    JsonFunction::register(&function_registry);

    // Vector related functions
    VectorFunction::register(&function_registry);

    // Geo functions
    #[cfg(feature = "geo")]
    crate::scalars::geo::GeoFunctions::register(&function_registry);

    // Ip functions
    IpFunctions::register(&function_registry);

    Arc::new(function_registry)
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scalars::test::TestAndFunction;

    #[test]
    fn test_function_registry() {
        let registry = FunctionRegistry::default();
        let func = Arc::new(TestAndFunction);

        assert!(registry.get_function("test_and").is_none());
        assert!(registry.functions().is_empty());
        registry.register(func);
        let _ = registry.get_function("test_and").unwrap();
        assert_eq!(1, registry.functions().len());
    }
}
