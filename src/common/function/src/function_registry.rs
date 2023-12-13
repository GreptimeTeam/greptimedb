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

use crate::function::FunctionRef;
use crate::scalars::aggregate::{AggregateFunctionMetaRef, AggregateFunctions};
use crate::scalars::date::DateFunction;
use crate::scalars::math::MathFunction;
use crate::scalars::numpy::NumpyFunction;
use crate::scalars::timestamp::TimestampFunction;
use crate::system::SystemFunction;

#[derive(Default)]
pub struct FunctionRegistry {
    functions: RwLock<HashMap<String, FunctionRef>>,
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

    MathFunction::register(&function_registry);
    NumpyFunction::register(&function_registry);
    TimestampFunction::register(&function_registry);
    DateFunction::register(&function_registry);

    AggregateFunctions::register(&function_registry);
    SystemFunction::register(&function_registry);
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
