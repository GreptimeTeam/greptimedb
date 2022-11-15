//! functions registry
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use once_cell::sync::Lazy;

use crate::scalars::aggregate::{AggregateFunctionMetaRef, AggregateFunctions};
use crate::scalars::function::FunctionRef;
use crate::scalars::math::MathFunction;
use crate::scalars::numpy::NumpyFunction;
use crate::scalars::timestamp::TimestampFunction;

#[derive(Default)]
pub struct FunctionRegistry {
    functions: RwLock<HashMap<String, FunctionRef>>,
    aggregate_functions: RwLock<HashMap<String, AggregateFunctionMetaRef>>,
}

impl FunctionRegistry {
    pub fn register(&self, func: FunctionRef) {
        self.functions
            .write()
            .unwrap()
            .insert(func.name().to_string(), func);
    }

    pub fn register_aggregate_function(&self, func: AggregateFunctionMetaRef) {
        self.aggregate_functions
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

    AggregateFunctions::register(&function_registry);

    Arc::new(function_registry)
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scalars::test::TestAndFunction;

    #[test]
    fn test_function_registry() {
        let registry = FunctionRegistry::default();
        let func = Arc::new(TestAndFunction::default());

        assert!(registry.get_function("test_and").is_none());
        assert!(registry.functions().is_empty());
        registry.register(func);
        assert!(registry.get_function("test_and").is_some());
        assert_eq!(1, registry.functions().len());
    }
}
