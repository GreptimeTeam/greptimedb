//! functions registry
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use once_cell::sync::Lazy;

use crate::scalars::function::FunctionRef;
use crate::scalars::math::MathsFunction;
use crate::scalars::numpy::NumpyFunction;

#[derive(Default)]
pub struct FunctionRegistry {
    functions: RwLock<HashMap<String, FunctionRef>>,
}

impl FunctionRegistry {
    pub fn register(&self, func: FunctionRef) {
        self.functions
            .write()
            .unwrap()
            .insert(func.name().to_string(), func);
    }

    pub fn get_function(&self, name: &str) -> Option<FunctionRef> {
        self.functions.read().unwrap().get(name).map(Clone::clone)
    }

    pub fn functions(&self) -> Vec<FunctionRef> {
        self.functions
            .read()
            .unwrap()
            .values()
            .map(Clone::clone)
            .collect()
    }
}

pub static FUNCTION_REGISTRY: Lazy<Arc<FunctionRegistry>> = Lazy::new(|| {
    let function_registry = FunctionRegistry::default();

    MathsFunction::register(&function_registry);
    NumpyFunction::register(&function_registry);

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
